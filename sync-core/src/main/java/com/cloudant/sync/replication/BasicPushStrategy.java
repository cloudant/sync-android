/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.replication;

import com.cloudant.mazha.CouchClient;
import com.cloudant.mazha.CouchConfig;
import com.cloudant.mazha.json.JSONHelper;
import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.AttachmentException;
import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.Changes;
import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.datastore.DocumentRevisionTree;
import com.cloudant.sync.datastore.MultipartAttachmentWriter;
import com.cloudant.sync.datastore.RevisionHistoryHelper;
import com.cloudant.sync.util.JSONUtils;
import com.cloudant.sync.util.Misc;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;

import org.apache.commons.codec.binary.Hex;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

class BasicPushStrategy implements ReplicationStrategy {

    private static final String LOG_TAG = "BasicPushStrategy";
    private static final Logger logger = Logger.getLogger(BasicPushStrategy.class.getCanonicalName());

    public static final int DEFAULT_CHANGES_LIMIT_PER_BATCH = 500;
    public static final int DEFAULT_MAX_BATCH_COUNTER_PER_RUN = 100;
    public static final int DEFAULT_BULK_INSERT_SIZE = 10;
    // by default push small attachments as inline base64, and larger ones as multipart
    public static final PushAttachmentsInline DEFAULT_PUSH_ATTACHMENTS_INLINE = PushAttachmentsInline.Small;

    final int changeLimitPerBatch;
    final int batchLimitPerRun;
    final int bulkInsertSize;
    final PushAttachmentsInline pushAttachmentsInline;

    CouchDB targetDb;
    DatastoreWrapper sourceDb;

    private int documentCounter = 0;
    private int batchCounter = 0;

    private final String name;

    // Flag to stop the replication thread.
    // Volatile as might be set from another thread.
    private volatile boolean cancel;

    public final EventBus eventBus = new EventBus();
    
    /**
     * Flag is set when the replication process is complete. The thread
     * may live on because the listener's callback is executed on the thread.
     */
    private volatile boolean replicationTerminated = false;

    private static JSONHelper sJsonHelper = new JSONHelper();


    public BasicPushStrategy(PushReplication pushReplication) {
            this(pushReplication,
                    DEFAULT_CHANGES_LIMIT_PER_BATCH,
                    DEFAULT_MAX_BATCH_COUNTER_PER_RUN,
                    DEFAULT_BULK_INSERT_SIZE,
                    DEFAULT_PUSH_ATTACHMENTS_INLINE);
    }

    public BasicPushStrategy (PushReplication pushReplication,
                              int changeLimitPerBatch,
                              int batchLimitPerRun,
                              int insertBatchSize,
                              PushAttachmentsInline pushAttachmentsInline){
        Preconditions.checkNotNull(pushReplication, "PushReplication must not be null.");
        CouchConfig couchConfig = pushReplication.getCouchConfig();
        this.changeLimitPerBatch = changeLimitPerBatch;
        this.batchLimitPerRun = batchLimitPerRun;
        this.bulkInsertSize = insertBatchSize;
        this.pushAttachmentsInline = pushAttachmentsInline;

        this.targetDb = new CouchClientWrapper(couchConfig);
        this.sourceDb = new DatastoreWrapper((DatastoreExtended) pushReplication.source);

        this.name = String.format("%s [%s]", LOG_TAG, pushReplication.getReplicatorName());
    }

    @Override
    public boolean isReplicationTerminated() {
        return replicationTerminated;
    }

    @Override
    public void setCancel() {
        this.cancel = true;
    }

    @Override
    public int getDocumentCounter() {
        return this.documentCounter;
    }

    @Override
    public int getBatchCounter() {
        return this.batchCounter;
    }

    /**
     * Handle exceptions in separate run() method to allow replicate() to
     * just return when cancel is set to true rather than having to keep
     * track of whether we terminated via an error in replicate().
     */
    @Override
    public void run() {

        ErrorInfo errorInfo = null;

        try {

            replicate();

        } catch (Throwable e) {
            logger.log(Level.SEVERE,String.format("Batch %s ended with error:", this.batchCounter),e);
            errorInfo = new ErrorInfo(e);
        }

        replicationTerminated = true;

        String msg = "Push replication terminated via ";
        msg += this.cancel? "cancel." : "completion.";

        // notify complete/errored on eventbus
        logger.info(msg + " Posting on EventBus.");
        if (errorInfo == null) {  // successful replication
            eventBus.post(new ReplicationStrategyCompleted(this));
        } else {
            eventBus.post(new ReplicationStrategyErrored(this, errorInfo));
        }
    }

    private void replicate()
            throws DatabaseNotFoundException, InterruptedException, ExecutionException, AttachmentException, DatastoreException {
        logger.info("Push replication started");
        long startTime = System.currentTimeMillis();

        // We were cancelled before we started
        if (this.cancel) { return; }

        if(!this.targetDb.exists()) {
            throw new DatabaseNotFoundException(
                    "Database not found: " + this.targetDb.getIdentifier());
        }

        this.documentCounter = 0;
        for(this.batchCounter = 1 ; this.batchCounter < this.batchLimitPerRun; this.batchCounter ++) {

            if (this.cancel) { return; }

            String msg = String.format(
                "Batch %s started (completed %s changes so far)",
                this.batchCounter,
                this.documentCounter
            );
            logger.info(msg);
            long batchStartTime = System.currentTimeMillis();

            Changes changes = getNextBatch();
            int changesProcessed = 0;

            // So we can check whether all changes were processed during
            // a log analysis.
            msg = String.format(
                    "Batch %s contains %s changes",
                    this.batchCounter,
                    changes.size()
            );
            logger.info(msg);

            if (changes.size() > 0) {
                changesProcessed = processOneChangesBatch(changes);
                this.documentCounter += changesProcessed;
            }

            long batchEndTime = System.currentTimeMillis();
            msg =  String.format(
                    "Batch %s completed in %sms (processed %s changes)",
                    this.batchCounter,
                    batchEndTime-batchStartTime,
                    changesProcessed
            );
            logger.info(msg);

            // This logic depends on the changes in the feed rather than the
            // changes we actually processed.
            if(changes.size() == 0) {
                break;
            }
        }

        long endTime = System.currentTimeMillis();
        long deltaTime = endTime - startTime;
        String msg =  String.format(
            "Push completed in %sms (%s total changes processed)",
            deltaTime,
            this.documentCounter
        );
        logger.info(msg);
    }

    private Changes getNextBatch() throws ExecutionException, InterruptedException , DatastoreException{
        long lastPushSequence = getLastCheckpointSequence();
        logger.fine("Last push sequence from remote database: " + lastPushSequence);
        return this.sourceDb.getDbCore().changes(lastPushSequence,
                this.changeLimitPerBatch);
    }

    /**
     * A small value class containing a set of documents to push, some
     * via multipart and some via _bulk_docs
     */
    private class ItemsToPush
    {
        public ItemsToPush() {
            serializedDocs = new ArrayList<String>();
            multiparts = new ArrayList<MultipartAttachmentWriter>();
        }

        List<String> serializedDocs;
        List<MultipartAttachmentWriter> multiparts;
    }

    private int processOneChangesBatch(Changes changes) throws AttachmentException, DatastoreException {

        int changesProcessed = 0;

        // Process the changes themselves in batches, where we post a batch
        // at a time to the remote database's _bulk_docs endpoint.
        List<List<BasicDocumentRevision>> batches = Lists.partition(
                changes.getResults(),
                this.bulkInsertSize
        );
        for (List<BasicDocumentRevision> batch : batches) {

            if (this.cancel) { break; }

            Map<String, DocumentRevisionTree> allTrees = this.sourceDb.getDocumentTrees(batch);
            Map<String, Set<String>> docOpenRevs = this.openRevisions(allTrees);
            Map<String, CouchClient.MissingRevisions> docMissingRevs = this.targetDb.revsDiff(docOpenRevs);

            ItemsToPush itemsToPush = missingRevisionsToJsonDocs(allTrees, docMissingRevs);
            List<String> serialisedMissingRevs = itemsToPush.serializedDocs;
            List<MultipartAttachmentWriter> multiparts = itemsToPush.multiparts;

            if (!this.cancel) {
                this.targetDb.putMultiparts(multiparts);
                this.targetDb.bulkSerializedDocs(serialisedMissingRevs);
                changesProcessed += docMissingRevs.size();
            }
        }

        if (!this.cancel) {
            try {
                this.putCheckpoint(String.valueOf(changes.getLastSequence()));
            } catch (DatastoreException e){
                logger.log(Level.WARNING,"Failed to put checkpoint doc, next replication will " +
                        "start from previous checkpoint",e);
            }
        }

        return changesProcessed;
    }

    /**
     * Generate serialised JSON strings and/or MIME multipart/related writer objects for revisions
     * which are missing on the server
     *
     * @param allTrees batch of document trees, keyed by document id, in local database
     * @param revisions {@code MissingRevisions} objects, keyed by document id, as returned from
     *                  remote database by querying revs_diff endpoint.
     *
     * @return {@code ItemsToPush} object representing serialised JSON strings and/or MIME
     *         multipart/related writer
     *
     * @throws AttachmentException
     *
     * @see com.cloudant.mazha.CouchClient.MissingRevisions
     * @see com.cloudant.sync.replication.BasicPushStrategy.ItemsToPush
     */
    private ItemsToPush missingRevisionsToJsonDocs(
            Map<String, DocumentRevisionTree> allTrees,
            Map<String, CouchClient.MissingRevisions> revisions) throws AttachmentException {

        ItemsToPush itemsToPush = new ItemsToPush();

        for(Map.Entry<String, CouchClient.MissingRevisions> e : revisions.entrySet()) {
            String docId = e.getKey();
            Set<String> missingRevisions = e.getValue().missing;
            DocumentRevisionTree tree = allTrees.get(docId);
            for(String rev : missingRevisions) {
                long sequence = tree.lookup(docId, rev).getSequence();
                List<BasicDocumentRevision> path = tree.getPathForNode(sequence);

                // get the attachments for the leaf of this path
                BasicDocumentRevision dr = path.get(0);
                List<? extends Attachment> atts = this.sourceDb.getDbCore().attachmentsForRevision(dr);

                // get common ancestor generation - needed to correctly stub out attachments
                // closest back (first) instance of one of the possible ancestors rev id in the history tree
                int minRevPos = 0;
                for (BasicDocumentRevision ancestor : path) {
                    if (e.getValue().possible_ancestors != null &&
                            e.getValue().possible_ancestors.contains(ancestor.getRevision())) {
                        minRevPos = ancestor.getGeneration();
                    }
                }

                // get the json, and inline any small attachments
                Map<String, Object> json = RevisionHistoryHelper.revisionHistoryToJson(path,
                        atts,
                        this.pushAttachmentsInline,
                        minRevPos);
                // if there are any large atts we will get a multipart writer, otherwise null
                MultipartAttachmentWriter mpw = RevisionHistoryHelper.createMultipartWriter(dr,
                        atts,
                        this.pushAttachmentsInline,
                        minRevPos);

                // now we will have either a multipart or a plain doc
                if (mpw == null) {
                    itemsToPush.serializedDocs.add(sJsonHelper.toJson(json));
                } else {
                    itemsToPush.multiparts.add(mpw);
                }
            }
        }

        return itemsToPush;
    }

    Map<String, Set<String>> openRevisions(Map<String, DocumentRevisionTree> trees) {
        Map<String, Set<String>> allOpenRevisions = new HashMap<String, Set<String>>();
        for(Map.Entry<String, DocumentRevisionTree> e : trees.entrySet()) {
            allOpenRevisions.put(e.getKey(), e.getValue().leafRevisionIds());
        }
        return allOpenRevisions;
    }

    public String getReplicationId() throws DatastoreException {
        HashMap<String, String> dict = new HashMap<String, String>();
        dict.put("source", this.sourceDb.getIdentifier());
        dict.put("target", this.targetDb.getIdentifier());
        // get raw SHA-1 of dictionary
        byte[] sha1Bytes = Misc.getSha1(new ByteArrayInputStream(JSONUtils.serializeAsBytes(dict)));
        // return SHA-1 as a hex string
        byte[] sha1Hex = new Hex().encode(sha1Bytes);
        return new String(sha1Hex);
    }

    private long getLastCheckpointSequence() throws DatastoreException {
        String lastSequence =  targetDb.getCheckpoint(this.getReplicationId());
        // As we are pretty sure the checkpoint is a number
        return Strings.isNullOrEmpty(lastSequence) ? 0 : Long.valueOf(lastSequence);
    }

    private void putCheckpoint(String checkpoint) throws DatastoreException {
        targetDb.putCheckpoint(this.getReplicationId(), checkpoint);
    }
    
    @Override
    public EventBus getEventBus() {
        return eventBus;
    }

}
