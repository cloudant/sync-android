/*
 * Copyright © 2013, 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.internal.replication;

import com.cloudant.http.HttpConnectionRequestInterceptor;
import com.cloudant.http.HttpConnectionResponseInterceptor;
import com.cloudant.sync.documentstore.Attachment;
import com.cloudant.sync.documentstore.AttachmentException;
import com.cloudant.sync.documentstore.Changes;
import com.cloudant.sync.documentstore.Database;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.event.EventBus;
import com.cloudant.sync.internal.documentstore.ChangesImpl;
import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.internal.documentstore.DocumentRevisionTree;
import com.cloudant.sync.internal.documentstore.InternalDocumentRevision;
import com.cloudant.sync.internal.documentstore.MultipartAttachmentWriter;
import com.cloudant.sync.internal.documentstore.RevisionHistoryHelper;
import com.cloudant.sync.internal.mazha.CouchClient;
import com.cloudant.sync.internal.util.CollectionUtils;
import com.cloudant.sync.internal.util.JSONUtils;
import com.cloudant.sync.internal.util.Misc;
import com.cloudant.sync.replication.DatabaseNotFoundException;
import com.cloudant.sync.replication.PushAttachmentsInline;
import com.cloudant.sync.replication.PushFilter;

import org.apache.commons.codec.binary.Hex;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @api_private
 */
public class PushStrategy implements ReplicationStrategy {

    // internal state which gets reset each time run() is called
    private static class State {
        // Flag to stop the replication thread.
        // Volatile as might be set from another thread.
        private volatile boolean cancel = false;

        /**
         * Flag is set when the replication process is complete. The thread
         * may live on because the listener's callback is executed on the thread.
         */
        private volatile boolean replicationTerminated = false;

        private int documentCounter = 0;

        private int batchCounter = 0;
    }

    private State state;

    private static final String LOG_TAG = "PushStrategy";

    private static final Logger logger = Logger.getLogger(PushStrategy.class.getCanonicalName());

    CouchDB targetDb;

    DatastoreWrapper sourceDb;

    private final String name;

    public final EventBus eventBus = new EventBus();

    public int changeLimitPerBatch = 500;

    public int bulkInsertSize = 10;

    public PushFilter filter = null;

    public PushAttachmentsInline pushAttachmentsInline = PushAttachmentsInline.Small;

    public PushStrategy(Database source,
                        URI target,
                        List<HttpConnectionRequestInterceptor> requestInterceptors,
                        List<HttpConnectionResponseInterceptor> responseInterceptors) {
        this.sourceDb = new DatastoreWrapper((DatabaseImpl) source);
        this.targetDb = new CouchClientWrapper(new CouchClient(target, requestInterceptors, responseInterceptors));
        String replicatorName = String.format("%s <-- %s ", target, source.getPath());
        this.name = String.format("%s [%s]", LOG_TAG, replicatorName);
    }

    @Override
    public boolean isReplicationTerminated() {
        if (state != null) {
            return state.replicationTerminated;
        } else {
            return false;
        }
    }

    @Override
    public void setCancel() {
        // if we have been cancelled before run(), we have to create the internal state
        if (this.state == null) {
            this.state = new State();
        }
        this.state.cancel = true;
    }

    @Override
    public int getDocumentCounter() {
        if (state != null) {
            return this.state.documentCounter;
        } else {
            return 0;
        }
    }

    @Override
    public int getBatchCounter() {
        if (state != null) {
            return this.state.batchCounter;
        } else {
            return 0;
        }
    }

    /**
     * Handle exceptions in separate run() method to allow replicate() to
     * just return when cancel is set to true rather than having to keep
     * track of whether we terminated via an error in replicate().
     */
    @Override
    public void run() {

        if (this.state != null && this.state.cancel) {
            // we were already cancelled, don't run, but still post completion
            this.state.documentCounter = 0;
            this.state.batchCounter = 0;
            runComplete(null);
            return;
        }
        // reset internal state
        this.state = new State();

        Throwable errorInfo = null;

        try {

            replicate();

        } catch (Throwable e) {
            logger.log(Level.SEVERE,String.format("Batch %s ended with error:", this.state.batchCounter),e);
            errorInfo = e;
        }

        runComplete(errorInfo);
    }

    private void runComplete(Throwable errorInfo) {
        state.replicationTerminated = true;

        String msg = "Push replication terminated via ";
        msg += this.state.cancel? "cancel." : "completion.";

        // notify complete/errored on eventbus
        logger.info(msg + " Posting on EventBus.");
        if (errorInfo == null) {  // successful replication
            eventBus.post(new ReplicationStrategyCompleted(this));
        } else {
            eventBus.post(new ReplicationStrategyErrored(this, errorInfo));
        }
    }

    private void replicate()
            throws DatabaseNotFoundException, InterruptedException, ExecutionException,
            AttachmentException, DocumentStoreException {
        logger.info("Push replication started");
        long startTime = System.currentTimeMillis();

        // We were cancelled before we started
        if (this.state.cancel) {
            return;
        }

        if (!this.targetDb.exists()) {
            throw new DatabaseNotFoundException(
                    "Database not found: " + this.targetDb.getIdentifier());
        }

        this.state.documentCounter = 0;
        while (!this.state.cancel) {
            this.state.batchCounter++;


            String msg = String.format(
                    "Batch %s started (completed %s changes so far)",
                    this.state.batchCounter,
                    this.state.documentCounter
            );
            logger.info(msg);
            long batchStartTime = System.currentTimeMillis();

            // Get the next batch of changes and record the size and last sequence
            Changes changes = getNextBatch();
            final int unfilteredChangesSize = changes.getResults().size();
            final long lastSeq = changes.getLastSequence();

            // Count the number of changes processed
            int changesProcessed = 0;

            // If there is a filter replace the changes with the filtered list of changes
            if (this.filter != null) {
                List<DocumentRevision> allowedChanges = new ArrayList<DocumentRevision>(changes
                        .getResults().size());

                for (DocumentRevision revision : changes.getResults()) {
                    if (this.filter.shouldReplicateDocument(revision)) {
                        allowedChanges.add(revision);
                    }
                }

                changes = new FilteredChanges(changes.getLastSequence(), allowedChanges);
            }
            final int filteredChangesSize = changes.getResults().size();

            // So we can check whether all changes were processed during
            // a log analysis.
            msg = String.format(
                    "Batch %s contains %s changes",
                    this.state.batchCounter,
                    filteredChangesSize
            );
            logger.info(msg);

            if (filteredChangesSize > 0) {
                changesProcessed = processOneChangesBatch(changes);
                this.state.documentCounter += changesProcessed;
            }

            // If not cancelled and there were any changes set a checkpoint
            if (!this.state.cancel && unfilteredChangesSize > 0) {
                try {
                    this.putCheckpoint(String.valueOf(lastSeq));
                } catch (DocumentStoreException e) {
                    logger.log(Level.WARNING, "Failed to put checkpoint doc, next replication " +
                            "will " +
                            "start from previous checkpoint", e);
                }
            }

            long batchEndTime = System.currentTimeMillis();
            msg = String.format(
                    "Batch %s completed in %sms (processed %s changes)",
                    this.state.batchCounter,
                    batchEndTime - batchStartTime,
                    changesProcessed
            );
            logger.info(msg);

            // This logic depends on the changes in the feed rather than the
            // changes we actually processed.
            if (unfilteredChangesSize == 0) {
                break;
            }
        }

        long endTime = System.currentTimeMillis();
        long deltaTime = endTime - startTime;
        String msg;
        if (this.state.cancel) {
            msg = String.format(Locale.ENGLISH,
                            "Push canceled after %sms (%s changes processed)",
                            deltaTime,
                            this.state.documentCounter
                    );
        } else {
            msg = String.format(Locale.ENGLISH,
                    "Push completed in %sms (%s total changes processed)",
                    deltaTime,
                    this.state.documentCounter
            );
        }
        logger.info(msg);
    }

    private Changes getNextBatch() throws ExecutionException, InterruptedException, DocumentStoreException {
        long lastPushSequence = getLastCheckpointSequence();
        logger.fine("Last push sequence from remote database: " + lastPushSequence);
        return this.sourceDb.getDbCore().changes(lastPushSequence, this.changeLimitPerBatch);
    }

    private static class FilteredChanges extends ChangesImpl {
        public FilteredChanges(long lastSequence, List<DocumentRevision> results) {
            super(lastSequence, results);
        }
    }

    /**
     * A small value class containing a set of documents to push, some
     * via multipart and some via _bulk_docs
     */
    private static class ItemsToPush
    {
        public ItemsToPush() {
            serializedDocs = new ArrayList<String>();
            multiparts = new ArrayList<MultipartAttachmentWriter>();
        }

        List<String> serializedDocs;
        List<MultipartAttachmentWriter> multiparts;
    }

    private int processOneChangesBatch(Changes changes) throws AttachmentException, DocumentStoreException {

        int changesProcessed = 0;

        // Process the changes themselves in batches, where we post a batch
        // at a time to the remote database's _bulk_docs endpoint.
        List<? extends List<DocumentRevision>> batches = CollectionUtils.partition(
                changes.getResults(),
                this.bulkInsertSize
        );
        for (List<DocumentRevision> batch : batches) {

            if (this.state.cancel) { break; }

            Map<String, DocumentRevisionTree> allTrees = this.sourceDb.getDocumentTrees(batch);
            Map<String, Set<String>> docOpenRevs = this.openRevisions(allTrees);
            Map<String, CouchClient.MissingRevisions> docMissingRevs = this.targetDb.revsDiff(docOpenRevs);

            ItemsToPush itemsToPush = missingRevisionsToJsonDocs(allTrees, docMissingRevs);
            List<String> serialisedMissingRevs = itemsToPush.serializedDocs;
            List<MultipartAttachmentWriter> multiparts = itemsToPush.multiparts;

            if (!this.state.cancel) {
                this.targetDb.putMultiparts(multiparts);
                this.targetDb.bulkCreateSerializedDocs(serialisedMissingRevs);
                changesProcessed += docMissingRevs.size();
            }
        }

        return changesProcessed;
    }

    /**
     * Generate serialised JSON strings and/or MIME multipart/related writer objects for revisions
     * which are missing on the server
     *
     * @param allTrees batch of document trees, keyed by document ID, in local database
     * @param revisions {@code MissingRevisions} objects, keyed by document ID, as returned from
     *                  remote database by querying revs_diff endpoint.
     *
     * @return {@code ItemsToPush} object representing serialised JSON strings and/or MIME
     *         multipart/related writer
     *
     * @throws AttachmentException
     *
     * @see CouchClient.MissingRevisions
     * @see PushStrategy.ItemsToPush
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
                List<InternalDocumentRevision> path = tree.getPathForNode(sequence);

                // get the attachments for the leaf of this path
                InternalDocumentRevision dr = path.get(0);
                Map<String, ? extends Attachment> atts = this.sourceDb.getDbCore().attachmentsForRevision(dr);

                // get common ancestor generation - needed to correctly stub out attachments
                // closest back (first) instance of one of the possible ancestors rev ID in the history tree
                int minRevPos = 0;
                for (InternalDocumentRevision ancestor : path) {
                    if (e.getValue().possible_ancestors != null &&
                            e.getValue().possible_ancestors.contains(ancestor.getRevision())) {
                        minRevPos = ancestor.getGeneration();
                    }
                }

                // do we inline all attachments as base64 or send them all via multipart writer?
                boolean shouldInline = RevisionHistoryHelper.shouldInline(atts,
                        this.pushAttachmentsInline,
                        minRevPos);
                // get the json, and inline any small attachments
                Map<String, Object> json = RevisionHistoryHelper.revisionHistoryToJson(path,
                        atts,
                        shouldInline,
                        minRevPos);
                // if there are any large atts we will get a multipart writer, otherwise null
                MultipartAttachmentWriter mpw = RevisionHistoryHelper.createMultipartWriter(json,
                        atts,
                        shouldInline,
                        minRevPos);

                // now we will have either a multipart or a plain doc
                if (mpw == null) {
                    itemsToPush.serializedDocs.add(JSONUtils.toJson(json));
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

    public String getReplicationId() throws DocumentStoreException {
        HashMap<String, String> dict = new HashMap<String, String>();
        dict.put("source", this.sourceDb.getIdentifier());
        dict.put("target", this.targetDb.getIdentifier());
        // get raw SHA-1 of dictionary
        try {
            byte[] sha1Bytes = Misc.getSha1(new ByteArrayInputStream(JSONUtils.serializeAsBytes(dict)));
            // return SHA-1 as a hex string
            byte[] sha1Hex = new Hex().encode(sha1Bytes);
            return new String(sha1Hex, Charset.forName("UTF-8"));
        } catch (IOException ioe) {
            throw new DocumentStoreException(ioe);
        }
    }

    private long getLastCheckpointSequence() throws DocumentStoreException {
        String lastSequence =  targetDb.getCheckpoint(this.getReplicationId());
        // As we are pretty sure the checkpoint is a number
        return Misc.isStringNullOrEmpty(lastSequence) ? 0 : Long.parseLong(lastSequence);
    }

    private void putCheckpoint(String checkpoint) throws DocumentStoreException {
        targetDb.putCheckpoint(this.getReplicationId(), checkpoint);
    }
    
    @Override
    public EventBus getEventBus() {
        return eventBus;
    }

    @Override
    public String getRemote() {
        return this.targetDb.getIdentifier();
    }
}
