/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.replication;

import com.cloudant.http.HttpConnectionRequestInterceptor;
import com.cloudant.http.HttpConnectionResponseInterceptor;
import com.cloudant.mazha.ChangesResult;
import com.cloudant.mazha.CouchClient;
import com.cloudant.mazha.DocumentRevs;
import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.datastore.DocumentException;
import com.cloudant.sync.datastore.DocumentRevsList;
import com.cloudant.sync.datastore.PreparedAttachment;
import com.cloudant.sync.datastore.UnsavedStreamAttachment;
import com.cloudant.sync.util.JSONUtils;
import com.cloudant.sync.util.Misc;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.eventbus.EventBus;

import org.apache.commons.codec.binary.Hex;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

class BasicPullStrategy implements ReplicationStrategy {

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

        int documentCounter = 0;

        int batchCounter = 0;
    }

    private State state;

    private static final Logger logger = Logger.getLogger(BasicPullStrategy.class
            .getCanonicalName());

    private static final String LOG_TAG = "BasicPullStrategy";

    CouchDB sourceDb;

    PullFilter filter;

    DatastoreWrapper targetDb;

    private final String name;

    private final EventBus eventBus = new EventBus();

    // Is _bulk_get endpoint supported?
    private boolean useBulkGet = false;

    public int changeLimitPerBatch = 1000;

    public int batchLimitPerRun = 100;

    public int insertBatchSize = 100;

    public boolean pullAttachmentsInline = false;

    public BasicPullStrategy(URI source,
                             Datastore target,
                             PullFilter filter,
                             List<HttpConnectionRequestInterceptor> requestInterceptors,
                             List<HttpConnectionResponseInterceptor> responseInterceptors) {
        this.filter = filter;
        this.sourceDb = new CouchClientWrapper(new CouchClient(source, requestInterceptors,
                responseInterceptors));
        this.targetDb = new DatastoreWrapper((DatastoreExtended) target);
        String replicatorName;
        if (filter == null) {
            replicatorName = String.format("%s <-- %s ", target.getDatastoreName(), source);
        } else {
            replicatorName = String.format("%s <-- %s (%s)", target.getDatastoreName(), source,
                    filter.getName());
        }
        this.name = String.format("%s [%s]", LOG_TAG, replicatorName);
    }

    @Override
    public boolean isReplicationTerminated() {
        if (this.state != null) {
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
        if (this.state != null) {
            return this.state.documentCounter;
        } else {
            return 0;
        }
    }

    @Override
    public int getBatchCounter() {
        if (this.state != null) {
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

        ErrorInfo errorInfo = null;

        try {
            this.useBulkGet = sourceDb.isBulkSupported();
            replicate();

        } catch (ExecutionException ex) {
            logger.log(Level.SEVERE, String.format("Batch %s ended with error:", this.state
                    .batchCounter), ex);
            errorInfo = new ErrorInfo(ex.getCause());
        } catch (Throwable e) {
            logger.log(Level.SEVERE, String.format("Batch %s ended with error:", this.state
                    .batchCounter), e);
            errorInfo = new ErrorInfo(e);
        }

        runComplete(errorInfo);
    }

    private void runComplete(ErrorInfo errorInfo) {
        state.replicationTerminated = true;

        String msg = "Pull replication terminated via ";
        msg += this.state.cancel ? "cancel." : "completion.";

        // notify complete/errored on eventbus
        logger.info(msg + " Posting on EventBus.");
        if (errorInfo == null) {  // successful replication
            eventBus.post(new ReplicationStrategyCompleted(this));
        } else {
            eventBus.post(new ReplicationStrategyErrored(this, errorInfo));
        }
    }

    private void replicate()
            throws DatabaseNotFoundException, ExecutionException, InterruptedException,
            DocumentException, DatastoreException {
        logger.info("Pull replication started");
        long startTime = System.currentTimeMillis();

        // We were cancelled before we started
        if (this.state.cancel) {
            return;
        }

        if (!this.sourceDb.exists()) {
            throw new DatabaseNotFoundException(
                    "Database not found " + this.sourceDb.getIdentifier());
        }

        this.state.documentCounter = 0;
        for (this.state.batchCounter = 1; this.state.batchCounter < this.batchLimitPerRun; this
                .state.batchCounter++) {

            if (this.state.cancel) {
                return;
            }

            String msg = String.format(
                    "Batch %s started (completed %s changes so far)",
                    this.state.batchCounter,
                    this.state.documentCounter
            );
            logger.info(msg);
            long batchStartTime = System.currentTimeMillis();

            ChangesResultWrapper changeFeeds = this.nextBatch();
            int batchChangesProcessed = 0;

            // So we can check whether all changes were processed during
            // a log analysis.
            msg = String.format(
                    "Batch %s contains %s changes",
                    this.state.batchCounter,
                    changeFeeds.size()
            );
            logger.info(msg);

            if (changeFeeds.size() > 0) {
                batchChangesProcessed = processOneChangesBatch(changeFeeds);
                state.documentCounter += batchChangesProcessed;
            }

            long batchEndTime = System.currentTimeMillis();
            msg = String.format(
                    "Batch %s completed in %sms (batch was %s changes)",
                    this.state.batchCounter,
                    batchEndTime - batchStartTime,
                    batchChangesProcessed
            );
            logger.info(msg);

            // This logic depends on the changes in the feed rather than the
            // changes we actually processed.
            if (changeFeeds.size() < this.changeLimitPerBatch) {
                break;
            }
        }

        long endTime = System.currentTimeMillis();
        long deltaTime = endTime - startTime;
        String msg = String.format(
                "Pull completed in %sms (%s total changes processed)",
                deltaTime,
                this.state.documentCounter
        );
        logger.info(msg);
    }

    public static class BatchItem {

        public BatchItem(DocumentRevsList revsList,
                         HashMap<String[], List<PreparedAttachment>> attachments) {
            this.revsList = revsList;
            this.attachments = attachments;
        }

        public HashMap<String[], List<PreparedAttachment>> attachments;
        public DocumentRevsList revsList;
    }

    private int processOneChangesBatch(ChangesResultWrapper changeFeeds)
            throws ExecutionException, InterruptedException, DocumentException {
        String feed = String.format(
                "Change feed: { last_seq: %s, change size: %s}",
                changeFeeds.getLastSeq(),
                changeFeeds.getResults().size()
        );
        logger.info(feed);

        Multimap<String, String> openRevs = changeFeeds.openRevisions(0, changeFeeds.size());
        Map<String, Collection<String>> missingRevisions = this.targetDb.getDbCore().revsDiff
                (openRevs);

        int changesProcessed = 0;

        // Process the changes in batches
        List<String> ids = Lists.newArrayList(missingRevisions.keySet());
        List<List<String>> batches = Lists.partition(ids, this.insertBatchSize);


        for (List<String> batch : batches) {

            List<BatchItem> batchesToInsert = new ArrayList<BatchItem>();

            if (this.state.cancel) { break; }

            try {
                Iterable<DocumentRevsList> result = createTask(batch, missingRevisions);

                for (DocumentRevsList revsList : result) {
                    // We promise not to insert documents after cancel is set
                    if (this.state.cancel) {
                        break;
                    }

                    // attachments, keyed by docId and revId, so that
                    // we can add the attachments to the correct leaf
                    // nodes
                    HashMap<String[], List<PreparedAttachment>> atts = new HashMap<String[],
                            List<PreparedAttachment>>();

                    // now put together a list of attachments we need to download
                    if (!this.pullAttachmentsInline) {
                        try {
                            for (DocumentRevs documentRevs : revsList) {
                                Map<String, Object> attachments = documentRevs.getAttachments();
                                // keep track of attachments we are going to prepare
                                ArrayList<PreparedAttachment> preparedAtts = new
                                        ArrayList<PreparedAttachment>();
                                atts.put(new String[]{documentRevs.getId(), documentRevs.getRev()
                                }, preparedAtts);

                                for (String attachmentName : attachments.keySet()) {
                                    Map attachmentMetadata = (Map) attachments.get(attachmentName);
                                    int revpos = (Integer) attachmentMetadata.get("revpos");
                                    String contentType = (String) attachmentMetadata.get
                                            ("content_type");
                                    String encoding = (String) attachmentMetadata.get("encoding");
                                    long length = (Integer) attachmentMetadata.get("length");
                                    long encodedLength = 0; // encodedLength can default to 0 if
                                    // it's not encoded
                                    if (Attachment.getEncodingFromString(encoding) != Attachment
                                            .Encoding.Plain) {
                                        encodedLength = (Integer) attachmentMetadata.get
                                                ("encoded_length");
                                    }

                                    // do we already have the attachment @ this revpos?
                                    // look back up the tree for this document and see:
                                    // if we already have it, then we don't need to fetch it
                                    DocumentRevs.Revisions revs = documentRevs.getRevisions();
                                    int offset = revs.getStart() - revpos;
                                    if (offset >= 0 && offset < revs.getIds().size()) {
                                        String revId = String.valueOf(revpos) + "-" + revs.getIds
                                                ().get(offset);

                                        Attachment a = this.targetDb.getDbCore()
                                                .getAttachment(documentRevs.getId(), revId,
                                                        attachmentName);
                                        if (a != null) {
                                            // skip attachment, already got it
                                            continue;
                                        }

                                    }
                                    UnsavedStreamAttachment usa = this.sourceDb
                                            .getAttachmentStream(documentRevs.getId(),
                                                    documentRevs.getRev(), attachmentName,
                                                    contentType, encoding);

                                    // by preparing the attachment here, it is downloaded outside
                                    // of the database transaction
                                    preparedAtts.add(this.targetDb.prepareAttachment(usa, length,
                                            encodedLength));
                                }
                            }
                        } catch (Exception e) {
                            logger.log(Level.SEVERE,
                                    "There was a problem downloading an attachment to the" +
                                            " datastore, terminating replication",
                                    e);
                            this.state.cancel = true;
                        }
                    }

                    if (this.state.cancel) {
                        break;
                    }

                    batchesToInsert.add(new BatchItem(revsList, atts));
                    changesProcessed++;
                }
                this.targetDb.bulkInsert(batchesToInsert, this.pullAttachmentsInline);
            } catch (Exception e) {
                throw new ExecutionException(e);
            }
        }

        if (!this.state.cancel) {
            try {
                this.targetDb.putCheckpoint(this.getReplicationId(), changeFeeds.getLastSeq());
            } catch (DatastoreException e) {
                logger.log(Level.WARNING, "Failed to put checkpoint doc, next replication will " +
                        "start from previous checkpoint", e);
            }
        }

        return changesProcessed;
    }

    public String getReplicationId() throws DatastoreException {
        HashMap<String, String> dict = new HashMap<String, String>();
        dict.put("source", this.sourceDb.getIdentifier());
        dict.put("target", this.targetDb.getIdentifier());
        if (filter != null) {
            dict.put("filter", this.filter.toQueryString());
        }
        // get raw SHA-1 of dictionary
        byte[] sha1Bytes = Misc.getSha1(new ByteArrayInputStream(JSONUtils.serializeAsBytes(dict)));
        // return SHA-1 as a hex string
        byte[] sha1Hex = new Hex().encode(sha1Bytes);
        return new String(sha1Hex, Charset.forName("UTF-8"));
    }

    private ChangesResultWrapper nextBatch() throws DatastoreException {
        final Object lastCheckpoint = this.targetDb.getCheckpoint(this.getReplicationId());
        logger.fine("last checkpoint " + lastCheckpoint);
        ChangesResult changeFeeds = this.sourceDb.changes(
                this.filter,
                lastCheckpoint,
                this.changeLimitPerBatch);
        logger.finer("changes feed: " + JSONUtils.toPrettyJson(changeFeeds));
        return new ChangesResultWrapper(changeFeeds);
    }

    public Iterable<DocumentRevsList> createTask(List<String> ids,
                                                 Map<String, Collection<String>> revisions) {

        List<BulkGetRequest> requests = new ArrayList<BulkGetRequest>();

        for (String id : ids) {
            //skip any document with an empty id
            if (id.isEmpty()) {
                logger.info("Found document with empty ID in change feed, skipping");
                continue;
            }
            // get list for atts_since (these are possible ancestors we have, it's ok to be eager

            // and get all revision IDs higher up in the tree even if they're not our
            // ancestors and
            // belong to a different subtree)
            HashSet<String> possibleAncestors = new HashSet<String>();
            for (String revId : revisions.get(id)) {
                List<String> thesePossibleAncestors = targetDb.getDbCore()
                        .getPossibleAncestorRevisionIDs(id, revId, 50);
                if (thesePossibleAncestors != null) {
                    possibleAncestors.addAll(thesePossibleAncestors);
                }
            }
            requests.add(new BulkGetRequest(
                    id,
                    new ArrayList<String>(revisions.get(id)),
                    new ArrayList<String>(possibleAncestors)));
        }

        if (useBulkGet) {
            return new GetRevisionTaskBulk(this.sourceDb, requests, this.pullAttachmentsInline);
        } else {
            return new GetRevisionTaskThreaded(this.sourceDb, requests, this.pullAttachmentsInline);
        }
    }

    @Override
    public EventBus getEventBus() {
        return eventBus;
    }
}
