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

import com.cloudant.common.RetriableTask;
import com.cloudant.mazha.ChangesResult;
import com.cloudant.mazha.CouchConfig;
import com.cloudant.mazha.DocumentRevs;
import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.DocumentException;
import com.cloudant.sync.datastore.DocumentNotFoundException;
import com.cloudant.sync.datastore.DocumentRevsList;
import com.cloudant.sync.datastore.PreparedAttachment;
import com.cloudant.sync.datastore.UnsavedStreamAttachment;
import com.cloudant.sync.util.JSONUtils;
import com.cloudant.sync.util.Misc;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.eventbus.EventBus;

import org.apache.commons.codec.binary.Hex;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

class BasicPullStrategy implements ReplicationStrategy {

    private static final Logger logger = Logger.getLogger(BasicPullStrategy.class.getCanonicalName());
    private static final String LOG_TAG = "BasicPullStrategy";
    CouchDB sourceDb;
    Replication.Filter filter;
    DatastoreWrapper targetDb;

    ExecutorService executor;
    private PullConfiguration config;

    int documentCounter = 0;
    int batchCounter = 0;

    private final String name;

    // Flag to stop the replication thread.
    // Volatile as might be set from another thread.
    private volatile boolean cancel = false;
    
    private final EventBus eventBus = new EventBus();

    /**
     * Flag is set when the replication process is complete. The thread
     * may live on because the listener's callback is executed on the thread.
     */
    private volatile boolean replicationTerminated = false;

    public BasicPullStrategy(PullReplication pullReplication) {
        this(pullReplication, null, null);
    }

    public BasicPullStrategy(PullReplication pullReplication,
                             ExecutorService executorService,
                             PullConfiguration config) {
        Preconditions.checkNotNull(pullReplication, "PullReplication must not be null.");

        if(executorService == null) {
            executorService = new ThreadPoolExecutor(4, 4, 1, TimeUnit.MINUTES,
                    new LinkedBlockingQueue<Runnable>());
        }

        if(config == null) {
            config = new PullConfiguration();
        }

        this.executor = executorService;
        this.config = config;
        this.filter = pullReplication.filter;

        CouchConfig couchConfig = pullReplication.getCouchConfig();
        this.sourceDb = new CouchClientWrapper(couchConfig);
        this.targetDb = new DatastoreWrapper((DatastoreExtended) pullReplication.target);
        this.name = String.format("%s [%s]", LOG_TAG, pullReplication.getReplicatorName());
    }

    @Override
    public boolean isReplicationTerminated() {
        return replicationTerminated;
    }

    @Override
    public void setCancel() {
        this.cancel = true;

        // Don't process further tasks to hasten shutdown
        this.executor.shutdownNow();
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

        } catch (ExecutionException ex) {
            logger.log(Level.SEVERE,String.format("Batch %s ended with error:", this.batchCounter),ex);
            errorInfo = new ErrorInfo(ex.getCause());
        } catch (Throwable e) {
            logger.log(Level.SEVERE,String.format("Batch %s ended with error:", this.batchCounter),e);
            errorInfo = new ErrorInfo(e);
        } finally {
            this.executor.shutdownNow();
        }

        // Give the in-flight HTTP requests time to complete. It's not vital
        // for correctness that we do this, but nice to give the remote server
        // some breathing room.
        try {
            this.executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // do nothing
        }

        replicationTerminated = true;

        String msg = "Pull replication terminated via ";
        msg += this.cancel? "cancel." : "completion.";

        // notify complete/errored on eventbus
        logger.info(msg + " Posting on EventBus.");
        if (errorInfo == null) {  // successful replication
            eventBus.post(new ReplicationStrategyCompleted(this));
        } else {
            eventBus.post(new ReplicationStrategyErrored(this, errorInfo));
        }
        
    }

    // Inner class used to fetch and process a batch of changes. See comment in replicate() for
    // more details.
    private class BatchProcessor implements Callable<BatchProcessor> {

        ChangesResultWrapper changeFeeds;
        int batchChangesProcessed;

        public BatchProcessor call() throws ExecutionException, InterruptedException, DocumentException, DatastoreException {

            if (cancel) {return this;}

            changeFeeds = nextBatch();
            batchChangesProcessed = 0;

            // So we can check whether all changes were processed during
            // a log analysis.
            String msg = String.format(
                    "Batch %s contains %s changes",
                    batchCounter,
                    changeFeeds.size()
            );
            logger.info(msg);

            if (changeFeeds.size() > 0) {
                batchChangesProcessed = processOneChangesBatch(changeFeeds);
            }
            return this;
        }
    }

    private void replicate()
            throws DatabaseNotFoundException, ExecutionException, InterruptedException, DocumentException, DatastoreException {
        logger.info("Pull replication started");
        long startTime = System.currentTimeMillis();

        // We were cancelled before we started
        if (this.cancel) { return; }

        if(!this.sourceDb.exists()) {
            throw new DatabaseNotFoundException(
                    "Database not found " + this.sourceDb.getIdentifier());
        }

        this.documentCounter = 0;
        for (this.batchCounter = 1; this.batchCounter < config.batchLimitPerRun; this.batchCounter++) {

            if (this.cancel) { return; }

            String msg = String.format(
                    "Batch %s started (completed %s changes so far)",
                    this.batchCounter,
                    this.documentCounter
            );
            logger.info(msg);
            long batchStartTime = System.currentTimeMillis();
            BatchProcessor batchProcessor = new BatchProcessor();

            // Call the BatchProcessor to fetch the next batch from the changes feed and then process
            // them. Retry this in case the changes feed is inconsistent with the revisions available.
            // See BasicPullStrategyTest#pull_changesNewerThanOpenRevs for more explanation of this
            // scenario.
            //
            // Essentially the BatchProcessor is just a wrapper for the Callable#call method and so
            // we can access changeFeeds and batchChangesProcessed fields after successful execution
            //
            // Note that it is OK to call nextBatch() repeatedly for each retry without missing any
            // batches - this is because we know that the checkpoint has not been updated.
            //
            // Since RetriableTask#call can throw Exception, we have to re-throw the exceptions we
            // are expecting, plus wrapping and throwing any that we aren't.
            try {
                new RetriableTask<BatchProcessor>(batchProcessor).call();
            } catch (ExecutionException e) {
                throw e;
            } catch (InterruptedException e) {
                throw e;
            } catch (DocumentException e) {
                throw e;
            } catch (DatastoreException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            long batchEndTime = System.currentTimeMillis();
            documentCounter += batchProcessor.batchChangesProcessed;
            msg =  String.format(
                    "Batch %s completed in %sms (batch was %s changes)",
                    this.batchCounter,
                    batchEndTime-batchStartTime,
                    batchProcessor.batchChangesProcessed
            );
            logger.info(msg);

            // This logic depends on the changes in the feed rather than the
            // changes we actually processed.
            if (batchProcessor.changeFeeds.size() < this.config.changeLimitPerBatch) {
                break;
            }
        }

        long endTime = System.currentTimeMillis();
        long deltaTime = endTime - startTime;
        String msg =  String.format(
            "Pull completed in %sms (%s total changes processed)",
            deltaTime,
            this.documentCounter
        );
        logger.info(msg);
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
        Map<String, Collection<String>> missingRevisions = this.targetDb.getDbCore().revsDiff(openRevs);

        int changesProcessed = 0;

        // Process the changes in batches
        List<String> ids = Lists.newArrayList(missingRevisions.keySet());
        List<List<String>> batches = Lists.partition(ids, this.config.insertBatchSize);
        for (List<String> batch : batches) {

            if (this.cancel) { break; }

            List<Callable<DocumentRevsList>> tasks = createTasks(batch, missingRevisions);
            try {
                List<Future<DocumentRevsList>> futures = executor.invokeAll(tasks);
                for(Future<DocumentRevsList> future : futures) {
                    DocumentRevsList result = future.get();

                    // We promise not to insert documents after cancel is set
                    if (this.cancel) { break; }

                    // attachments, keyed by docId and revId, so that
                    // we can add the attachments to the correct leaf
                    // nodes
                    HashMap<String[], List<PreparedAttachment>> atts = new HashMap<String[], List<PreparedAttachment>>();

                    // now put together a list of attachments we need to download
                    if (!config.pullAttachmentsInline) {
                        try {
                            for (DocumentRevs documentRevs : result) {
                                Map<String, Object> attachments = documentRevs.getAttachments();
                                // keep track of attachments we are going to prepare
                                ArrayList<PreparedAttachment> preparedAtts = new ArrayList<PreparedAttachment>();
                                atts.put(new String[]{documentRevs.getId(), documentRevs.getRev()}, preparedAtts);

                                for (String attachmentName : attachments.keySet()) {
                                    Map attachmentMetadata = (Map)attachments.get(attachmentName);
                                    int revpos = (Integer) attachmentMetadata.get("revpos");
                                    String contentType = (String) attachmentMetadata.get("content_type");
                                    String encoding = (String) attachmentMetadata.get("encoding");
                                    long length = (Integer) attachmentMetadata.get("length");
                                    long encodedLength = 0; // encodedLength can default to 0 if it's not encoded
                                    if (Attachment.getEncodingFromString(encoding) != Attachment
                                            .Encoding.Plain) {
                                        encodedLength = (Integer) attachmentMetadata.get("encoded_length");
                                    }

                                    // do we already have the attachment @ this revpos?
                                    // look back up the tree for this document and see:
                                    // if we already have it, then we don't need to fetch it
                                    DocumentRevs.Revisions revs = documentRevs.getRevisions();
                                    int offset = revs.getStart() - revpos;
                                    if (offset >= 0 && offset < revs.getIds().size()) {
                                        String revId = String.valueOf(revpos) + "-" + revs.getIds().get(offset);
                                        try {
                                            BasicDocumentRevision dr = this.targetDb.getDbCore().getDocument(documentRevs.getId(), revId);
                                                Attachment a = this.targetDb.getDbCore()
                                                        .getAttachment(dr, attachmentName);
                                                if (a != null) {
                                                    // skip attachment, already got it
                                                    continue;
                                                }
                                        } catch (DocumentNotFoundException e){
                                            //do nothing, we may not have the document yet
                                        }
                                    }
                                    UnsavedStreamAttachment usa = this.sourceDb.getAttachmentStream(documentRevs.getId(), documentRevs.getRev(), attachmentName, contentType, encoding);

                                    // by preparing the attachment here, it is downloaded outside of the database transaction
                                    preparedAtts.add(this.targetDb.prepareAttachment(usa, length, encodedLength));
                                }
                            }
                        } catch (Exception e) {
                            logger.log(Level.SEVERE,
                                    "There was a problem downloading an attachment to the" +
                                            " datastore, terminating replication",
                                    e);
                            this.cancel = true;
                        }
                    }

                    if (this.cancel)
                        break;

                    this.targetDb.bulkInsert(result, atts, config.pullAttachmentsInline);
                    changesProcessed++;
                }
            } catch (InterruptedException ex) {
                // invokeAll() or future.get() was interrupted, expected on
                // cancelling as shutdownNow is called in setCancel()
                if (this.cancel) {
                    break;
                } else {
                    throw ex;
                }
            }
        }

        if (!this.cancel) {
            try {
                this.targetDb.putCheckpoint(this.getReplicationId(), changeFeeds.getLastSeq());
            } catch (DatastoreException e){
                logger.log(Level.WARNING,"Failed to put checkpoint doc, next replication will start from previous checkpoint",e);
            }
        }

        return changesProcessed;
    }

    public String getReplicationId() throws DatastoreException {
        HashMap<String, String> dict = new HashMap<String, String>();
        dict.put("source", this.sourceDb.getIdentifier());
        dict.put("target", this.targetDb.getIdentifier());
        if(filter != null) {
            dict.put("filter", this.filter.toQueryString());
        }
        // get raw SHA-1 of dictionary
        byte[] sha1Bytes = Misc.getSha1(new ByteArrayInputStream(JSONUtils.serializeAsBytes(dict)));
        // return SHA-1 as a hex string
        byte[] sha1Hex = new Hex().encode(sha1Bytes);
        return new String(sha1Hex);
    }

    private ChangesResultWrapper nextBatch() throws DatastoreException {
        final Object lastCheckpoint = this.targetDb.getCheckpoint(this.getReplicationId());
        logger.fine("last checkpoint "+lastCheckpoint);
        ChangesResult changeFeeds = this.sourceDb.changes(
                filter,
                lastCheckpoint,
                this.config.changeLimitPerBatch);
        logger.finer("changes feed: "+JSONUtils.toPrettyJson(changeFeeds));
        return new ChangesResultWrapper(changeFeeds);
    }

    public List<Callable<DocumentRevsList>> createTasks(List<String> ids,
                                                        Map<String, Collection<String>> revisions) {


        List<Callable<DocumentRevsList>> tasks = new ArrayList<Callable<DocumentRevsList>>();
        for(String id : ids) {
            //skip any document with an empty id
            if(id.isEmpty()){
                logger.info("Found document with empty ID in change feed, skipping");
                continue;
            }
            // get list for atts_since (these are possible ancestors we have, it's ok to be eager
            // and get all revision IDs higher up in the tree even if they're not our ancestors and
            // belong to a different subtree)
            HashSet<String> possibleAncestors = new HashSet<String>();
            for (String revId : revisions.get(id)) {
                List<String> thesePossibleAncestors = targetDb.getDbCore().getPossibleAncestorRevisionIDs(id, revId, 50);
                if (thesePossibleAncestors != null) {
                    possibleAncestors.addAll(thesePossibleAncestors);
                }
            }
            tasks.add(GetRevisionTask.createGetRevisionTask(this.sourceDb,
                    id,
                    revisions.get(id),
                    possibleAncestors,
                    config.pullAttachmentsInline));
        }
        return tasks;
    }
    
    @Override
    public EventBus getEventBus() {
        return eventBus;
    }
}
