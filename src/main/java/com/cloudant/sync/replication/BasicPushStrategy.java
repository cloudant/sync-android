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

import com.cloudant.common.Log;
import com.cloudant.mazha.CouchConfig;
import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.Changes;
import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.datastore.DocumentRevisionTree;
import com.cloudant.sync.datastore.RevisionHistoryHelper;
import com.cloudant.sync.datastore.SavedAttachment;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

class BasicPushStrategy implements ReplicationStrategy {

    private static final String LOG_TAG = "BasicPushStrategy";

    CouchDB targetDb;
    DatastoreWrapper sourceDb;

    private final PushConfiguration config;

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

    public BasicPushStrategy(PushReplication pushReplication) {
        this(pushReplication, null);
    }

    public BasicPushStrategy(PushReplication pushReplication,
                             PushConfiguration config) {
        Preconditions.checkNotNull(pushReplication, "PushReplication must not be null.");
        if(config == null) {
            config = new PushConfiguration();
        }

        String dbName = pushReplication.getTargetDbName();
        CouchConfig couchConfig = pushReplication.getCouchConfig();

        this.targetDb = new CouchClientWrapper(dbName, couchConfig);
        this.sourceDb = new DatastoreWrapper((DatastoreExtended) pushReplication.source);
        // Push config is immutable
        this.config = config;

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

    public int getDocumentCounter() {
        return this.documentCounter;
    }

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
            Log.e(
                    this.name,
                    String.format("Batch %s ended with error:", this.batchCounter),
                    e
            );

            errorInfo = new ErrorInfo(e);
        }

        replicationTerminated = true;

        String msg = "Push replication terminated via ";
        msg += this.cancel? "cancel." : "completion.";

        // notify complete/errored on eventbus
        Log.i(this.name, msg + " Posting on EventBus.");
        if (errorInfo == null) {  // successful replication
            eventBus.post(new ReplicationStrategyCompleted(this));
        } else {
            eventBus.post(new ReplicationStrategyErrored(this, errorInfo));
        }
    }

    private void replicate()
            throws DatabaseNotFoundException, InterruptedException, ExecutionException {
        Log.i(this.name, "Push replication started");
        long startTime = System.currentTimeMillis();

        // We were cancelled before we started
        if (this.cancel) { return; }

        if(!this.targetDb.exists()) {
            throw new DatabaseNotFoundException(
                    "Database not found: " + this.targetDb.getDbName());
        }

        this.documentCounter = 0;
        for(this.batchCounter = 1 ; this.batchCounter < config.batchLimitPerRun; this.batchCounter ++) {

            if (this.cancel) { return; }

            String msg = String.format(
                "Batch %s started (completed %s changes so far)",
                this.batchCounter,
                this.documentCounter
            );
            Log.i(this.name, msg);
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
            Log.i(this.name, msg);

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
            Log.i(this.name, msg);

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
        Log.i(this.name, msg);
    }

    private Changes getNextBatch() throws ExecutionException, InterruptedException {
        long lastPushSequence = getLastCheckpointSequence();
        Log.d(this.name, "Last push sequence from remote database: " + lastPushSequence);
        return this.sourceDb.getDbCore().changes(lastPushSequence,
                config.changeLimitPerBatch);
    }

    private int processOneChangesBatch(Changes changes) {

        int changesProcessed = 0;

        // Process the changes themselves in batches, where we post a batch
        // at a time to the remote database's _bulk_docs endpoint.
        List<List<DocumentRevision>> batches = Lists.partition(
                changes.getResults(),
                config.bulkInsertSize
        );
        for (List<DocumentRevision> batch : batches) {

            if (this.cancel) { break; }

            Map<String, DocumentRevisionTree> allTrees = this.sourceDb.getDocumentTrees(batch);
            Map<String, Set<String>> docOpenRevs = this.openRevisions(allTrees);
            Map<String, Set<String>> docMissingRevs = this.targetDb.revsDiff(docOpenRevs);
            // get attachments for these revs?

            List<String> serialisedMissingRevs = missingRevisionsToJsonDocs(allTrees, docMissingRevs);

            if (!this.cancel) {
                this.targetDb.bulkSerializedDocs(serialisedMissingRevs);
                changesProcessed += docMissingRevs.size();
            }
        }

        if (!this.cancel) {
            this.putCheckpoint(String.valueOf(changes.getLastSequence()));
        }

        return changesProcessed;
    }

    private List<String> missingRevisionsToJsonDocs(
            Map<String, DocumentRevisionTree> allTrees,
            Map<String, Set<String>> revisions) {
        List<String> docs = new ArrayList<String>();
        for(Map.Entry<String, Set<String>> e : revisions.entrySet()) {
            String docId = e.getKey();
            Set<String> missingRevisions = e.getValue();
            DocumentRevisionTree tree = allTrees.get(docId);
            for(String rev : missingRevisions) {
                long sequence = tree.lookup(docId, rev).getSequence();
                List<DocumentRevision> path = tree.getPathForNode(sequence);
                // NB this will probably look like AttachmentHistoryHelper.....() later
                DocumentRevision dr = path.get(0);
                List<? extends Attachment> atts = this.sourceDb.getDbCore().attachmentsForRevision(dr);
                // need to graft on _attachments somehow
                // need to stub out attachments for in between versions
                docs.add(RevisionHistoryHelper.revisionHistoryToJson(path, atts));
            }
        }
        return docs;
    }

    Map<String, Set<String>> openRevisions(Map<String, DocumentRevisionTree> trees) {
        Map<String, Set<String>> allOpenRevisions = new HashMap<String, Set<String>>();
        for(Map.Entry<String, DocumentRevisionTree> e : trees.entrySet()) {
            allOpenRevisions.put(e.getKey(), e.getValue().leafRevisionIds());
        }
        return allOpenRevisions;
    }

    private long getLastCheckpointSequence() {
        String lastSequence =  targetDb.getCheckpoint(this.sourceDb.getIdentifier());
        // As we are pretty sure the checkpoint is a number
        return Strings.isNullOrEmpty(lastSequence) ? 0 : Long.valueOf(lastSequence);
    }

    private void putCheckpoint(String checkpoint) {
        targetDb.putCheckpoint(sourceDb.getIdentifier(), checkpoint);
    }
    
    @Override
    public EventBus getEventBus() {
        return eventBus;
    }

}
