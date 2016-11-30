/*
 * Copyright © 2016 Cloudant, Inc. All rights reserved.
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

package com.cloudant.todo;

import android.content.Context;
import android.os.Handler;
import android.os.Looper;
import android.util.Log;

import com.cloudant.sync.documentstore.ConflictException;
import com.cloudant.sync.documentstore.Database;
import com.cloudant.sync.documentstore.DocumentBodyFactory;
import com.cloudant.sync.documentstore.DocumentException;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.documentstore.DocumentStore;
import com.cloudant.sync.documentstore.DocumentStoreNotOpenedException;
import com.cloudant.sync.event.Subscribe;
import com.cloudant.sync.event.notifications.ReplicationCompleted;
import com.cloudant.sync.event.notifications.ReplicationErrored;
import com.cloudant.sync.replication.Replicator;
import com.cloudant.sync.replication.ReplicatorBuilder;
import com.cloudant.todo.ui.activities.ReplicationSettingsActivity;
import com.cloudant.todo.ui.activities.TodoActivity;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;


/**
 * <p>Handles dealing with the datastore and replication.</p>
 */
public class TasksModel {

    private static final String LOG_TAG = "TasksModel";

    private static final String DATASTORE_MANGER_DIR = "data";
    private static final String TASKS_DATASTORE_NAME = "tasks";
    private final Handler mHandler;
    private Database mDatabase;
    private Replicator mPushReplicator;
    private Replicator mPullReplicator;
    private TodoActivity mListener;

    public TasksModel(Context context) {

        // Set up our tasks datastore within its own folder in the applications
        // data directory.
        File path = context.getDir(
            DATASTORE_MANGER_DIR,
            Context.MODE_PRIVATE
        );
        try {
            this.mDatabase = DocumentStore.getInstance(new File(path, TASKS_DATASTORE_NAME))
                .database;
        } catch (DocumentStoreNotOpenedException dsnoe) {
            Log.e(LOG_TAG, "Unable to open Datastore", dsnoe);
        }

        Log.d(LOG_TAG, "Set up database at " + path.getAbsolutePath());

        // Set up the replicator objects from the app's settings.
        try {
            this.reloadReplicationSettings(context);
        } catch (URISyntaxException e) {
            Log.e(LOG_TAG, "Unable to construct remote URI from configuration", e);
        }

        // Allow us to switch code called by the ReplicationListener into
        // the main thread so the UI can update safely.
        this.mHandler = new Handler(Looper.getMainLooper());

        Log.d(LOG_TAG, "TasksModel set up " + path.getAbsolutePath());
    }

    //
    // GETTERS AND SETTERS
    //

    /**
     * Sets the listener for replication callbacks as a weak reference.
     *
     * @param listener {@link TodoActivity} to receive callbacks.
     */
    public void setReplicationListener(TodoActivity listener) {
        this.mListener = listener;
    }

    //
    // DOCUMENT CRUD
    //

    /**
     * Creates a task, assigning an ID.
     *
     * @param task task to create
     * @return new revision of the document
     */
    public Task createDocument(Task task) {
        DocumentRevision rev = new DocumentRevision();
        rev.setBody(DocumentBodyFactory.create(task.asMap()));
        try {
            DocumentRevision created = this.mDatabase.createDocumentFromRevision(rev);
            return Task.fromRevision(created);
        } catch (DocumentException de) {
            return null;
        }
    }

    /**
     * Updates a Task document within the datastore.
     *
     * @param task task to update
     * @return the updated revision of the Task
     * @throws ConflictException if the task passed in has a rev which doesn't
     *                           match the current rev in the datastore.
     */
    public Task updateDocument(Task task) throws ConflictException {
        DocumentRevision rev = task.getDocumentRevision();
        rev.setBody(DocumentBodyFactory.create(task.asMap()));
        try {
            DocumentRevision updated = this.mDatabase.updateDocumentFromRevision(rev);
            return Task.fromRevision(updated);
        } catch (DocumentException de) {
            return null;
        }
    }

    /**
     * Deletes a Task document within the datastore.
     *
     * @param task task to delete
     * @throws ConflictException if the task passed in has a rev which doesn't
     *                           match the current rev in the datastore.
     */
    public void deleteDocument(Task task) throws ConflictException {
        this.mDatabase.deleteDocumentFromRevision(task.getDocumentRevision());
    }

    /**
     * <p>Returns all {@code Task} documents in the datastore.</p>
     */
    public List<Task> allTasks() {
        int nDocs = this.mDatabase.getDocumentCount();
        List<DocumentRevision> all = this.mDatabase.getAllDocuments(0, nDocs, true);
        List<Task> tasks = new ArrayList<>();

        // Filter all documents down to those of type Task.
        for (DocumentRevision rev : all) {
            Task t = Task.fromRevision(rev);
            if (t != null) {
                tasks.add(t);
            }
        }

        return tasks;
    }

    //
    // MANAGE REPLICATIONS
    //

    /**
     * <p>Stops running replications.</p>
     * <p>
     * <p>The stop() methods stops the replications asynchronously, see the
     * replicator docs for more information.</p>
     */
    public void stopAllReplications() {
        if (this.mPullReplicator != null) {
            this.mPullReplicator.stop();
        }
        if (this.mPushReplicator != null) {
            this.mPushReplicator.stop();
        }
    }

    /**
     * <p>Starts the configured push replication.</p>
     */
    public void startPushReplication() {
        if (this.mPushReplicator != null) {
            this.mPushReplicator.start();
        } else {
            throw new RuntimeException("Push replication not set up correctly");
        }
    }

    /**
     * <p>Starts the configured pull replication.</p>
     */
    public void startPullReplication() {
        if (this.mPullReplicator != null) {
            this.mPullReplicator.start();
        } else {
            throw new RuntimeException("Push replication not set up correctly");
        }
    }

    /**
     * <p>Stops running replications and reloads the replication settings from
     * the app's preferences.</p>
     */
    public void reloadReplicationSettings(Context context)
        throws URISyntaxException {

        // Stop running replications before reloading the replication
        // settings.
        // The stop() method instructs the replicator to stop ongoing
        // processes, and to stop making changes to the datastore. Therefore,
        // we don't clear the listeners because their complete() methods
        // still need to be called once the replications have stopped
        // for the UI to be updated correctly with any changes made before
        // the replication was stopped.
        this.stopAllReplications();

        // Set up the new replicator objects
        URI uri = ReplicationSettingsActivity.constructServerURI(context);

        mPullReplicator = ReplicatorBuilder.pull().to(mDatabase).from(uri).build();
        mPushReplicator = ReplicatorBuilder.push().from(mDatabase).to(uri).build();

        mPushReplicator.getEventBus().register(this);
        mPullReplicator.getEventBus().register(this);

        Log.d(LOG_TAG, "Set up replicators for URI:" + uri.toString());
    }

    //
    // REPLICATIONLISTENER IMPLEMENTATION
    //

    /**
     * Calls the TodoActivity's replicationComplete method on the main thread,
     * as the complete() callback will probably come from a replicator worker
     * thread.
     */
    @Subscribe
    public void complete(ReplicationCompleted rc) {
        mHandler.post(new Runnable() {
            @Override
            public void run() {
                if (mListener != null) {
                    mListener.replicationComplete();
                }
            }
        });
    }

    /**
     * Calls the TodoActivity's replicationComplete method on the main thread,
     * as the error() callback will probably come from a replicator worker
     * thread.
     */
    @Subscribe
    public void error(ReplicationErrored re) {
        Log.e(LOG_TAG, "Replication error:", re.errorInfo.getCause());
        mHandler.post(new Runnable() {
            @Override
            public void run() {
                if (mListener != null) {
                    mListener.replicationError();
                }
            }
        });
    }
}
