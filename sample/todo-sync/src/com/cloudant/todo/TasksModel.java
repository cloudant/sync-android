package com.cloudant.todo;

import com.cloudant.sync.datastore.ConflictException;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.replication.ErrorInfo;
import com.cloudant.sync.replication.ReplicationListener;
import com.cloudant.sync.replication.Replicator;
import com.cloudant.sync.replication.ReplicatorFactory;
import com.cloudant.sync.util.TypedDatastore;

import android.content.Context;
import android.content.SharedPreferences;
import android.os.Handler;
import android.os.Looper;
import android.preference.PreferenceManager;
import android.util.Log;

import java.lang.RuntimeException;
import java.util.ArrayList;
import java.io.File;
import java.util.Map;
import java.util.List;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * <p>Handles dealing with the datastore and replication.</p>
 */
class TasksModel implements ReplicationListener {

    private static final String LOG_TAG = "TasksModel";

    private static final String DATASTORE_MANGER_DIR = "data";
    private static final String TASKS_DATASTORE_NAME = "tasks";

    private final Datastore mDatastore;
    private final TypedDatastore<Task> mTasksDatastore;

    private Replicator mPushReplicator;
    private Replicator mPullReplicator;

    private final Context mContext;
    private final Handler mHandler;
    private TodoActivity mListener;

    public TasksModel(Context context) {

        this.mContext = context;

        // Set up our tasks datastore within its own folder in the applications
        // data directory.
        File path = this.mContext.getApplicationContext().getDir(
                DATASTORE_MANGER_DIR,
                Context.MODE_PRIVATE
        );
        DatastoreManager manager = new DatastoreManager(path.getAbsolutePath());
        this.mDatastore = manager.openDatastore(TASKS_DATASTORE_NAME);

        Log.d(LOG_TAG, "Set up database at " + path.getAbsolutePath());

        // A TypedDatastore handles object mapping from DBObject to another
        // class, saving us some code in this class for CRUD methods.
        this.mTasksDatastore = new TypedDatastore<Task>(Task.class, this.mDatastore);

        // Set up the replicator objects from the app's settings.
        try {
            this.reloadReplicationSettings();
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
     * @param task task to create
     * @return new revision of the document
     */
    public Task createDocument(Task task) {
        return this.mTasksDatastore.createDocument(task);
    }

    /**
     * Updates a Task document within the datastore.
     * @param task task to update
     * @return the updated revision of the Task
     * @throws ConflictException if the task passed in has a rev which doesn't
     *      match the current rev in the datastore.
     */
    public Task updateDocument(Task task)
            throws ConflictException {
        return this.mTasksDatastore.updateDocument(task);
    }

    /**
     * Deletes a Task document within the datastore.
     * @param task task to delete
     * @throws ConflictException if the task passed in has a rev which doesn't
     *      match the current rev in the datastore.
     */
    public void deleteDocument(Task task)
            throws ConflictException {
        this.mTasksDatastore.deleteDocument(task);
    }

    /**
     * <p>Returns all {@code Task} documents in the datastore.</p>
     */
    public List<Task> allTasks() {
        int nDocs = this.mDatastore.getDocumentCount();
        List<DocumentRevision> all = this.mDatastore.getAllDocuments(0, nDocs, true);
        List<Task> tasks = new ArrayList<Task>();

        // Filter all documents down to those of type Task.
        for(DocumentRevision obj : all) {
            Map<String, Object> map = obj.asMap();
            if(map.containsKey("type") && map.get("type").equals(Task.DOC_TYPE)) {
                Task t = this.mTasksDatastore.deserializeDocumentRevision(obj);
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
     *
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
    public void reloadReplicationSettings()
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
        URI uri = this.createServerURI();

        mPushReplicator = ReplicatorFactory.oneway(mDatastore, uri);
        mPushReplicator.setListener(this);

        mPullReplicator = ReplicatorFactory.oneway(uri, mDatastore);
        mPullReplicator.setListener(this);

        Log.d(LOG_TAG, "Set up replicators for URI:" + uri.toString());
    }

    /**
     * <p>Returns the URI for the remote database, based on the app's
     * configuration.</p>
     * @return the remote database's URI
     * @throws URISyntaxException if the settings give an invalid URI
     */
    private URI createServerURI()
            throws URISyntaxException {
        // We store this in plain text for the purposes of simple demonstration,
        // you might want to use something more secure.
        SharedPreferences sharedPref = PreferenceManager.getDefaultSharedPreferences(this.mContext);
        String username = sharedPref.getString(TodoActivity.SETTINGS_CLOUDANT_USER, "");
        String dbName = sharedPref.getString(TodoActivity.SETTINGS_CLOUDANT_DB, "");
        String apiKey = sharedPref.getString(TodoActivity.SETTINGS_CLOUDANT_API_KEY, "");
        String apiSecret = sharedPref.getString(TodoActivity.SETTINGS_CLOUDANT_API_SECRET, "");
        String host = username + ".cloudant.com";

        // We recommend always using HTTPS to talk to Cloudant.
        return new URI("https", apiKey + ":" + apiSecret, host, 443, "/" + dbName, null, null);
    }

    //
    // REPLICATIONLISTENER IMPLEMENTATION
    //

    /**
     * Calls the TodoActivity's replicationComplete method on the main thread,
     * as the complete() callback will probably come from a replicator worker
     * thread.
     */
    @Override
    public void complete(Replicator replicator) {
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
    @Override
    public void error(Replicator replicator, final ErrorInfo error) {
        Log.e(LOG_TAG, "Replication error:", error.getException());
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
