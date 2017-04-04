package com.cloudant.todo.replicationpolicy;

import android.annotation.TargetApi;
import android.app.job.JobParameters;
import android.app.job.JobService;
import android.content.Context;
import android.os.AsyncTask;
import android.util.Log;

import com.cloudant.sync.documentstore.DocumentStore;
import com.cloudant.sync.documentstore.DocumentStoreNotOpenedException;
import com.cloudant.sync.event.EventBus;
import com.cloudant.sync.event.Subscribe;
import com.cloudant.sync.event.notifications.ReplicationCompleted;
import com.cloudant.sync.event.notifications.ReplicationErrored;
import com.cloudant.sync.replication.Replicator;
import com.cloudant.sync.replication.ReplicatorBuilder;
import com.cloudant.todo.ui.activities.SettingsActivity;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;

@TargetApi(21)
public class TodoJobService extends JobService {

    public static final String TAG = "BMHTodoJS";
    private static final String DOCUMENT_STORE_DIR = "data";
    private static final String DOCUMENT_STORE_NAME = "tasks";
    public static int PUSH_REPLICATION_ID = 0;
    public static int PULL_REPLICATION_ID = 1;

    private ReplicationTask mReplicationTask = new ReplicationTask();

    private JobParameters mJobParameters;

    private static EventBus sEventBus = new EventBus();

    public class Listener {

        private final CountDownLatch latch;

        Listener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Subscribe
        public void complete(ReplicationCompleted event) {
            latch.countDown();
            Log.d(TAG, "Posting the event onto the static event bus ID=" + event.replicator.getId());
            sEventBus.post(event);
        }

        @Subscribe
        public void error(ReplicationErrored event) {
            latch.countDown();
        }
    }

    class ReplicationTask extends AsyncTask<Void, Void, Void> {
        @Override
        protected Void doInBackground(Void... params) {
            try {
                URI uri = SettingsActivity.constructTodoURI(getApplicationContext());

                File path = getApplicationContext().getDir(
                    DOCUMENT_STORE_DIR,
                    Context.MODE_PRIVATE
                );

                DocumentStore documentStore = null;
                try {
                    documentStore = DocumentStore.getInstance(new File(path, DOCUMENT_STORE_NAME));
                } catch (DocumentStoreNotOpenedException dsnoe) {
                    Log.e(TAG, "Unable to open DocumentStore", dsnoe);
                }

                Replicator pullReplicator = ReplicatorBuilder.pull().from(uri).to(documentStore).withId
                    (PULL_REPLICATION_ID).build();
                Replicator pushReplicator = ReplicatorBuilder.push().to(uri).from(documentStore).withId
                    (PUSH_REPLICATION_ID).build();

                CountDownLatch latch = new CountDownLatch(2);
                Listener listener = new Listener(latch);
                pullReplicator.getEventBus().register(listener);
                pullReplicator.start();
                pushReplicator.getEventBus().register(listener);
                pushReplicator.start();
                latch.await();
                pullReplicator.getEventBus().unregister(listener);
                pushReplicator.getEventBus().unregister(listener);
            } catch (URISyntaxException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                Log.d(TAG, "Replication AsyncTask is cancelled");
                e.printStackTrace();
            }
            return null;
        }

        @Override
        protected void onPostExecute(Void aVoid) {
            Log.d(TAG, "Replications have finished, calling jobFinished()");
            jobFinished(mJobParameters, false);
        }

        @Override
        protected void onCancelled(Void aVoid) {
            Log.d(TAG, "Replications were cancelled, calling jobFinished()");
            jobFinished(mJobParameters, true);
        }
    }

    @Override
    public boolean onStartJob(JobParameters jobParameters) {
        Log.d(TAG, "jobParameters.isOverrideDeadlineExpired()" + jobParameters
            .isOverrideDeadlineExpired());
        mJobParameters = jobParameters;
        Log.d(TAG, getClass().getSimpleName() + ": in onStartJob() for the " +
            "JobScheduler");

        // If we have a periodic job we need to check this due to a bug in the JobScheduler.
        if (!jobParameters.isOverrideDeadlineExpired()) {
            mReplicationTask.execute();
        } else {
            Log.e(TAG, "Android JobScheduler bug occurred. Not starting job");
            // Workaround bug in JobScheduler.
            // https://code.google.com/p/android/issues/detail?id=81265
            jobFinished(mJobParameters, true);
        }

        // Work is being done on a separate thread.
        return true;
    }

    @Override
    public boolean onStopJob(JobParameters jobParameters) {
        Log.d(TAG, getClass().getSimpleName() + ": in onStopJob() for the " +
            "JobScheduler");

        mReplicationTask.cancel(true);

        // We want the job rescheduled next time the conditions for execution are met.
        return true;
    }

    public static EventBus getEventBus() {
        return sEventBus;
    }
}
