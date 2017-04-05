package com.cloudant.sync.replication;

import android.app.job.JobParameters;
import android.app.job.JobService;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;

public abstract class ReplicationJobService<T extends PeriodicReplicationService> extends
    JobService {

    Class<T> clazz;

    // Add a handler to allow us to post UI updates on the main thread.
    private final Handler mHandler = new Handler(Looper.getMainLooper());

    // Reference to our service.
    private ReplicationService mReplicationService;

    // Flag indicating whether the Activity is currently bound to the Service.
    private boolean mIsBound;

    private JobParameters mJobParameters;

    private ServiceConnection mConnection;

    public ReplicationJobService(Class<T> clazz) {
        this.clazz = clazz;
        mConnection = new ServiceConnection() {
            @Override
            public void onServiceConnected(ComponentName name, IBinder service) {
                mReplicationService = ((ReplicationService.LocalBinder) service).getService();
                mReplicationService.addListener(new PolicyReplicationsCompletedListener.SimpleListener() {
                    @Override
                    public void allReplicationsCompleted() {
                        Log.d(ReplicationService.TAGJS, ReplicationJobService.this.getClass()
                            .getSimpleName() + ": calling " +
                            "jobFinished() on the JobScheduler");
                        mReplicationService.removeListener(this);
                        jobFinished(mJobParameters, false);
                    }
                });
                unbindService(mConnection);
            }

            @Override
            public void onServiceDisconnected(ComponentName name) {
                mReplicationService = null;
            }
        };
    }

    @Override
    public boolean onStartJob(JobParameters jobParameters) {
        mJobParameters = jobParameters;
        Log.d(ReplicationService.TAGJS, getClass().getSimpleName() + ": in onStartJob() for the " +
            "JobScheduler");

        Intent serviceIntent = new Intent(getApplicationContext(), clazz);
        serviceIntent.putExtra(PeriodicReplicationService.EXTRA_COMMAND,
            PeriodicReplicationService.COMMAND_START_REPLICATION);
        startService(serviceIntent);
        bindService(new Intent(this, clazz), mConnection, Context.BIND_AUTO_CREATE);

        // Work is being done on a separate thread.
        return true;
    }

    @Override
    public boolean onStopJob(JobParameters jobParameters) {
        Log.d(ReplicationService.TAGJS, getClass().getSimpleName() + ": in onStopJob() for the " +
            "JobScheduler");
        Intent serviceIntent = new Intent(getApplicationContext(), getClass());
        serviceIntent.putExtra(PeriodicReplicationService.EXTRA_COMMAND,
            PeriodicReplicationService.COMMAND_STOP_REPLICATION);
        startService(serviceIntent);

        // We want the job rescheduled next time the conditions for execution are met.
        return true;
    }
}
