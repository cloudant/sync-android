package com.cloudant.sync.replication;

import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.net.wifi.WifiManager;
import android.os.Binder;
import android.os.Bundle;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.IBinder;
import android.os.Looper;
import android.os.Message;
import android.support.v4.content.WakefulBroadcastReceiver;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * This abstract class forms the basis for creating replication policies on Android.
 * The replications run in a {@link Service} so that they can properly manage the
 * lifecycle and handle being killed or restarted by the operating system
 */
public abstract class ReplicationService extends Service
        implements ReplicationPolicyManager.ReplicationsCompletedListener {

    public static final String EXTRA_INTENT = "intent";
    public static final String EXTRA_COMMAND = "command";
    public static final int COMMAND_NONE = -1;
    public static final int COMMAND_START_REPLICATION = 0;
    public static final int COMMAND_STOP_REPLICATION = 1;

    private Handler mServiceHandler;
    private ReplicationPolicyManager mReplicationPolicyManager;

    /**
     * Stores the set of {@link ReplicationCompleteListener}s listening for replication complete
     * events. Note that all modifications or iterations over mListeners should be protected by
     * synchronization on the mListeners object.
     */
    private final Set<ReplicationCompleteListener> mListeners = new HashSet<ReplicationCompleteListener>();

    // It's safest to assume we could be transferring a large amount of data in a
    // replication, so we want a high performance WiFi connection even though it
    // requires more power.
    private final WifiManager.WifiLock mWifiLock =
            ((WifiManager) getSystemService(Context.WIFI_SERVICE)).
                    createWifiLock(WifiManager.WIFI_MODE_FULL_HIGH_PERF, "ReplicationService");

    private OperationStartedListener mOperationStartedListener;

    private final IBinder mBinder = new LocalBinder();

    interface OperationStartedListener {
        /** Callback to indicate that an operation has started. */
        void operationStarted(int operationId);
    }

    public interface ReplicationCompleteListener {
        /**
         * Callback to indicate that all replications specified by {@link #getReplicators(Context)}
         * are complete.
         */
        void allReplicationsComplete();

        /** Callback to indicate that the individual replication with the given {@code id} is
         * complete.
         * @param id the {@code id} number associated wit the replication that has completed. See
         *           {@link ReplicatorBuilder#withId(int)}.
         */
        void replicationComplete(int id);

        /** Callback to indicate that the individual replication with the given {@code id} has
         * errored.
         * @param id the {@code id} number associated wit the replication that has completed. See
         *           {@link ReplicatorBuilder#withId(int)}.
         */
        void replicationErrored(int id);
    }

    /**
     * A simple {@link com.cloudant.sync.replication.ReplicationService.ReplicationCompleteListener}
     * to save clients having to override every method if they are only interested in a subset of
     * the events.
     */
    public static class SimpleReplicationCompleteListener implements ReplicationCompleteListener {
        @Override
        public void allReplicationsComplete() {
        }

        @Override
        public void replicationComplete(int id) {
        }

        @Override
        public void replicationErrored(int id) {
        }
    }

    // A binder to allow components running in the same process to bind to this Service.
    public class LocalBinder extends Binder {
        public ReplicationService getService() {
            return ReplicationService.this;
        }
    }

    // Handler that receives messages from the thread.
    protected class ServiceHandler extends Handler {
        public ServiceHandler(Looper looper) {
            super(looper);
        }

        @Override
        public void handleMessage(Message msg) {
            try {
                // Process the commands passed in msg.arg2.
                switch (msg.arg2) {
                    case COMMAND_START_REPLICATION:
                        startReplications();
                        break;
                    case COMMAND_STOP_REPLICATION:
                        stopReplications();
                        break;
                }
            } finally {
                // Get the Intent used to start the service and release the WakeLock if there is one.
                // Calling completeWakefulIntent is safe even if there is no wakelock held.
                Intent intent = msg.getData().getParcelable(EXTRA_INTENT);
                WakefulBroadcastReceiver.completeWakefulIntent(intent);

                notifyOperationStarted(msg.arg2);
            }
        }
    }

    protected Handler getHandler(Looper looper) {
        return new ServiceHandler(looper);
    }

    @Override
    public void onCreate() {
        super.onCreate();

        // Create a background priority thread to so we don't block the process's main thread.
        HandlerThread thread = new HandlerThread("ServiceStartArguments",
            android.os.Process.THREAD_PRIORITY_BACKGROUND);
        thread.start();

        // Get the HandlerThread's Looper and use it for our Handler.
        Looper serviceLooper = thread.getLooper();
        mServiceHandler = getHandler(serviceLooper);

        Replicator[] replicators = getReplicators(getApplicationContext());

        if (replicators != null && replicators.length > 0) {
            if (mReplicationPolicyManager == null) {
                mReplicationPolicyManager = new ReplicationPolicyManager();
            }
            mReplicationPolicyManager.addReplicators(replicators);
            mReplicationPolicyManager.setReplicationsCompletedListener(this);
        } else {
            throw new RuntimeException(
                    "No replications setup. Please return Replicators from getReplicators()");
        }
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        // Extract the command from the given Intent and pass it to our handler to process the
        // command on a separate thread.
        if (intent != null && intent.hasExtra(EXTRA_COMMAND)) {
            Message msg = mServiceHandler.obtainMessage();
            msg.arg1 = startId;
            msg.arg2 = intent.getIntExtra(EXTRA_COMMAND, COMMAND_STOP_REPLICATION);
            Bundle bundle = new Bundle();
            bundle.putParcelable(EXTRA_INTENT, intent);
            msg.setData(bundle);
            mServiceHandler.sendMessage(msg);
        }

        return START_REDELIVER_INTENT;
    }

    @Override
    public IBinder onBind(Intent intent) {
        return mBinder;
    }

    @Override
    public boolean onUnbind(Intent intent) {
        // Ensure onRebind is called when new clients bind to the service.
        return true;
    }

    @Override
    public void onRebind(Intent intent) {
        super.onRebind(intent);
    }

    /**
     * Set the {@link ReplicationPolicyManager} to be used by this ReplicationService.
     * @param replicationPolicyManager
     */
    public void setReplicationPolicyManager(ReplicationPolicyManager replicationPolicyManager) {
        mReplicationPolicyManager = replicationPolicyManager;
    }

    /**
     * Start the set of replications specified in the set of {@link Replicator}s returned by
     * {@link #getReplicators(Context)}.
     */
    protected void startReplications() {
        if (mReplicationPolicyManager != null) {
            // Make sure we've got a WiFi lock so that the wifi isn't switched off while we're
            // trying to replicate.
            synchronized (mWifiLock) {
                if (!mWifiLock.isHeld()) {
                    mWifiLock.acquire();
                }
            }
            mReplicationPolicyManager.startReplications();
        }
    }

    private void releaseWifiLockIfHeld() {
        synchronized (mWifiLock) {
            if (mWifiLock.isHeld()) {
                mWifiLock.release();
            }
        }
    }

    /**
     * Stop replications currently in progress and terminate this Service.
     */
    protected void stopReplications() {
        if (mReplicationPolicyManager != null) {
            mReplicationPolicyManager.stopReplications();
            releaseWifiLockIfHeld();
        }
        stopSelf();
    }

    @Override
    public void allReplicationsCompleted() {
        synchronized (mListeners) {
            for (ReplicationCompleteListener listener : mListeners) {
                listener.allReplicationsComplete();
            }
        }
        releaseWifiLockIfHeld();
        stopSelf();
    }

    @Override
    public void replicationCompleted(int id) {
        synchronized (mListeners) {
            for (ReplicationCompleteListener listener : mListeners) {
                listener.replicationComplete(id);
            }
        }
    }

    @Override
    public void replicationErrored(int id) {
        synchronized (mListeners) {
            for (ReplicationCompleteListener listener : mListeners) {
                listener.replicationErrored(id);
            }
        }
    }

    /**
     * Add a listener to the set of {@link ReplicationCompleteListener}s that are notified when
     * replications complete.
     * @param listener The listener to add.
     */
    public void addListener(ReplicationCompleteListener listener) {
        synchronized (mListeners) {
            mListeners.add(listener);
        }
    }

    /**
     * Remove a listener from the set of {@link ReplicationCompleteListener}s that are notified when
     * replications complete.
     * @param listener The listener to remove.
     */
    public void removeListener(ReplicationCompleteListener listener) {
        synchronized (mListeners) {
            mListeners.remove(listener);
        }
    }

    /**
     * Set a listener to be notified when an operation has started.
     * @param listener The listener to add
     */
    public void setOperationStartedListener(OperationStartedListener listener) {
        mOperationStartedListener = listener;
    }

    /**
     * Invoke the callback used to notify listeners that the operation has started.
     */
    void notifyOperationStarted(int operationId) {
        if (mOperationStartedListener != null) {
            mOperationStartedListener.operationStarted(operationId);
        }
    }

    /**
     * @param context A valid application context.
     * @return An array of {@link Replicator} instances containing the replications you wish
     * to run.
     */
    protected abstract Replicator[] getReplicators(Context context);
}

