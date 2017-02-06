/*
 * Copyright © 2016 IBM Corp. All rights reserved.
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This abstract class forms the basis for creating replication policies on Android.
 * The replications run in a {@link Service} so that they can properly manage the
 * lifecycle and handle being killed or restarted by the operating system
 */
public abstract class ReplicationService extends Service
        implements PolicyReplicationsCompletedListener {

    public static final String EXTRA_INTENT = "intent";

    /**
     * The key used to identify the command being sent to the service.
     */
    public static final String EXTRA_COMMAND = "command";

    public static final int COMMAND_NONE = -1;

    /**
     * To start replications, this value should be passed to this service in an Intent using an
     * Extra with the {@link #EXTRA_COMMAND} key.
     *
     * @see
     * <a href="http://github.com/cloudant/sync-android/blob/master/doc/replication-policies.md#controlling-the-replication-service">
     * Replication Policy User Guide</a> for more details.
     */
    public static final int COMMAND_START_REPLICATION = 0;

    /**
     * To stop replications, this value should be passed to this service in an Intent using an
     * Extra with the {@link #EXTRA_COMMAND} key.
     *
     * @see
     * <a href="http://github.com/cloudant/sync-android/blob/master/doc/replication-policies.md#controlling-the-replication-service">
     * Replication Policy User Guide</a> for more details.
     */
    public static final int COMMAND_STOP_REPLICATION = 1;

    private static final int INTERNALLY_RESERVED_COMMAND_MAX_USED = 5;
    private static final int INTERNALLY_RESERVED_COMMAND_MAX = 99;

    private Handler mServiceHandler;
    private ReplicationPolicyManager mReplicationPolicyManager;
    private boolean mReplicatorsInitialised;
    private List<Message> mCommandQueue = new ArrayList<Message>();

    /**
     * Stores the set of {@link PolicyReplicationsCompletedListener}s
     * listening for replication complete
     * events. Note that all modifications or iterations over mListeners should be protected by
     * synchronization on the mListeners object.
     */
    private final Set<PolicyReplicationsCompletedListener> mListeners = new
            HashSet<PolicyReplicationsCompletedListener>();

    // It's safest to assume we could be transferring a large amount of data in a
    // replication, so we want a high performance WiFi connection even though it
    // requires more power.
    private WifiManager.WifiLock mWifiLock;

    private OperationStartedListener mOperationStartedListener;

    private final IBinder mBinder = new LocalBinder();

    interface OperationStartedListener {
        /**
         * Callback to indicate that an operation has started.
         */
        void operationStarted(int operationId);
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
                if (msg.arg2 > INTERNALLY_RESERVED_COMMAND_MAX_USED && msg.arg2 <=
                        INTERNALLY_RESERVED_COMMAND_MAX) {
                    throw new RuntimeException("ReplicationService received an EXTRA_COMMAND " +
                            "using an id in the range reserved for internal use. Custom commands " +
                            "must use an id above " + INTERNALLY_RESERVED_COMMAND_MAX);
                }
                // Process the commands passed in msg.arg2.
                switch (msg.arg2) {
                    case COMMAND_START_REPLICATION:
                        startReplications();
                        break;
                    case COMMAND_STOP_REPLICATION:
                        stopReplications();
                        break;
                    default:
                        // Do nothing
                        break;
                }
            } finally {
                // Get the Intent used to start the service and release the WakeLock if there is
                // one.
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

        WifiManager wifiManager = (WifiManager) getSystemService(Context.WIFI_SERVICE);
        if (wifiManager != null) {
            mWifiLock = wifiManager.createWifiLock(WifiManager.WIFI_MODE_FULL_HIGH_PERF,
                    "ReplicationService");
        }

        // Create a background priority thread to so we don't block the process's main thread.
        HandlerThread thread = new HandlerThread("ServiceStartArguments",
                android.os.Process.THREAD_PRIORITY_BACKGROUND);
        thread.start();

        // Get the HandlerThread's Looper and use it for our Handler.
        Looper serviceLooper = thread.getLooper();
        mServiceHandler = getHandler(serviceLooper);
    }

    /**
     * Set the {@link Replicator} objects configured to perform the required replications.
     * The {@code ReplicationService} will not begin replications until this method has
     * been called. This operation should only be called once.
     *
     * @param replicators An array of the configured {@code Replicator} objects.
     * @throws IllegalArgumentException if {@code replicators} is null or empty.
     * @throws IllegalStateException    if called again after a previous call to this method
     *                                  with a valid array of {@code Replicator} objects.
     */
    public void setReplicators(Replicator[] replicators) {
        if (mReplicatorsInitialised) {
            throw new IllegalStateException("Replicators already set");
        }
        if (replicators != null && replicators.length > 0) {
            if (mReplicationPolicyManager == null) {
                mReplicationPolicyManager = new ReplicationPolicyManager();
            }
            mReplicationPolicyManager.addReplicators(replicators);
            mReplicationPolicyManager.setReplicationsCompletedListener(this);
            synchronized (mCommandQueue) {
                mReplicatorsInitialised = true;
                for (Message msg : mCommandQueue) {
                    mServiceHandler.sendMessage(msg);
                }
                mCommandQueue.clear();
            }
        } else {
            throw new IllegalArgumentException(
                    "No replications setup. Please pass Replicators to setReplicators" +
                            "(Replicator[])");
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
            synchronized (mCommandQueue) {
                if (mReplicatorsInitialised) {
                    mServiceHandler.sendMessage(msg);
                } else {
                    // Add the message to the command queue if it's different to the one at the
                    // tail of the queue. These messages will then be
                    // processed once setReplicators(Replicator[]) has been called.
                    if (mCommandQueue.size() == 0 ||
                            mCommandQueue.get(mCommandQueue.size() - 1).arg2 != msg.arg2) {
                        mCommandQueue.add(msg);
                    }
                }
            }
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
     *
     * @param replicationPolicyManager the {@link ReplicationPolicyManager}
     */
    public void setReplicationPolicyManager(ReplicationPolicyManager replicationPolicyManager) {
        mReplicationPolicyManager = replicationPolicyManager;
    }

    /**
     * Start the set of replications specified in the set of {@link Replicator}s passed to
     * {@link #setReplicators(Replicator[])}.
     */
    protected void startReplications() {
        if (mReplicationPolicyManager != null) {
            // Make sure we've got a WiFi lock so that the wifi isn't switched off while we're
            // trying to replicate.
            if (mWifiLock != null) {
                synchronized (mWifiLock) {
                    if (!mWifiLock.isHeld()) {
                        mWifiLock.acquire();
                    }
                }
            }
            mReplicationPolicyManager.startReplications();
        }
    }

    private void releaseWifiLockIfHeld() {
        if (mWifiLock != null) {
            synchronized (mWifiLock) {
                if (mWifiLock.isHeld()) {
                    mWifiLock.release();
                }
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
            for (PolicyReplicationsCompletedListener listener : mListeners) {
                listener.allReplicationsCompleted();
            }
        }
        releaseWifiLockIfHeld();
        stopSelf();
    }

    @Override
    public void replicationCompleted(int id) {
        synchronized (mListeners) {
            for (PolicyReplicationsCompletedListener listener : mListeners) {
                listener.replicationCompleted(id);
            }
        }
    }

    @Override
    public void replicationErrored(int id) {
        synchronized (mListeners) {
            for (PolicyReplicationsCompletedListener listener : mListeners) {
                listener.replicationErrored(id);
            }
        }
    }

    /**
     * Add a listener to the set of
     * {@link PolicyReplicationsCompletedListener}s that are notified when
     * replications complete.
     *
     * @param listener The listener to add.
     */
    public void addListener(PolicyReplicationsCompletedListener listener) {
        synchronized (mListeners) {
            mListeners.add(listener);
        }
    }

    /**
     * Remove a listener from the set of
     * {@link PolicyReplicationsCompletedListener}s that are notified when
     * replications complete.
     *
     * @param listener The listener to remove.
     */
    public void removeListener(PolicyReplicationsCompletedListener listener) {
        synchronized (mListeners) {
            mListeners.remove(listener);
        }
    }

    /**
     * Set a listener to be notified when an operation has started.
     *
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
}

