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

import android.app.AlarmManager;
import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;
import android.os.Message;
import android.os.SystemClock;
import android.util.Log;

/**
 * This {@link android.app.Service} is an abstract class that is the basis for creating a service
 * that performs periodic replications (i.e. replications that occur at regular intervals). The
 * period between replications may be varied depending on whether other application components
 * are bound to the service or not, so as to allow for more frequent replications when an app
 * is in active use and less frequent replications the rest of the time.
 *
 * @param <T> The {@link PeriodicReplicationReceiver} associated with this Service that is
 *           responsible for handling the alarms triggered by the {@link AlarmManager} at
 *           the intervals when replication is required and handles resetting of alarms after
 *           reboot of the device.
 */
public abstract class PeriodicReplicationService<T extends PeriodicReplicationReceiver>
    extends ReplicationService {

    /* Name of the SharedPreferences file used to store alarm times. We store the alarm
     * times in preferences so we can reset the alarms as accurately as possible after reboot
     * and so we can adjust alarm times when components bind to or unbind from this Service.
     * So that multiple PeriodicReplicationService instances can be used in one application without
     * interfering with each other, the preferences in this file are stored using keys prefixed with
     * the name of the concrete class implementing the PeriodicReplicationService. */
    static final String PREFERENCES_FILE_NAME = "com.cloudant.preferences";

    /* We store the elapsed time since booting at which the last alarm occurred in SharedPreferences
     * using a key with this suffix. This is used to set the initial alarm time when periodic
     * replications are started. */
    private static final String LAST_ALARM_ELAPSED_TIME_SUFFIX = ".lastAlarmElapsed";

    /* We store the wall-clock time at which the last alarm occurred in SharedPreferences
     * using a key with this suffix. This is used to set the initial alarm after a reboot. */
    private static final String LAST_ALARM_CLOCK_TIME_SUFFIX = ".lastAlarmClock";

    /* We store a flag indicating whether periodic replications are enabled in SharedPreferences
     * using a key with this suffix. We have to store the flag persistently as the service may be
     * stopped and started by the operating system. */
    private static final String PERIODIC_REPLICATION_ENABLED_SUFFIX
        = ".periodicReplicationsActive";

    /* We store a flag indicating whether periodic replications were explicitly stopped in
     * SharedPreferences using a key with this suffix. We have to store the flag persistently as
     * the service may be stopped and started by the operating system. */
    private static final String EXPLICITLY_STOPPED_SUFFIX = ".explicitlyStopped";

    /* We store a flag indicating whether there are replications pending in
     * SharedPreferences using a key with this suffix. Replications may be pending because they
     * are currently in progress and have not yet completed, or becasue a previous scheduled
     * replication didn't take place because the conditions for replication were not met. We have
     * to store the flag persistently as the service may be stopped and started by the operating
     * system. */
    private static final String REPLICATIONS_PENDING_SUFFIX = ".replicationsPending";

    private static final long MILLISECONDS_IN_SECOND = 1000L;

    /**
     * To start periodic replications, this value should be passed to this service in an Intent
     * using an Extra with the {@link #EXTRA_COMMAND} key.
     * @see
     * <a href="http://github.com/cloudant/sync-android/blob/master/doc/replication-policies.md#controlling-the-replication-service">
     *     Replication Policy User Guide</a>
     */
    public static final int COMMAND_START_PERIODIC_REPLICATION = 2;

    /**
     * To stop periodic replications, this value should be passed to this service in an Intent
     * using an Extra with the {@link #EXTRA_COMMAND} key.
     * @see
     * <a href="http://github.com/cloudant/sync-android/blob/master/doc/replication-policies.md#controlling-the-replication-service">
     *     Replication Policy User Guide</a>
     */
    public static final int COMMAND_STOP_PERIODIC_REPLICATION = 3;

    /**
     * When the device is rebooted, this value should be passed to this service in an Intent
     * using an Extra with the {@link #EXTRA_COMMAND} key so that replications can be setup again
     * following a reboot.
     * @see
     * <a href="http://github.com/cloudant/sync-android/blob/master/doc/replication-policies.md#controlling-the-replication-service">
     *     Replication Policy User Guide</a>
     */
    public static final int COMMAND_DEVICE_REBOOTED = 4;

    /**
     * When the period between replications has been altered, this value should be passed to this
     * service in an Intent using an Extra with the {@link #EXTRA_COMMAND} key so that the interval
     * between replications can be re-evaluated.
     * @see
     * <a href="http://github.com/cloudant/sync-android/blob/master/doc/replication-policies.md#controlling-the-replication-service">
     *     Replication Policy User Guide</a>
     */
    public static final int COMMAND_RESET_REPLICATION_TIMERS = 5;

    private static final String TAG = "PRS";

    private SharedPreferences mPrefs;
    Class<T> clazz;
    protected boolean mBound;

    protected PeriodicReplicationService(Class<T> clazz) {
        this.clazz = clazz;
    }

    /**
     * If the stored preferences are in the old format, upgrade them to the new format so that
     * the app continues to work after upgrade to this version.
     */
    protected void upgradePreferences() {
        String alarmDueElapsed = "com.cloudant.sync.replication.PeriodicReplicationService.alarmDueElapsed";
        if (mPrefs.contains(alarmDueElapsed)) {
            // These are old style preferences. We need to rewrite them in the new form that allows
            // multiple replication policies.
            String alarmDueClock = "com.cloudant.sync.replication.PeriodicReplicationService.alarmDueClock";
            String replicationsActive = "com.cloudant.sync.replication.PeriodicReplicationService.periodicReplicationsActive";
            long elapsed = mPrefs.getLong(alarmDueElapsed, 0);
            long clock = mPrefs.getLong(alarmDueClock, 0);
            boolean enabled = mPrefs.getBoolean(replicationsActive, false);

            SharedPreferences.Editor editor = mPrefs.edit();
            editor.putLong(constructKey(LAST_ALARM_ELAPSED_TIME_SUFFIX),
                elapsed - (getIntervalInSeconds() * MILLISECONDS_IN_SECOND));
            editor.putLong(constructKey(LAST_ALARM_CLOCK_TIME_SUFFIX),
                clock - (getIntervalInSeconds() * MILLISECONDS_IN_SECOND));
            editor.putBoolean(constructKey(PERIODIC_REPLICATION_ENABLED_SUFFIX), enabled);

            editor.remove(alarmDueElapsed);
            editor.remove(alarmDueClock);
            editor.remove(replicationsActive);

            editor.apply();
        }
    }

    protected class ServiceHandler extends ReplicationService.ServiceHandler {
        public ServiceHandler(Looper looper) {
            super(looper);
        }

        @Override
        public void handleMessage(Message msg) {
            Intent intent = msg.getData().getParcelable(EXTRA_INTENT);

            switch (msg.arg2) {
                case COMMAND_START_PERIODIC_REPLICATION:
                    startPeriodicReplication();
                    releaseWakeLock(intent);
                    stopSelf(msg.arg1);
                    break;
                case COMMAND_STOP_PERIODIC_REPLICATION:
                    stopPeriodicReplication();
                    setExplicitlyStopped(true);
                    releaseWakeLock(intent);
                    stopSelf(msg.arg1);
                    break;
                case COMMAND_DEVICE_REBOOTED:
                    resetAlarmDueTimesOnReboot();
                    releaseWakeLock(intent);
                    stopSelf(msg.arg1);
                    break;
                case COMMAND_RESET_REPLICATION_TIMERS:
                    restartPeriodicReplications();
                    releaseWakeLock(intent);
                    stopSelf(msg.arg1);
                    break;
                default:
                    // Do nothing
                    break;
            }

            super.handleMessage(msg);
        }
    }

    @Override
    protected Handler getHandler(Looper looper) {
        return new ServiceHandler(looper);
    }

    @Override
    public void onCreate() {
        mPrefs = getSharedPreferences(PREFERENCES_FILE_NAME, Context.MODE_PRIVATE);
        upgradePreferences();
        super.onCreate();
    }

    @Override
    public synchronized IBinder onBind(Intent intent) {
        mBound = true;
        if (isPeriodicReplicationEnabled()) {
            restartPeriodicReplications();
        } else if (startReplicationOnBind()) {
            startPeriodicReplication();
        }
        return super.onBind(intent);
    }

    @Override
    public synchronized boolean onUnbind(Intent intent) {
        super.onUnbind(intent);
        mBound = false;
        if (isPeriodicReplicationEnabled()) {
            restartPeriodicReplications();
        }
        // Ensure onRebind is called when new clients bind to the service.
        return true;
    }

    @Override
    public synchronized void onRebind(Intent intent) {
        super.onRebind(intent);
        mBound = true;
        if (isPeriodicReplicationEnabled()) {
            restartPeriodicReplications();
        } else if (startReplicationOnBind()) {
            startPeriodicReplication();
        }
    }

    @Override
    protected void startReplications() {
        super.startReplications();
        setLastAlarmTime(0);
        setReplicationsPending(this, getClass(), true);
    }

    /** Start periodic replications. */
    public synchronized void startPeriodicReplication() {
        if (!isPeriodicReplicationEnabled()) {
            setPeriodicReplicationEnabled(true);
            AlarmManager alarmManager = (AlarmManager) getSystemService(Context.ALARM_SERVICE);
            Intent alarmIntent = new Intent(this, clazz);
            alarmIntent.setAction(PeriodicReplicationReceiver.ALARM_ACTION);
            // We need to use a BroadcastReceiver rather than sending the Intent directly to the
            // Service to ensure the device wakes up if it's asleep. Sending the Intent directly
            // to the Service would be unreliable.
            PendingIntent pendingAlarmIntent = PendingIntent.getBroadcast(this, 0, alarmIntent, 0);

            long initialTriggerTime;
            if (explicitlyStopped()) {
                // Replications were explicitly stopped, so we want the first replication to
                // happen immediately.
                initialTriggerTime = SystemClock.elapsedRealtime();
                setExplicitlyStopped(false);
            } else {
                // Replications were implicitly stopped (e.g. by rebooting the device), so we
                // want to resume the previous schedule.
                initialTriggerTime = getNextAlarmDueElapsedTime();
            }

            alarmManager.setInexactRepeating(AlarmManager.ELAPSED_REALTIME_WAKEUP,
                initialTriggerTime,
                getIntervalInSeconds() * MILLISECONDS_IN_SECOND,
                pendingAlarmIntent);
        } else {
            Log.i(TAG, "Attempted to start an already running alarm manager");
        }
    }

    /** Stop replications currently in progress and cancel future scheduled replications. */
    public synchronized void stopPeriodicReplication() {
        if (isPeriodicReplicationEnabled()) {
            setPeriodicReplicationEnabled(false);
            AlarmManager alarmManager = (AlarmManager) getSystemService(Context.ALARM_SERVICE);
            Intent alarmIntent = new Intent(this, clazz);
            alarmIntent.setAction(PeriodicReplicationReceiver.ALARM_ACTION);
            PendingIntent pendingAlarmIntent = PendingIntent.getBroadcast(this, 0, alarmIntent, 0);

            alarmManager.cancel(pendingAlarmIntent);

            stopReplications();
        } else {
            Log.i(TAG, "Attempted to stop an already stopped alarm manager");
        }
    }

    /** Stop and restart periodic replication. */
    final protected void restartPeriodicReplications() {
        stopPeriodicReplication();
        startPeriodicReplication();
    }

    /**
     * Store the time the alarm last fired, both as elapsed time since boot (using
     * {@link SystemClock#elapsedRealtime()}) and as standard "wall" clock time (using
     * {@link System#currentTimeMillis()}. We generally want to set our alarms based on the elapsed
     * time since booting as that is not affected by the system clock being reset. However, we use
     * the clock time to set the alarm after a reboot as clearly in this case the time since boot is
     * useless.
     * @param millisBeforeNow The number of milliseconds before the current time at which to record
     *                        that the alarm should have last fired.
     */
    private void setLastAlarmTime(long millisBeforeNow) {
        SharedPreferences.Editor editor = mPrefs.edit();
        editor.putLong(constructKey(LAST_ALARM_ELAPSED_TIME_SUFFIX),
            SystemClock.elapsedRealtime() - millisBeforeNow);
        editor.putLong(constructKey(LAST_ALARM_CLOCK_TIME_SUFFIX),
            System.currentTimeMillis() - millisBeforeNow);
        editor.apply();
    }

    /**
     * @return The time since the device was booted at which the next periodic replication should
     * begin, calculated by adding the replication interval to the SharedPreferences value
     * storing the time since device boot at which the last periodic replication began.
     */
    private long getNextAlarmDueElapsedTime() {
        return mPrefs.getLong(constructKey(LAST_ALARM_ELAPSED_TIME_SUFFIX), 0)
            + (getIntervalInSeconds() * MILLISECONDS_IN_SECOND);
    }

    /**
     * @return The wall clock time at which the next periodic replication should begin, calculated
     * by adding the replication interval to the SharedPreferences value storing the wall clock time
     * at which the last periodic replication began.
     */
    private long getNextAlarmDueClockTime() {
        return mPrefs.getLong(constructKey(LAST_ALARM_CLOCK_TIME_SUFFIX), 0)
            + (getIntervalInSeconds() * MILLISECONDS_IN_SECOND);
    }

    /**
     * Set a flag in SharedPreferences to indicate whether periodic replications are enabled.
     * @param running true to indicate that periodic replications are enabled, otherwise false.
     */
    private void setPeriodicReplicationEnabled(boolean running) {
        SharedPreferences.Editor editor = mPrefs.edit();
        editor.putBoolean(constructKey(PERIODIC_REPLICATION_ENABLED_SUFFIX), running);
        editor.apply();
    }

    /**
     * @return The value of the flag stored in SharedPreferences indicating whether periodic
     * replications are currently enabled.
     */
    private boolean isPeriodicReplicationEnabled() {
        return mPrefs.getBoolean(constructKey(PERIODIC_REPLICATION_ENABLED_SUFFIX),
            false);
    }

    public static boolean isPeriodicReplicationEnabled(Context context,
                                                        Class <? extends
                                                            PeriodicReplicationService> prsClass) {
        SharedPreferences prefs = context.getSharedPreferences(PREFERENCES_FILE_NAME, Context
            .MODE_PRIVATE);
        return prefs.getBoolean(constructKey(prsClass, PERIODIC_REPLICATION_ENABLED_SUFFIX), false);
    }

    /**
     * Set a flag in SharedPreferences to indicate whether periodic replications were explicitly
     * stopped.
     * @param explicitlyStopped true to indicate that periodic replications were stopped
     *                          explicitly by the sending of COMMAND_STOP_PERIODIC_REPLICATION,
     *                          otherwise false.
     */
    private void setExplicitlyStopped(boolean explicitlyStopped) {
        SharedPreferences.Editor editor = mPrefs.edit();
        editor.putBoolean(constructKey(EXPLICITLY_STOPPED_SUFFIX), explicitlyStopped);
        editor.apply();
    }

    /**
     * @return The value of the flag stored in SharedPreferences indicating whether periodic
     * replications were explicitly stopped.
     */
    private boolean explicitlyStopped() {
        return mPrefs.getBoolean(constructKey(EXPLICITLY_STOPPED_SUFFIX), true);
    }

    /**
     * Reset the alarm times stored in SharedPreferences following a reboot of the device.
     * After a reboot, the AlarmManager must be setup again so that periodic replications will
     * occur following reboot.
     */
    private void resetAlarmDueTimesOnReboot() {
        // As the device has been rebooted, we use clock time rather than elapsed time since
        // booting to set check whether we missed any alarms while the device was off and to
        // make sure the next alarm time isn't too far in the future (indicating the system clock
        // has been reset).
        //
        // We subtract the current time from the next expected alarm time and then do the
        // following checks:
        // * If it's less than zero, that means we missed an alarm when the device was off, so
        //   we schedule a replication immediately by setting the last alarm time to a time
        //   getIntervalInSeconds() ago.
        // * If it's more than getIntervalInSeconds() in the future, that means the system clock
        //   has been reset since the last alarm was fired.  Therefore, we check that the initial
        //   interval for the alarm is no later than getIntervalInSeconds() after the
        //   current time so we minimise the impact of the system clock being reset and will at most
        //   have to wait for the normal interval time.
        // * Otherwise, the clock time for the alarm seems reasonable, but we still need to update
        //   the SharedPreference for the elapsed time since boot at which the last alarm would have
        //   fired as that currently refers to the time since boot from the previous boot of the
        //   device.
        //
        // We don't actually setup the AlarmManager here as it is up to the subclass to determine
        // if all other conditions for the replication policy are met and determine whether to
        // restart replications after a reboot.
        setPeriodicReplicationEnabled(false);
        long initialInterval = getNextAlarmDueClockTime() - System.currentTimeMillis();
        if (initialInterval < 0) {
            setLastAlarmTime(getIntervalInSeconds() * MILLISECONDS_IN_SECOND);
        } else if (initialInterval > getIntervalInSeconds() * MILLISECONDS_IN_SECOND) {
            setLastAlarmTime(0);
        } else {
            setLastAlarmTime((getIntervalInSeconds() * MILLISECONDS_IN_SECOND) - initialInterval);
        }
    }

    /**
     * @return The interval (in seconds) between replications depending on whether a component is
     * bound to the service or not.
     */
    private int getIntervalInSeconds() {
        if (mBound) {
            return getBoundIntervalInSeconds();
        } else {
            return getUnboundIntervalInSeconds();
        }
    }

    String constructKey(String suffix) {
        return constructKey(getClass(), suffix);
    }

    static String constructKey(Class<? extends PeriodicReplicationService> prsClass,
                                       String suffix) {
        return prsClass.getName() + suffix;
    }

    @Override
    public void allReplicationsCompleted() {
        super.allReplicationsCompleted();
        setReplicationsPending(this, getClass(), false);
    }

    /**
     * Sets whether there are replications pending. This may be because replications are
     * currently in progress and have not yet completed, or because a previous scheduled
     * replication didn't take place because the conditions for replication were not met.
     * @param context
     * @param prsClass
     * @param pending true if there is a replication pending, or false otherwise.
     */
    public static void setReplicationsPending(Context context, Class<? extends
        PeriodicReplicationService> prsClass, boolean pending) {
        SharedPreferences prefs = context.getSharedPreferences(PREFERENCES_FILE_NAME, Context
            .MODE_PRIVATE);
        SharedPreferences.Editor editor = prefs.edit();
        editor.putBoolean(constructKey(prsClass, REPLICATIONS_PENDING_SUFFIX), pending);
        editor.apply();
    }

    /**
     * Gets whether there are replications pending. Replications may be pending because they are
     * currently in progress and have not yet completed, or because a previous scheduled
     * replication didn't take place because the conditions for replication were not met.
     * @param context
     * @param prsClass
     * @return
     */
    public static boolean replicationsPending(Context context, Class<? extends
        PeriodicReplicationService> prsClass) {
        SharedPreferences prefs = context.getSharedPreferences(PREFERENCES_FILE_NAME, Context
            .MODE_PRIVATE);
        return prefs.getBoolean(constructKey(prsClass, REPLICATIONS_PENDING_SUFFIX), true);
    }

    /**
     * @return The interval between replications to be used when a client is bound to the service.
     * To prolong battery life, this should be made as long as possible. Note that on Android 5.1
     * and above (API Level 22), if the interval is less than 60 seconds, Android expands the
     * interval to 60 seconds.
     */
    protected abstract int getBoundIntervalInSeconds();

    /**
     * @return The interval between replications to be used when no client is bound to the service.
     * To prolong battery life, this should be made as long as possible. Note that on Android 5.1
     * and above (API Level 22), if the interval is less than 60 seconds, Android expands the
     * interval to 60 seconds.
     */
    protected abstract int getUnboundIntervalInSeconds();

    /**
     * @return True if you want periodic replication to start when a client binds to the service and
     * false otherwise. This is called each when a client binds to the service, so should contain
     * any logic to determine whether replications should begin. E.g. if you want replications to
     * only happen when on WiFi, this should only return true if you are on WiFi when it is called.
     */
    protected abstract boolean startReplicationOnBind();
}

