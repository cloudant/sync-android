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
import android.net.wifi.WifiManager;
import android.os.SystemClock;
import android.test.ServiceTestCase;

import com.cloudant.android.TestReplicationService;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.File;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicationServiceTest extends ServiceTestCase<TestReplicationService> {

    private static final long ALARM_TOLERANCE_MS = 500;
    private static final int DEFAULT_WAIT_SECONDS = 5;
    private static final int MILLIS_IN_SECOND = 1000;

    /** An arbitrary number of replication intervals that's greater than one. */
    private static final int ARBITRARY_NUMBER_OF_INTERVALS = 10;

    /** An arbitrary number of replication intervals that's between zero and one. */
    private static final float ARBITRARY_FRACTIONAL_INTERVAL = 0.5f;

    private static final String PREFERENCE_CLASS_NAME = "com.cloudant.android.TestReplicationService";

    private Context mMockContext;
    private SharedPreferences mMockPreferences;
    private SharedPreferences.Editor mMockPreferencesEditor;
    private AlarmManager mMockAlarmManager;
    private WifiManager mMockWifiManager;
    private WifiManager.WifiLock mMockWifiLock;
    private Replicator[] mMockReplicators;

    ReplicationPolicyManager mMockReplicationPolicyManager;
    private TestReplicationService mService;

    public ReplicationServiceTest() {
        super(TestReplicationService.class);
    }

    @BeforeClass
    public void setUp() {
        mMockContext = mock(Context.class);
        mMockPreferences = mock(SharedPreferences.class);
        when(mMockContext.getSharedPreferences("com.cloudant.preferences", Context.MODE_PRIVATE)).thenReturn(mMockPreferences);
        when(mMockContext.getPackageName()).thenReturn("cloudant.com.androidtest");
        when(mMockContext.getDir(anyString(), anyInt())).thenReturn(new File("/data/data/cloudant.com.androidtest/files"));
        when(mMockContext.getApplicationContext()).thenReturn(mMockContext);
        mMockPreferencesEditor = mock(SharedPreferences.Editor.class);
        when(mMockPreferences.edit()).thenReturn(mMockPreferencesEditor);
        mMockAlarmManager = mock(AlarmManager.class);
        mMockWifiManager = mock(WifiManager.class);
        mMockWifiLock = mock(WifiManager.WifiLock.class);
        mMockReplicationPolicyManager = mock(ReplicationPolicyManager.class);
        mMockReplicators = new Replicator[]{mock(Replicator.class)};
    }

    @Test
    public void testServiceStart() {
        Intent intent = new Intent(getContext(), TestReplicationService.class);
        startService(intent);
    }

    @Test
    public void testServiceBind() {
        Intent intent = new Intent(getContext(), TestReplicationService.class);
        bindService(intent);
    }

    /**
     * Check that when an intent is sent to the {@link PeriodicReplicationService} indicating that
     * the device has been rebooted, if the value in SharedPreferences for the last alarm
     * is more than the alarm period in the past, then the SharedPreferences are updated to indicate
     * that the last alarm occurred one alarm period ago.
     */
    @Test
    public void testOnStartCommandRebootImmediateAlarm() {
        PeriodicReplicationService service = new TestReplicationService(mMockContext);
        Intent intent = new Intent(mMockContext, TestReplicationService.class);
        intent.putExtra(ReplicationService.EXTRA_COMMAND, PeriodicReplicationService.COMMAND_DEVICE_REBOOTED);
        service.onCreate();
        final CountDownLatch latch = new CountDownLatch(1);

        // An arbitrary time more than 1 replication interval in the past.
        long moreThan1ReplicationPeriodInPast = System.currentTimeMillis() - intervalMillis(ARBITRARY_NUMBER_OF_INTERVALS, service);
        when(mMockPreferences.getLong(PREFERENCE_CLASS_NAME + ".lastAlarmClock", 0)).thenReturn(moreThan1ReplicationPeriodInPast);

        service.setOperationStartedListener(new PeriodicReplicationService
            .OperationStartedListener() {
            @Override
            public void operationStarted(int operationId) {
                assertEquals("Unexpected command received",
                    PeriodicReplicationService.COMMAND_DEVICE_REBOOTED,
                    operationId);
                latch.countDown();
            }
        });

        ArgumentCaptor<String> captorPrefKeys = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Long> captorPrefValues = ArgumentCaptor.forClass(Long.class);
        service.onStartCommand(intent, 0, 0);
        service.setReplicators(mMockReplicators);
        try {
            assertTrue("The countdown should reach zero", latch.await(DEFAULT_WAIT_SECONDS, TimeUnit.SECONDS));
            verify(mMockPreferencesEditor, times(2)).putLong(captorPrefKeys.capture(), captorPrefValues.capture());
            List<String> prefsKeys = captorPrefKeys.getAllValues();
            List<Long> prefsValues = captorPrefValues.getAllValues();
            long expectedElapsedTime = SystemClock.elapsedRealtime() - intervalMillis(1, service);
            long expectedRealTime = System.currentTimeMillis() - intervalMillis(1, service);
            checkElapsedPreferenceName(prefsKeys.get(0));
            checkElapsedTime(expectedElapsedTime, prefsValues.get(0));
            checkClockPreferenceName(prefsKeys.get(1));
            checkClockTime(expectedRealTime, prefsValues.get(1));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Check that when an intent is sent to the {@link PeriodicReplicationService} indicating that
     * the device has been rebooted, if the last alarm time in SharedPreferences is within
     * the alarm period of the current time, the last alarm time is reset based on the clock time
     * stored in SharedPreferences. This is important because although the clock time won't be
     * greatly affected, the elapsed time since boot must be updated.
     */
    @Test
    public void testOnStartCommandRebootDelayedAlarm() {
        PeriodicReplicationService service = new TestReplicationService(mMockContext);
        Intent intent = new Intent(mMockContext, TestReplicationService.class);
        intent.putExtra(ReplicationService.EXTRA_COMMAND, PeriodicReplicationService.COMMAND_DEVICE_REBOOTED);
        service.onCreate();
        final CountDownLatch latch = new CountDownLatch(1);

        // An arbitrary time less than 1 replication interval in the past.
        long between0and1ReplicationPeriodsInPast = System.currentTimeMillis() - intervalMillis(ARBITRARY_FRACTIONAL_INTERVAL, service);
        when(mMockPreferences.getLong(PREFERENCE_CLASS_NAME + ".lastAlarmClock", 0)).thenReturn(between0and1ReplicationPeriodsInPast);

        service.setOperationStartedListener(new PeriodicReplicationService
            .OperationStartedListener() {
            @Override
            public void operationStarted(int operationId) {
                assertEquals("Unexpected command received",
                    PeriodicReplicationService.COMMAND_DEVICE_REBOOTED,
                    operationId);
                latch.countDown();
            }
        });

        ArgumentCaptor<String> captorPrefKeys = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Long> captorPrefValues = ArgumentCaptor.forClass(Long.class);
        service.onStartCommand(intent, 0, 0);
        service.setReplicators(mMockReplicators);
        try {
            assertTrue("The countdown should reach zero", latch.await(DEFAULT_WAIT_SECONDS, TimeUnit.SECONDS));
            verify(mMockPreferencesEditor, times(2)).putLong(captorPrefKeys.capture(), captorPrefValues.capture());
            List<String> prefsKeys = captorPrefKeys.getAllValues();
            List<Long> prefsValues = captorPrefValues.getAllValues();
            long expectedElapsedTime = SystemClock.elapsedRealtime() - intervalMillis(ARBITRARY_FRACTIONAL_INTERVAL, service);
            long expectedRealTime = System.currentTimeMillis() - intervalMillis(ARBITRARY_FRACTIONAL_INTERVAL, service);
            checkElapsedPreferenceName(prefsKeys.get(0));
            checkElapsedTime(expectedElapsedTime, prefsValues.get(0));
            checkClockPreferenceName(prefsKeys.get(1));
            checkClockTime(expectedRealTime, prefsValues.get(1));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Check that when an intent is sent to the {@link PeriodicReplicationService} indicating that
     * the device has been rebooted, if the last alarm time stored in SharedPreferences is
     * after the current time, the last alarm time is updated to the current time.
     */
    @Test
    public void testOnStartCommandRebootLateAlarm() {
        PeriodicReplicationService service = new TestReplicationService(mMockContext);
        Intent intent = new Intent(mMockContext, TestReplicationService.class);
        intent.putExtra(PeriodicReplicationService.EXTRA_COMMAND, PeriodicReplicationService.COMMAND_DEVICE_REBOOTED);
        service.onCreate();
        final CountDownLatch latch = new CountDownLatch(1);

        // An arbitrary time more than 1 replication interval in the future.
        long moreThan1ReplicationPeriodInFuture = System.currentTimeMillis() + intervalMillis(ARBITRARY_NUMBER_OF_INTERVALS, service);
        when(mMockPreferences.getLong(PREFERENCE_CLASS_NAME + ".lastAlarmClock", 0)).thenReturn(moreThan1ReplicationPeriodInFuture);

        service.setOperationStartedListener(new PeriodicReplicationService
            .OperationStartedListener() {
            @Override
            public void operationStarted(int operationId) {
                assertEquals("Unexpected command received",
                    PeriodicReplicationService.COMMAND_DEVICE_REBOOTED,
                    operationId);
                latch.countDown();
            }
        });

        ArgumentCaptor<String> captorPrefKeys = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Long> captorPrefValues = ArgumentCaptor.forClass(Long.class);
        service.onStartCommand(intent, 0, 0);
        service.setReplicators(mMockReplicators);
        try {
            assertTrue("The countdown should reach zero", latch.await(DEFAULT_WAIT_SECONDS, TimeUnit.SECONDS));
            verify(mMockPreferencesEditor, times(2)).putLong(captorPrefKeys.capture(), captorPrefValues.capture());
            List<String> prefsKeys = captorPrefKeys.getAllValues();
            List<Long> prefsValues = captorPrefValues.getAllValues();
            long expectedElapsedTime = SystemClock.elapsedRealtime();
            long expectedRealTime = System.currentTimeMillis();
            checkElapsedPreferenceName(prefsKeys.get(0));
            checkElapsedTime(expectedElapsedTime, prefsValues.get(0));
            checkClockPreferenceName(prefsKeys.get(1));
            checkClockTime(expectedRealTime, prefsValues.get(1));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Check that when an intent is sent to the {@link PeriodicReplicationService} indicating that
     * periodic replications should be started, the {@link AlarmManager}, is setup to fire
     * at the correct time and with the correct frequency.
     * @param afterExplicitStop If true, indicates that periodic replications were previously
     *                          stopped explicitly, in which case the initial alarm is expected
     *                          to be triggered immediately. If false, indicates that periodic
     *                          replications were previously stopped implicitly (e.g. by rebooting
     *                          the device), in which case the existing replication schedule
     *                          should be maintained.
     */
    private void testOnStartCommandStartPeriodicReplications(boolean afterExplicitStop) {
        PeriodicReplicationService service = new TestReplicationService(mMockContext);
        Intent intent = new Intent(mMockContext, TestReplicationService.class);
        intent.putExtra(PeriodicReplicationService.EXTRA_COMMAND, PeriodicReplicationService.COMMAND_START_PERIODIC_REPLICATION);
        service.onCreate();
        final CountDownLatch latch = new CountDownLatch(1);

        // An arbitrary time less than 1 replication interval in the past.
        long between0and1ReplicationPeriodsInPast = SystemClock.elapsedRealtime() - intervalMillis(ARBITRARY_FRACTIONAL_INTERVAL, service);
        when(mMockPreferences.getBoolean(PREFERENCE_CLASS_NAME + ".periodicReplicationsActive", false)).thenReturn(false);
        when(mMockContext.getSystemService(Context.ALARM_SERVICE)).thenReturn(mMockAlarmManager);
        when(mMockPreferences.getLong(PREFERENCE_CLASS_NAME + ".lastAlarmElapsed", 0)).thenReturn(between0and1ReplicationPeriodsInPast);
        when(mMockPreferences.getBoolean(eq(PREFERENCE_CLASS_NAME + ".explicitlyStopped"), anyBoolean())).thenReturn(afterExplicitStop);

        service.setOperationStartedListener(new PeriodicReplicationService
            .OperationStartedListener() {
            @Override
            public void operationStarted(int operationId) {
                assertEquals("Unexpected command received",
                    PeriodicReplicationService.COMMAND_START_PERIODIC_REPLICATION,
                    operationId);
                latch.countDown();
            }
        });

        service.onStartCommand(intent, 0, 0);
        service.setReplicators(mMockReplicators);
        try {
            assertTrue("The countdown should reach zero", latch.await(DEFAULT_WAIT_SECONDS, TimeUnit.SECONDS));
            ArgumentCaptor<String> captorPrefKeys = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<Boolean> captorPrefValues = ArgumentCaptor.forClass(Boolean.class);
            List<String> prefKeys = captorPrefKeys.getAllValues();
            List<Boolean> prefValues = captorPrefValues.getAllValues();
            verify(mMockPreferencesEditor, times(afterExplicitStop ? 2 : 1)).putBoolean
                (captorPrefKeys.capture(),
                captorPrefValues.capture());

            assertEquals(PREFERENCE_CLASS_NAME + ".periodicReplicationsActive", prefKeys.get(0));
            assertTrue("Alarm manager should be set in running state", prefValues.get(0));

            if (afterExplicitStop) {
                assertEquals(PREFERENCE_CLASS_NAME + ".explicitlyStopped", prefKeys.get(1));
                assertFalse("Alarm manager should be set in running state", prefValues.get(1));
            }

            ArgumentCaptor<Integer> captorType = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<Long> captorTriggerAtMillis = ArgumentCaptor.forClass(Long.class);
            ArgumentCaptor<Long> captorIntervalMillis = ArgumentCaptor.forClass(Long.class);

            verify(mMockAlarmManager, times(1)).setInexactRepeating(captorType.capture(), captorTriggerAtMillis.capture(), captorIntervalMillis.capture(), Mockito.any(PendingIntent.class));
            assertEquals("Incorrect alarm type", AlarmManager.ELAPSED_REALTIME_WAKEUP, (int) captorType.getValue());

            if (afterExplicitStop) {
                long expectedInitialTriggerTime = SystemClock.elapsedRealtime();
                assertThat("Initial trigger time not within " + ALARM_TOLERANCE_MS + "ms of expected time",
                    (double)captorTriggerAtMillis.getValue(), closeTo(expectedInitialTriggerTime,
                        ALARM_TOLERANCE_MS));
            } else {
                long expectedInitialTriggerTime = between0and1ReplicationPeriodsInPast + intervalMillis(1, service);
                assertEquals("Incorrect initial trigger time", expectedInitialTriggerTime, (long) captorTriggerAtMillis.getValue());
            }
            assertEquals("Incorrect alarm period", intervalMillis(1, service), (long) captorIntervalMillis.getValue ());

            // Unfortunately, we can't do much testing of the PendingIntent itself as there aren't
            // methods to extract anything useful.
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Check that when an intent is sent to the {@link PeriodicReplicationService} indicating that
     * periodic replications should be started following an implicit stop (e.g. device reboot), the
     * {@link AlarmManager} is setup to fire initially on the existing schedule and then with the
     * correct frequency.
     */
    @Test
    public void testOnStartCommandStartPeriodicReplicationsAfterImplicitStop() {
        testOnStartCommandStartPeriodicReplications(false);
    }

    /**
     * Check that when an intent is sent to the {@link PeriodicReplicationService} indicating that
     * periodic replications should be started following an explicit stop, the
     * {@link AlarmManager} is setup to fire immediately and then with the correct frequency.
     */
    @Test
    public void testOnStartCommandStartPeriodicReplicationsAfterExplicitStop() {
        testOnStartCommandStartPeriodicReplications(true);
    }

    /**
     * Check that when an intent is sent to the {@link PeriodicReplicationService} indicating that
     * periodic replications should be started, if they have already been started, the
     * {@link AlarmManager} is not invoked to restart the periodic replications.
     */
    @Test
    public void testOnStartCommandStartPeriodicReplicationsAlreadyStarted() {
        PeriodicReplicationService service = new TestReplicationService(mMockContext);
        Intent intent = new Intent(mMockContext, TestReplicationService.class);
        intent.putExtra(PeriodicReplicationService.EXTRA_COMMAND, PeriodicReplicationService.COMMAND_START_PERIODIC_REPLICATION);
        service.onCreate();
        final CountDownLatch latch = new CountDownLatch(1);

        when(mMockPreferences.getBoolean(PREFERENCE_CLASS_NAME + ".periodicReplicationsActive", false)).thenReturn(true);

        service.setOperationStartedListener(new PeriodicReplicationService
            .OperationStartedListener() {
            @Override
            public void operationStarted(int operationId) {
                assertEquals("Unexpected command received",
                    PeriodicReplicationService.COMMAND_START_PERIODIC_REPLICATION,
                    operationId);
                latch.countDown();
            }
        });

        service.onStartCommand(intent, 0, 0);
        service.setReplicators(mMockReplicators);
        try {
            assertTrue("The countdown should reach zero", latch.await(DEFAULT_WAIT_SECONDS, TimeUnit.SECONDS));
            verify(mMockAlarmManager, never()).setInexactRepeating(Mockito.anyInt(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any(PendingIntent.class));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Check that when an intent is sent to the {@link PeriodicReplicationService} indicating that
     * the replication timers have changed, the service is restarted with the new timer settings.
     */
    @Test
    public void testOnStartCommandResetReplicationTimers() {
        PeriodicReplicationService service = new TestReplicationService(mMockContext);
        service.onCreate();
        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);

        long thirtySecondsAgo = SystemClock.elapsedRealtime() - 30000;
        when(mMockPreferences.getBoolean(PREFERENCE_CLASS_NAME + ".periodicReplicationsActive", false)).thenReturn(false);
        when(mMockContext.getSystemService(Context.ALARM_SERVICE)).thenReturn(mMockAlarmManager);
        when(mMockPreferences.getLong(PREFERENCE_CLASS_NAME + ".lastAlarmElapsed", 0)).thenReturn(thirtySecondsAgo);

        service.setOperationStartedListener(new PeriodicReplicationService
            .OperationStartedListener() {
            @Override
            public void operationStarted(int operationId) {
                if (operationId == PeriodicReplicationService.COMMAND_START_PERIODIC_REPLICATION
                    && latch2.getCount() == 1) {
                    latch1.countDown();
                } else if (operationId == PeriodicReplicationService
                    .COMMAND_RESET_REPLICATION_TIMERS && latch1.getCount() == 0) {
                    latch2.countDown();
                } else {
                    fail("Unexpected command received");
                }
            }
        });

        final int unboundIntervalSeconds1 = 50;
        ((TestReplicationService) service).setUnboundIntervalSeconds(unboundIntervalSeconds1);

        Intent startIntent = new Intent(mMockContext, TestReplicationService.class);
        startIntent.putExtra(PeriodicReplicationService.EXTRA_COMMAND, PeriodicReplicationService.COMMAND_START_PERIODIC_REPLICATION);
        service.onStartCommand(startIntent, 0, 0);
        service.setReplicators(mMockReplicators);

        try {
            assertTrue("The first countdown should reach zero", latch1.await(DEFAULT_WAIT_SECONDS,
                TimeUnit.SECONDS));

            final int unboundIntervalSeconds2 = 999;
            ((TestReplicationService) service).setUnboundIntervalSeconds(unboundIntervalSeconds2);

            Intent resetTimersIntent = new Intent(mMockContext, TestReplicationService.class);
            resetTimersIntent.putExtra(PeriodicReplicationService.EXTRA_COMMAND, PeriodicReplicationService.COMMAND_RESET_REPLICATION_TIMERS);
            service.onStartCommand(resetTimersIntent, 0, 0);

            assertTrue("The second countdown should reach zero", latch2.await(DEFAULT_WAIT_SECONDS,
                TimeUnit.SECONDS));
            ArgumentCaptor<Integer> captorType = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<Long> captorTriggerAtMillis = ArgumentCaptor.forClass(Long.class);
            ArgumentCaptor<Long> captorIntervalMillis = ArgumentCaptor.forClass(Long.class);
            List<Integer> alarmTypes = captorType.getAllValues();
            List<Long> triggerAtTimes = captorTriggerAtMillis.getAllValues();
            List<Long> intervals = captorIntervalMillis.getAllValues();

            verify(mMockAlarmManager, times(2)).setInexactRepeating(captorType.capture(), captorTriggerAtMillis.capture(), captorIntervalMillis.capture(), Mockito.any(PendingIntent.class));
            assertEquals("Incorrect alarm type (first invocation)", AlarmManager
                    .ELAPSED_REALTIME_WAKEUP,
                (int) alarmTypes.get(0));
            long expectedInitialTriggerTime = thirtySecondsAgo + (unboundIntervalSeconds1 * MILLIS_IN_SECOND);
            assertEquals("Incorrect initial trigger time (first invocation)", expectedInitialTriggerTime, (long)
                triggerAtTimes.get(0));
            assertEquals("Incorrect alarm period (first invocation)", (unboundIntervalSeconds1 * MILLIS_IN_SECOND),
                (long) intervals.get(0));

            assertEquals("Incorrect alarm type (second invocation)", AlarmManager
                .ELAPSED_REALTIME_WAKEUP, (int) alarmTypes.get(1));
            expectedInitialTriggerTime = thirtySecondsAgo + (unboundIntervalSeconds2 * MILLIS_IN_SECOND);
            assertEquals("Incorrect initial trigger time (second invocation)", expectedInitialTriggerTime, (long)
                triggerAtTimes.get(1));
            assertEquals("Incorrect alarm period (second invocation)", (unboundIntervalSeconds2 * MILLIS_IN_SECOND),
                (long) intervals.get(1));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Check that when an intent is sent to the {@link PeriodicReplicationService} indicating that
     * periodic replications should be stopped, the {@link AlarmManager}, is cancelled.
     */
    @Test
    public void testOnStartCommandStopPeriodicReplications() {
        PeriodicReplicationService service = new TestReplicationService(mMockContext);
        Intent intent = new Intent(mMockContext, TestReplicationService.class);
        intent.putExtra(PeriodicReplicationService.EXTRA_COMMAND, PeriodicReplicationService.COMMAND_STOP_PERIODIC_REPLICATION);
        service.onCreate();
        final CountDownLatch latch = new CountDownLatch(1);

        long timeReturned = SystemClock.elapsedRealtime();
        when(mMockPreferences.getBoolean(PREFERENCE_CLASS_NAME + ".periodicReplicationsActive", false)).thenReturn(true);
        when(mMockContext.getSystemService(Context.ALARM_SERVICE)).thenReturn(mMockAlarmManager);
        when(mMockPreferences.getLong(PREFERENCE_CLASS_NAME + ".lastAlarmElapsed", 0)).thenReturn(timeReturned);

        service.setOperationStartedListener(new PeriodicReplicationService
            .OperationStartedListener() {
            @Override
            public void operationStarted(int operationId) {
                assertEquals("Unexpected command received",
                    PeriodicReplicationService.COMMAND_STOP_PERIODIC_REPLICATION,
                    operationId);
                latch.countDown();
            }
        });

        service.onStartCommand(intent, 0, 0);
        service.setReplicators(mMockReplicators);
        try {
            assertTrue("The countdown should reach zero", latch.await(DEFAULT_WAIT_SECONDS, TimeUnit.SECONDS));
            ArgumentCaptor<String> captorPrefKeys = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<Boolean> captorPrefValues = ArgumentCaptor.forClass(Boolean.class);
            List<String> prefKeys = captorPrefKeys.getAllValues();
            List<Boolean> prefValues = captorPrefValues.getAllValues();

            verify(mMockPreferencesEditor, times(2)).putBoolean(captorPrefKeys.capture(),
                captorPrefValues.capture());
            assertEquals(PREFERENCE_CLASS_NAME + ".periodicReplicationsActive", prefKeys.get(0));
            assertFalse("Alarm manager should be set in stopped state", prefValues.get(0));
            assertEquals(PREFERENCE_CLASS_NAME + ".explicitlyStopped", prefKeys.get(1));
            assertTrue("Periodic replications should be set in explictly stopped state", prefValues
                .get(1));

            verify(mMockAlarmManager, times(1)).cancel(Mockito.any(PendingIntent.class));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Check that when an intent is sent to the {@link PeriodicReplicationService} indicating that
     * periodic replications should be stopped, if they have already been stopped, the
     * {@link AlarmManager} is not cancelled again.
     */
    @Test
    public void testOnStartCommandStopPeriodicReplicationsAlreadyStopped() {
        PeriodicReplicationService service = new TestReplicationService(mMockContext);
        Intent intent = new Intent(mMockContext, TestReplicationService.class);
        intent.putExtra(PeriodicReplicationService.EXTRA_COMMAND, PeriodicReplicationService.COMMAND_STOP_PERIODIC_REPLICATION);
        service.onCreate();
        final CountDownLatch latch = new CountDownLatch(1);

        long timeReturned = SystemClock.elapsedRealtime();
        when(mMockPreferences.getBoolean(PREFERENCE_CLASS_NAME + ".periodicReplicationsActive", false)).thenReturn(false);
        when(mMockContext.getSystemService(Context.ALARM_SERVICE)).thenReturn(mMockAlarmManager);
        when(mMockPreferences.getLong(PREFERENCE_CLASS_NAME + ".lastAlarmElapsed", 0)).thenReturn(timeReturned);

        service.setOperationStartedListener(new PeriodicReplicationService
            .OperationStartedListener() {
            @Override
            public void operationStarted(int operationId) {
                assertEquals("Unexpected command received",
                    PeriodicReplicationService.COMMAND_STOP_PERIODIC_REPLICATION,
                    operationId);
                latch.countDown();
            }
        });

        service.onStartCommand(intent, 0, 0);
        service.setReplicators(mMockReplicators);
        try {
            assertTrue("The countdown should reach zero", latch.await(DEFAULT_WAIT_SECONDS, TimeUnit.SECONDS));
            verify(mMockAlarmManager, never()).cancel(Mockito.any(PendingIntent.class));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Check that when an intent is sent to the {@link PeriodicReplicationService} indicating that
     * a replication should be started, a {@link android.net.wifi.WifiManager.WifiLock} is acquired,
     * the {@link ReplicationPolicyManager} is started, the last alarm times in SharedPreferences
     * are updated to the current time and the replications pending flag is set to true.
     */
    @Test
    public void testOnStartCommandStartReplication() {
        PeriodicReplicationService service = new TestReplicationService(mMockContext);
        Intent intent = new Intent(mMockContext, TestReplicationService.class);
        intent.putExtra(PeriodicReplicationService.EXTRA_COMMAND, PeriodicReplicationService.COMMAND_START_REPLICATION);
        service.setReplicationPolicyManager(mMockReplicationPolicyManager);

        when(mMockContext.getSystemService(Context.WIFI_SERVICE)).thenReturn(mMockWifiManager);
        when(mMockWifiManager.createWifiLock(WifiManager.WIFI_MODE_FULL_HIGH_PERF, "ReplicationService")).thenReturn(mMockWifiLock);

        service.onCreate();
        final CountDownLatch latch = new CountDownLatch(1);

        service.setOperationStartedListener(new PeriodicReplicationService
            .OperationStartedListener() {
            @Override
            public void operationStarted(int operationId) {
                assertEquals("Unexpected command received",
                    PeriodicReplicationService.COMMAND_START_REPLICATION,
                    operationId);
                latch.countDown();
            }
        });

        service.onStartCommand(intent, 0, 0);
        service.setReplicators(mMockReplicators);
        try {
            assertTrue("The countdown should reach zero", latch.await(DEFAULT_WAIT_SECONDS, TimeUnit.SECONDS));
            verify(mMockWifiLock, times(1)).acquire();
            verify(mMockReplicationPolicyManager, times(1)).startReplications();
            ArgumentCaptor<String> captorPrefKeys = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<Long> captorPrefValues = ArgumentCaptor.forClass(Long.class);
            verify(mMockPreferencesEditor, times(2)).putLong(captorPrefKeys.capture(), captorPrefValues.capture());
            List<String> prefsKeys = captorPrefKeys.getAllValues();
            List<Long> prefsValues = captorPrefValues.getAllValues();
            long expectedElapsedTime = SystemClock.elapsedRealtime();
            long expectedRealTime = System.currentTimeMillis();
            checkElapsedPreferenceName(prefsKeys.get(0));
            checkElapsedTime(expectedElapsedTime, prefsValues.get(0));
            checkClockPreferenceName(prefsKeys.get(1));
            checkClockTime(expectedRealTime, prefsValues.get(1));

            ArgumentCaptor<String> captorPrefKeys2 = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<Boolean> captorPrefValues2 = ArgumentCaptor.forClass(Boolean.class);
            List<String> prefKeys = captorPrefKeys2.getAllValues();
            List<Boolean> prefValues = captorPrefValues2.getAllValues();
            verify(mMockPreferencesEditor, times(1)).putBoolean
                (captorPrefKeys2.capture(),
                    captorPrefValues2.capture());

            assertEquals(PREFERENCE_CLASS_NAME + ".replicationsPending", prefKeys.get(0));
            assertTrue("Replications pending flag should be true", prefValues.get(0));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Check that when an intent is sent to the {@link PeriodicReplicationService} indicating that
     * a replication should be stopped, the {@link android.net.wifi.WifiManager.WifiLock} is
     * released, the {@link ReplicationPolicyManager} is stopped and the replications pending flag
     * remains true.
     */
    @Test
    public void testOnStartCommandStopReplication() {
        PeriodicReplicationService service = new TestReplicationService(mMockContext);
        service.setReplicationPolicyManager(mMockReplicationPolicyManager);

        when(mMockContext.getSystemService(Context.WIFI_SERVICE)).thenReturn(mMockWifiManager);
        when(mMockWifiManager.createWifiLock(WifiManager.WIFI_MODE_FULL_HIGH_PERF, "ReplicationService")).thenReturn(mMockWifiLock);
        when(mMockWifiLock.isHeld()).thenReturn(true);

        service.onCreate();
        final CountDownLatch latch = new CountDownLatch(2);

        service.setOperationStartedListener(new PeriodicReplicationService
            .OperationStartedListener() {
            @Override
            public void operationStarted(int operationId) {
                if (operationId == PeriodicReplicationService.COMMAND_START_REPLICATION && latch.getCount() == 2) {
                    latch.countDown();
                } else if (operationId == PeriodicReplicationService.COMMAND_STOP_REPLICATION && latch.getCount() == 1) {
                    latch.countDown();
                } else {
                    fail("Unexpected command received");
                }
            }
        });

        Intent startIntent = new Intent(mMockContext, TestReplicationService.class);
        startIntent.putExtra(PeriodicReplicationService.EXTRA_COMMAND, PeriodicReplicationService.COMMAND_START_REPLICATION);
        service.onStartCommand(startIntent, 0, 0);
        Intent stopIntent = new Intent(mMockContext, TestReplicationService.class);
        stopIntent.putExtra(PeriodicReplicationService.EXTRA_COMMAND, PeriodicReplicationService.COMMAND_STOP_REPLICATION);
        service.onStartCommand(stopIntent, 0, 0);
        service.setReplicators(mMockReplicators);

        try {
            assertTrue("The countdown should reach zero", latch.await(DEFAULT_WAIT_SECONDS, TimeUnit.SECONDS));
            verify(mMockWifiLock, times(1)).release();
            verify(mMockReplicationPolicyManager, times(1)).stopReplications();

            ArgumentCaptor<String> captorPrefKeys = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<Boolean> captorPrefValues = ArgumentCaptor.forClass(Boolean.class);
            List<String> prefKeys = captorPrefKeys.getAllValues();
            List<Boolean> prefValues = captorPrefValues.getAllValues();
            verify(mMockPreferencesEditor, times(1)).putBoolean
                    (captorPrefKeys.capture(),
                            captorPrefValues.capture());

            assertEquals(PREFERENCE_CLASS_NAME + ".replicationsPending", prefKeys.get(0));
            assertTrue("Replications pending flag should be true", prefValues.get(0));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Check that when an intent is sent to the {@link PeriodicReplicationService} indicating that
     * a replication should be started, the {@link PeriodicReplicationService} waits until
     * {@link ReplicationService#setReplicators(Replicator[])} is called before processing
     * the start replication operation.
     */
    @Test
    public void testOnStartCommandStartReplicationWaitsForSetReplicators() {
        PeriodicReplicationService service = new TestReplicationService(mMockContext);
        Intent intent = new Intent(mMockContext, TestReplicationService.class);
        intent.putExtra(PeriodicReplicationService.EXTRA_COMMAND, PeriodicReplicationService.COMMAND_START_REPLICATION);
        service.setReplicationPolicyManager(mMockReplicationPolicyManager);

        service.onCreate();
        final CountDownLatch latch = new CountDownLatch(1);

        service.setOperationStartedListener(new PeriodicReplicationService
            .OperationStartedListener() {
            @Override
            public void operationStarted(int operationId) {
                assertEquals("Unexpected command received",
                    PeriodicReplicationService.COMMAND_START_REPLICATION,
                    operationId);
                latch.countDown();
            }
        });

        service.onStartCommand(intent, 0, 0);
        try {
            // Wait a bit to check the OperationStartedListener isn't called.
            latch.await(1000, TimeUnit.MILLISECONDS);
            assertEquals("CountDownLatch should not be decremented", latch.getCount(), 1);
            service.setReplicators(mMockReplicators);
            assertTrue("The countdown should reach zero", latch.await(DEFAULT_WAIT_SECONDS, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Check that when intents are sent to the {@link PeriodicReplicationService}
     * the commands are queued until {@link ReplicationService#setReplicators(Replicator[])} is
     * called and the queued messages are then processed.
     */
    @Test
    public void testOnStartCommandStartReplicationCommandsQueuedBeforeSetReplicators() {
        PeriodicReplicationService service = new TestReplicationService(mMockContext);
        Intent startIntent = new Intent(mMockContext, TestReplicationService.class);
        startIntent.putExtra(PeriodicReplicationService.EXTRA_COMMAND, PeriodicReplicationService.COMMAND_START_REPLICATION);

        Intent stopIntent = new Intent(mMockContext, TestReplicationService.class);
        stopIntent.putExtra(PeriodicReplicationService.EXTRA_COMMAND, PeriodicReplicationService
            .COMMAND_STOP_REPLICATION);

        service.setReplicationPolicyManager(mMockReplicationPolicyManager);

        service.onCreate();
        final CountDownLatch latch = new CountDownLatch(2);

        service.setOperationStartedListener(new PeriodicReplicationService
            .OperationStartedListener() {
            @Override
            public void operationStarted(int operationId) {
                if (latch.getCount() == 2 && operationId == PeriodicReplicationService
                    .COMMAND_START_REPLICATION) {
                    latch.countDown();
                } else if (latch.getCount() == 1 && operationId == PeriodicReplicationService
                    .COMMAND_STOP_REPLICATION) {
                    latch.countDown();
                } else {
                    fail("Unexpected command or commands received in incorrect order");
                }
            }
        });

        service.onStartCommand(startIntent, 0, 0);
        service.onStartCommand(stopIntent, 0, 0);
        service.setReplicators(mMockReplicators);
        try {
            assertTrue("The countdown should reach zero", latch.await(DEFAULT_WAIT_SECONDS, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Check that when intents are sent to the {@link PeriodicReplicationService}, consecutive
     * duplicate commands are ignored and commands are queued until
     * {@link ReplicationService#setReplicators(Replicator[])}
     * is called and the queued messages are then processed.
     */
    @Test
    public void testOnStartCommandStartReplicationCommandsQueuedConsecutiveDuplicatesRemoved() {
        PeriodicReplicationService service = new TestReplicationService(mMockContext);
        Intent startIntent = new Intent(mMockContext, TestReplicationService.class);
        startIntent.putExtra(PeriodicReplicationService.EXTRA_COMMAND, PeriodicReplicationService.COMMAND_START_REPLICATION);

        Intent stopIntent = new Intent(mMockContext, TestReplicationService.class);
        stopIntent.putExtra(PeriodicReplicationService.EXTRA_COMMAND, PeriodicReplicationService
            .COMMAND_STOP_REPLICATION);

        service.setReplicationPolicyManager(mMockReplicationPolicyManager);

        service.onCreate();
        final CountDownLatch latch = new CountDownLatch(2);

        service.setOperationStartedListener(new PeriodicReplicationService
            .OperationStartedListener() {
            @Override
            public void operationStarted(int operationId) {
                if (latch.getCount() == 2 && operationId == PeriodicReplicationService
                    .COMMAND_START_REPLICATION) {
                    latch.countDown();
                } else if (latch.getCount() == 1 && operationId == PeriodicReplicationService
                    .COMMAND_STOP_REPLICATION) {
                    latch.countDown();
                } else {
                    fail("Unexpected command or commands received in incorrect order");
                }
            }
        });

        service.onStartCommand(startIntent, 0, 0);
        service.onStartCommand(startIntent, 0, 0);
        service.onStartCommand(stopIntent, 0, 0);
        service.setReplicators(mMockReplicators);
        try {
            assertTrue("The countdown should reach zero", latch.await(DEFAULT_WAIT_SECONDS, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Check that when {@link PeriodicReplicationService#setReplicators(Replicator[])}, is called
     * with a null argument, {@link IllegalArgumentException} is thrown.
     */
    @Test
    public void testSetReplicatorsNull() {
        PeriodicReplicationService service = new TestReplicationService(mMockContext);
        try {
            service.setReplicators(null);
            fail("IllegalArgumentException should have been thrown");
        } catch (IllegalArgumentException e) {
            // Success.
        }
    }

    /**
     * Check that when {@link PeriodicReplicationService#setReplicators(Replicator[])}, is called
     * with an empty array, {@link IllegalArgumentException} is thrown.
     */
    @Test
    public void testSetReplicatorsEmpty() {
        PeriodicReplicationService service = new TestReplicationService(mMockContext);
        try {
            service.setReplicators(new Replicator[]{});
            fail("IllegalArgumentException should have been thrown");
        } catch (IllegalArgumentException e) {
            // Success.
        }
    }

    /**
     * Check that when {@link PeriodicReplicationService#setReplicators(Replicator[])}, is called
     * multiple times, {@link IllegalStateException} is thrown.
     */
    @Test
    public void testSetReplicatorsMultipleInvocations() {
        PeriodicReplicationService service = new TestReplicationService(mMockContext);
        service.setReplicators(mMockReplicators);
        try {
            service.setReplicators(mMockReplicators);
            fail("IllegalStateException should have been thrown");
        } catch (IllegalStateException e) {
            // Success.
        }
    }

    private void checkClockTime(double expectedTime, double actualTime) {
        assertThat("Last alarm elapsed time not within " + ALARM_TOLERANCE_MS + "ms of expected time",
            actualTime, closeTo(expectedTime, ALARM_TOLERANCE_MS));
    }

    private void checkElapsedTime(double expectedTime, double actualTime) {
        assertThat("Last alarm clock time not within " + ALARM_TOLERANCE_MS + "ms of expected time",
            actualTime, closeTo(expectedTime, ALARM_TOLERANCE_MS));
    }

    private void checkClockPreferenceName(String actualName){
        assertEquals(PREFERENCE_CLASS_NAME + ".lastAlarmClock", actualName);
    }

    private void checkElapsedPreferenceName(String actualName){
        assertEquals(PREFERENCE_CLASS_NAME + ".lastAlarmElapsed", actualName);
    }

    /**
     * Multiplies {@code numberOfIntervals} by the number of milliseconds resulting from
     * calling the given {@code service}'s {@code getUnboundIntervalInSeconds()} method, and
     * returns the result.
     * @param numberOfIntervals The number of intervals.
     * @param service The service.
     * @return The number of milliseconds in {@code numberOfIntervals} replication periods.
     */
    private static long intervalMillis(double numberOfIntervals, PeriodicReplicationService
        service) {
        return (long)(numberOfIntervals * service.getUnboundIntervalInSeconds() * MILLIS_IN_SECOND);
    }
}
