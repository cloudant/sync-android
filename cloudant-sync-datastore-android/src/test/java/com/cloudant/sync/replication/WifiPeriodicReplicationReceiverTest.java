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

import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.test.AndroidTestCase;
import android.test.mock.MockContext;

import org.junit.Test;
import org.junit.BeforeClass;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.ArrayList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WifiPeriodicReplicationReceiverTest extends AndroidTestCase {

    private WifiPeriodicReplicationReceiver mReceiver;
    private TestContext mMockContext;
    private SharedPreferences mMockPreferences = mock(SharedPreferences.class);
    private SharedPreferences.Editor mMockPreferencesEditor;

    class TestContext extends MockContext {
        private List<Intent> mIntentsReceived = new ArrayList<Intent>();
        private ConnectivityManager mMockedConnectivityManager;

        @Override
        public String getPackageName() {
            return "com.cloudant.android";
        }

        @Override
        public Context getApplicationContext() {
            return this;
        }

        @Override
        public SharedPreferences getSharedPreferences(String name, int mode) {
            return mMockPreferences;
        }

        @Override
        public ComponentName startService(Intent service) {
            mIntentsReceived.add(service);
            if (service.hasExtra("android.support.content.wakelockid")) {
                return null;
            } else {
                return new ComponentName("mock.package.name", "MockClassName");
            }
        }

        @Override
        public Object getSystemService(String name) {
            if (name.equals(Context.CONNECTIVITY_SERVICE)) {
                return mMockedConnectivityManager;
            }

            return null;
        }

        public List<Intent> getIntentsReceived() {
            return mIntentsReceived;
        }

        public void setMockConnectivityManager(int type, boolean isConnectedOrConnecting) {
            NetworkInfo mockedNetworkInfo = Mockito.mock(NetworkInfo.class);
            when(mockedNetworkInfo.getType()).thenReturn(type);
            when(mockedNetworkInfo.isConnectedOrConnecting()).thenReturn(isConnectedOrConnecting);

            mMockedConnectivityManager = Mockito.mock(ConnectivityManager.class);
            when(mMockedConnectivityManager.getActiveNetworkInfo()).thenReturn(mockedNetworkInfo);
        }
    }

    private static class ReplicationService extends PeriodicReplicationService {
        public ReplicationService() {
            super(Receiver.class);
        }
        @Override
        protected int getBoundIntervalInSeconds() {
            return 20;
        }
        @Override
        protected int getUnboundIntervalInSeconds() {
            return 120;
        }
        @Override
        protected boolean startReplicationOnBind() {
            return true;
        }
    }

    private static class Receiver extends WifiPeriodicReplicationReceiver {
        public Receiver() {
            super(ReplicationService.class);
        }
    }

    @BeforeClass
    public void setUp() {
        mReceiver = new Receiver();
        mMockContext = new TestContext();
    }

    /**
     * Check that when {@link WifiPeriodicReplicationReceiver} receives
     * {@link Intent#ACTION_BOOT_COMPLETED}, the wasOnWifi flag is set to false and
     * an {@link Intent} is sent out to start the Service
     * {@link ReplicationService} associated with
     * {@link WifiPeriodicReplicationReceiver} containing the extra
     * {@link ReplicationService#EXTRA_COMMAND} with the value
     * {@link PeriodicReplicationService#COMMAND_DEVICE_REBOOTED}.
     */
    @Test
    public void testBootCompleted() {
        Intent intent = new Intent(Intent.ACTION_BOOT_COMPLETED);

        mMockPreferencesEditor = mock(SharedPreferences.Editor.class);
        when(mMockPreferences.edit()).thenReturn(mMockPreferencesEditor);

        mReceiver.onReceive(mMockContext, intent);
        verify(mMockPreferencesEditor, times(1)).putBoolean(ReplicationService.class.getName() + ".wasOnWifi",
                false);
        assertEquals(1, mMockContext.getIntentsReceived().size());

        Intent receivedIntent = mMockContext.getIntentsReceived().get(0);
        assertEquals(ReplicationService.class.getName(), receivedIntent.getComponent().getClassName());
        assertNull(receivedIntent.getAction());
        assertEquals(PeriodicReplicationService.COMMAND_DEVICE_REBOOTED, receivedIntent.getIntExtra(ReplicationService.EXTRA_COMMAND, ReplicationService.COMMAND_NONE));
    }

    /**
     * Check that when {@link WifiPeriodicReplicationReceiver} receives
     * {@link ConnectivityManager#CONNECTIVITY_ACTION} and WiFi is connected and there is a pending
     * replication an {@link Intent} is sent out to start the Service
     * {@link ReplicationService} associated with
     * {@link WifiPeriodicReplicationReceiver} containing the extra
     * {@link ReplicationService#EXTRA_COMMAND} with the value
     * {@link PeriodicReplicationService#COMMAND_START_REPLICATION}.
     */
    public void testWifiConnected() {
        Intent intent = new Intent(ConnectivityManager.CONNECTIVITY_ACTION);

        mMockContext.setMockConnectivityManager(ConnectivityManager.TYPE_WIFI, true);
        when(mMockPreferences.getBoolean(ReplicationService.class.getName() + ".wasOnWifi",
            false)).thenReturn(false);
        when(mMockPreferences.getBoolean(ReplicationService.class.getName() + "" +
            ".replicationsPending", true)).thenReturn(true);

        mMockPreferencesEditor = mock(SharedPreferences.Editor.class);
        when(mMockPreferences.edit()).thenReturn(mMockPreferencesEditor);

        mReceiver.onReceive(mMockContext, intent);
        verify(mMockPreferencesEditor, times(1)).putBoolean(ReplicationService.class.getName() + ".wasOnWifi",
            true);
        assertEquals(1, mMockContext.getIntentsReceived().size());

        Intent receivedIntent = mMockContext.getIntentsReceived().get(0);
        assertEquals(ReplicationService.class.getName(), receivedIntent.getComponent().getClassName());
        assertNull(receivedIntent.getAction());
        assertEquals(PeriodicReplicationService.COMMAND_START_REPLICATION, receivedIntent.getIntExtra(ReplicationService.EXTRA_COMMAND, ReplicationService.COMMAND_NONE));
    }

    /**
     * Check that when {@link WifiPeriodicReplicationReceiver} receives
     * {@link ConnectivityManager#CONNECTIVITY_ACTION} and WiFi is not connected,
     * an {@link Intent} is sent out to stop the Service
     * {@link ReplicationService} associated with
     * {@link WifiPeriodicReplicationReceiver} containing the extra
     * {@link ReplicationService#EXTRA_COMMAND} with the value
     * {@link PeriodicReplicationService#COMMAND_STOP_REPLICATION}.
     */
    public void testWifiDisconnected() {
        Intent intent = new Intent(ConnectivityManager.CONNECTIVITY_ACTION);

        mMockContext.setMockConnectivityManager(ConnectivityManager.TYPE_WIFI, false);
        when(mMockPreferences.getBoolean(ReplicationService.class.getName() + ".wasOnWifi",
            false)).thenReturn(true);
        mMockPreferencesEditor = mock(SharedPreferences.Editor.class);
        when(mMockPreferences.edit()).thenReturn(mMockPreferencesEditor);

        mReceiver.onReceive(mMockContext, intent);
        verify(mMockPreferencesEditor, times(1)).putBoolean(ReplicationService.class.getName() + ".wasOnWifi",
            false);
        assertEquals(1, mMockContext.getIntentsReceived().size());

        Intent receivedIntent = mMockContext.getIntentsReceived().get(0);
        assertEquals(ReplicationService.class.getName(), receivedIntent.getComponent().getClassName());
        assertNull(receivedIntent.getAction());
        assertEquals(PeriodicReplicationService.COMMAND_STOP_REPLICATION, receivedIntent.getIntExtra(ReplicationService.EXTRA_COMMAND, ReplicationService.COMMAND_NONE));
    }

    /**
     * Check that when {@link WifiPeriodicReplicationReceiver} receives
     * {@link ConnectivityManager#CONNECTIVITY_ACTION} and the device is connected to
     * a non-WiFi network, an {@link Intent} is sent out to stop the Service
     * {@link ReplicationService} associated with
     * {@link WifiPeriodicReplicationReceiver} containing the extra
     * {@link ReplicationService#EXTRA_COMMAND} with the value
     * {@link PeriodicReplicationService#COMMAND_STOP_REPLICATION}.
     */
    public void testConnectedToMobileNetwork() {
        Intent intent = new Intent(ConnectivityManager.CONNECTIVITY_ACTION);

        mMockContext.setMockConnectivityManager(ConnectivityManager.TYPE_MOBILE, true);
        when(mMockPreferences.getBoolean(ReplicationService.class.getName() + ".wasOnWifi",
            false)).thenReturn(true);
        mMockPreferencesEditor = mock(SharedPreferences.Editor.class);
        when(mMockPreferences.edit()).thenReturn(mMockPreferencesEditor);

        mReceiver.onReceive(mMockContext, intent);
        verify(mMockPreferencesEditor, times(1)).putBoolean(ReplicationService.class.getName() + ".wasOnWifi",
            false);

        assertEquals(1, mMockContext.getIntentsReceived().size());

        Intent receivedIntent = mMockContext.getIntentsReceived().get(0);
        assertEquals(ReplicationService.class.getName(), receivedIntent.getComponent().getClassName());
        assertNull(receivedIntent.getAction());
        assertEquals(PeriodicReplicationService.COMMAND_STOP_REPLICATION, receivedIntent.getIntExtra(ReplicationService.EXTRA_COMMAND, ReplicationService.COMMAND_NONE));
    }

    /**
     * Check that when {@link WifiPeriodicReplicationReceiver} receives
     * an unknown action no Intent is sent.
     */
    public void testUnknownAction() {
        Intent intent = new Intent(Intent.ACTION_ANSWER);

        mMockContext.setMockConnectivityManager(ConnectivityManager.TYPE_WIFI, true);

        mReceiver.onReceive(mMockContext, intent);
        assertEquals(0, mMockContext.getIntentsReceived().size());
    }

    /**
     * Check that when {@link WifiPeriodicReplicationReceiver} receives
     * {@link PeriodicReplicationReceiver#ALARM_ACTION} and WiFi is not connected,
     * the replications pending flag is set to true.
     */
    public void testAlarmActionWifiNotConnected() {
        Intent intent = new Intent(PeriodicReplicationReceiver.ALARM_ACTION);

        mMockContext.setMockConnectivityManager(ConnectivityManager.TYPE_WIFI, false);
        mMockPreferencesEditor = mock(SharedPreferences.Editor.class);
        when(mMockPreferences.edit()).thenReturn(mMockPreferencesEditor);

        mReceiver.onReceive(mMockContext, intent);

        ArgumentCaptor<String> captorPrefKeys = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Boolean> captorPrefValues = ArgumentCaptor.forClass(Boolean.class);
        List<String> prefKeys = captorPrefKeys.getAllValues();
        List<Boolean> prefValues = captorPrefValues.getAllValues();
        verify(mMockPreferencesEditor, times(1)).putBoolean
            (captorPrefKeys.capture(),
                captorPrefValues.capture());

        assertEquals("com.cloudant.sync.replication" +
            ".WifiPeriodicReplicationReceiverTest$ReplicationService.replicationsPending",
            prefKeys.get(0));
        assertTrue("Replications pending flag should be true", prefValues.get(0));
    }

    /**
     * Check that when {@link WifiPeriodicReplicationReceiver} receives
     * {@link PeriodicReplicationReceiver#ALARM_ACTION} and WiFi is connected,
     *  an {@link Intent} is sent out to start the Service
     * {@link ReplicationService} associated with
     * {@link WifiPeriodicReplicationReceiver} containing the extra
     * {@link ReplicationService#EXTRA_COMMAND} with the value
     * {@link PeriodicReplicationService#COMMAND_START_REPLICATION}.
     */
    public void testAlarmActionWifiConnected() {
        Intent intent = new Intent(PeriodicReplicationReceiver.ALARM_ACTION);

        mMockContext.setMockConnectivityManager(ConnectivityManager.TYPE_WIFI, true);
        mMockPreferencesEditor = mock(SharedPreferences.Editor.class);
        when(mMockPreferences.edit()).thenReturn(mMockPreferencesEditor);

        mReceiver.onReceive(mMockContext, intent);

        assertEquals(1, mMockContext.getIntentsReceived().size());

        Intent receivedIntent = mMockContext.getIntentsReceived().get(0);
        assertEquals(ReplicationService.class.getName(), receivedIntent.getComponent().getClassName());
        assertNull(receivedIntent.getAction());
        assertEquals(PeriodicReplicationService.COMMAND_START_REPLICATION, receivedIntent.getIntExtra(ReplicationService.EXTRA_COMMAND, ReplicationService.COMMAND_NONE));
 }

}
