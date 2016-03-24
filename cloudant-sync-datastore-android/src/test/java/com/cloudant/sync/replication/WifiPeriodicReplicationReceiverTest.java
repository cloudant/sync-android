package com.cloudant.sync.replication;

import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.test.AndroidTestCase;
import android.test.mock.MockContext;

import org.junit.Test;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import java.lang.Override;
import java.util.List;
import java.util.ArrayList;

import static org.mockito.Mockito.when;

public class WifiPeriodicReplicationReceiverTest extends AndroidTestCase {

    private WifiPeriodicReplicationReceiver mReceiver;
    private TestContext mMockContext;

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
     * {@link Intent#ACTION_BOOT_COMPLETED}, an {@link Intent} is sent out to start the Service
     * {@link ReplicationService} associated with
     * {@link WifiPeriodicReplicationReceiver} containing the extra
     * {@link ReplicationService#EXTRA_COMMAND} with the value
     * {@link PeriodicReplicationService#COMMAND_DEVICE_REBOOTED}.
     */
    @Test
    public void testBootCompleted() {
        Intent intent = new Intent(Intent.ACTION_BOOT_COMPLETED);

        mReceiver.onReceive(mMockContext, intent);
        assertEquals(1, mMockContext.getIntentsReceived().size());

        Intent receivedIntent = mMockContext.getIntentsReceived().get(0);
        assertEquals(ReplicationService.class.getName(), receivedIntent.getComponent().getClassName());
        assertNull(receivedIntent.getAction());
        assertEquals(PeriodicReplicationService.COMMAND_DEVICE_REBOOTED, receivedIntent.getIntExtra(ReplicationService.EXTRA_COMMAND, ReplicationService.COMMAND_NONE));
    }

    /**
     * Check that when {@link WifiPeriodicReplicationReceiver} receives
     * {@link ConnectivityManager#CONNECTIVITY_ACTION} and WiFi is connected,
     * an {@link Intent} is sent out to start the Service
     * {@link ReplicationService} associated with
     * {@link WifiPeriodicReplicationReceiver} containing the extra
     * {@link ReplicationService#EXTRA_COMMAND} with the value
     * {@link PeriodicReplicationService#COMMAND_START_PERIODIC_REPLICATION}.
     */
    public void testWifiConnected() {
        Intent intent = new Intent(ConnectivityManager.CONNECTIVITY_ACTION);

        mMockContext.setMockConnectivityManager(ConnectivityManager.TYPE_WIFI, true);

        mReceiver.onReceive(mMockContext, intent);
        assertEquals(1, mMockContext.getIntentsReceived().size());

        Intent receivedIntent = mMockContext.getIntentsReceived().get(0);
        assertEquals(ReplicationService.class.getName(), receivedIntent.getComponent().getClassName());
        assertNull(receivedIntent.getAction());
        assertEquals(PeriodicReplicationService.COMMAND_START_PERIODIC_REPLICATION, receivedIntent.getIntExtra(ReplicationService.EXTRA_COMMAND, ReplicationService.COMMAND_NONE));
    }

    /**
     * Check that when {@link WifiPeriodicReplicationReceiver} receives
     * {@link ConnectivityManager#CONNECTIVITY_ACTION} and WiFi is not connected,
     * an {@link Intent} is sent out to start the Service
     * {@link ReplicationService} associated with
     * {@link WifiPeriodicReplicationReceiver} containing the extra
     * {@link ReplicationService#EXTRA_COMMAND} with the value
     * {@link PeriodicReplicationService#COMMAND_STOP_PERIODIC_REPLICATION}.
     */
    public void testWifiDisconnected() {
        Intent intent = new Intent(ConnectivityManager.CONNECTIVITY_ACTION);

        mMockContext.setMockConnectivityManager(ConnectivityManager.TYPE_WIFI, false);

        mReceiver.onReceive(mMockContext, intent);
        assertEquals(1, mMockContext.getIntentsReceived().size());

        Intent receivedIntent = mMockContext.getIntentsReceived().get(0);
        assertEquals(ReplicationService.class.getName(), receivedIntent.getComponent().getClassName());
        assertNull(receivedIntent.getAction());
        assertEquals(PeriodicReplicationService.COMMAND_STOP_PERIODIC_REPLICATION, receivedIntent.getIntExtra(ReplicationService.EXTRA_COMMAND, ReplicationService.COMMAND_NONE));
    }

    /**
     * Check that when {@link WifiPeriodicReplicationReceiver} receives
     * {@link ConnectivityManager#CONNECTIVITY_ACTION} and the device is connected to
     * a non-WiFi network, an {@link Intent} is sent out to start the Service
     * {@link ReplicationService} associated with
     * {@link WifiPeriodicReplicationReceiver} containing the extra
     * {@link ReplicationService#EXTRA_COMMAND} with the value
     * {@link PeriodicReplicationService#COMMAND_STOP_PERIODIC_REPLICATION}.
     */
    public void testConnectedToMobileNetwork() {
        Intent intent = new Intent(ConnectivityManager.CONNECTIVITY_ACTION);

        mMockContext.setMockConnectivityManager(ConnectivityManager.TYPE_MOBILE, true);

        mReceiver.onReceive(mMockContext, intent);
        assertEquals(1, mMockContext.getIntentsReceived().size());

        Intent receivedIntent = mMockContext.getIntentsReceived().get(0);
        assertEquals(ReplicationService.class.getName(), receivedIntent.getComponent().getClassName());
        assertNull(receivedIntent.getAction());
        assertEquals(PeriodicReplicationService.COMMAND_STOP_PERIODIC_REPLICATION, receivedIntent.getIntExtra(ReplicationService.EXTRA_COMMAND, ReplicationService.COMMAND_NONE));
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

}
