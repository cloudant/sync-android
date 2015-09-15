package com.cloudant.sync.replication;

import com.cloudant.sync.notifications.ReplicationCompleted;
import com.cloudant.sync.notifications.ReplicationErrored;
import com.google.common.eventbus.EventBus;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicationPolicyManagerTest extends ReplicationTestBase {

    URI source;
    BasicReplicator replicator;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        source = getCouchConfig(getDbName()).getRootUri();

        prepareTwoDocumentsInRemoteDB();
    }

    private void prepareTwoDocumentsInRemoteDB() {
        Bar bar1 = BarUtils.createBar(remoteDb, "Tom", 31);
        couchClient.create(bar1);
        Bar bar2 = BarUtils.createBar(remoteDb, "Jerry", 52);
        couchClient.create(bar2);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with a single
     * {@link Replicator}, when {@link ReplicationPolicyManager#start()} is called,
     * the {@link Replicator#start()} method is called on the {@link Replicator}.
     */
    @Test
    public void testSingleStartSingleReplicator() {
        Replicator mockReplicator = mock(Replicator.class);
        EventBus mockEventBus = mock(EventBus.class);
        when(mockReplicator.getEventBus()).thenReturn(mockEventBus);
        ReplicationPolicyManager rpm = new TestPolicy();
        rpm.addReplicators(mockReplicator);
        verify(mockReplicator, never()).start();
        rpm.start();
        verify(mockReplicator, times(1)).start();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with multiple
     * {@link Replicator}s, when {@link ReplicationPolicyManager#start()} is called,
     * the {@link Replicator#start()} method is called on each {@link Replicator}.
     */
    @Test
    public void testSingleStartMultipleReplicators() {
        Replicator mockReplicator1 = mock(Replicator.class);
        Replicator mockReplicator2 = mock(Replicator.class);
        Replicator mockReplicator3 = mock(Replicator.class);
        EventBus mockEventBus = mock(EventBus.class);
        when(mockReplicator1.getEventBus()).thenReturn(mockEventBus);
        when(mockReplicator2.getEventBus()).thenReturn(mockEventBus);
        when(mockReplicator3.getEventBus()).thenReturn(mockEventBus);
        ReplicationPolicyManager rpm = new TestPolicy();
        rpm.addReplicators(mockReplicator1, mockReplicator2, mockReplicator3);
        verify(mockReplicator1, never()).start();
        verify(mockReplicator2, never()).start();
        verify(mockReplicator3, never()).start();
        rpm.start();
        verify(mockReplicator1, times(1)).start();
        verify(mockReplicator2, times(1)).start();
        verify(mockReplicator3, times(1)).start();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with a single
     * {@link Replicator}, when {@link ReplicationPolicyManager#start()} is called,
     * more than once before the {@link Replicator} has completed replication,
     * the {@link Replicator#start()} method is called only once the {@link Replicator}.
     */
    @Test
    public void testMultipleStartSingleReplicator() {
        Replicator mockReplicator = mock(Replicator.class);
        EventBus mockEventBus = mock(EventBus.class);
        when(mockReplicator.getEventBus()).thenReturn(mockEventBus);
        ReplicationPolicyManager rpm = new TestPolicy();
        rpm.addReplicators(mockReplicator);
        verify(mockReplicator, never()).start();
        rpm.start();
        rpm.start();
        verify(mockReplicator, times(1)).start();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with multiple
     * {@link Replicator}s, when {@link ReplicationPolicyManager#start()} is called,
     * more than once before {@link Replicator} has completed replication,
     * the {@link Replicator#start()} method is called only once on each {@link Replicator}.
     */
    @Test
    public void testMultipleStartMultipleReplicators() {
        Replicator mockReplicator1 = mock(Replicator.class);
        Replicator mockReplicator2 = mock(Replicator.class);
        Replicator mockReplicator3 = mock(Replicator.class);
        EventBus mockEventBus = mock(EventBus.class);
        when(mockReplicator1.getEventBus()).thenReturn(mockEventBus);
        when(mockReplicator2.getEventBus()).thenReturn(mockEventBus);
        when(mockReplicator3.getEventBus()).thenReturn(mockEventBus);
        ReplicationPolicyManager rpm = new TestPolicy();
        rpm.addReplicators(mockReplicator1, mockReplicator2, mockReplicator3);
        verify(mockReplicator1, never()).start();
        verify(mockReplicator2, never()).start();
        verify(mockReplicator3, never()).start();
        rpm.start();
        rpm.start();
        verify(mockReplicator1, times(1)).start();
        verify(mockReplicator2, times(1)).start();
        verify(mockReplicator3, times(1)).start();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with a single
     * {@link Replicator}, when {@link ReplicationPolicyManager#start()} is called,
     * the {@link Replicator#start()} method is called on the {@link Replicator}.
     */
    @Test
    public void testStopSingleReplicator() {
        Replicator mockReplicator = mock(Replicator.class);
        EventBus mockEventBus = mock(EventBus.class);
        when(mockReplicator.getEventBus()).thenReturn(mockEventBus);
        ReplicationPolicyManager rpm = new TestPolicy();
        rpm.addReplicators(mockReplicator);
        verify(mockReplicator, never()).start();
        rpm.start();
        rpm.stop();
        verify(mockReplicator, times(1)).stop();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with multiple
     * {@link Replicator}s, when {@link ReplicationPolicyManager#start()} is called,
     * more than once before {@link Replicator} has completed replication,
     * the {@link Replicator#start()} method is called only once on each {@link Replicator}.
     */
    @Test
    public void testStopMultipleReplicators() {
        Replicator mockReplicator1 = mock(Replicator.class);
        Replicator mockReplicator2 = mock(Replicator.class);
        Replicator mockReplicator3 = mock(Replicator.class);
        EventBus mockEventBus = mock(EventBus.class);
        when(mockReplicator1.getEventBus()).thenReturn(mockEventBus);
        when(mockReplicator2.getEventBus()).thenReturn(mockEventBus);
        when(mockReplicator3.getEventBus()).thenReturn(mockEventBus);
        ReplicationPolicyManager rpm = new TestPolicy();
        rpm.addReplicators(mockReplicator1, mockReplicator2, mockReplicator3);
        verify(mockReplicator1, never()).start();
        verify(mockReplicator2, never()).start();
        verify(mockReplicator3, never()).start();
        rpm.start();
        rpm.stop();
        verify(mockReplicator1, times(1)).stop();
        verify(mockReplicator2, times(1)).stop();
        verify(mockReplicator3, times(1)).stop();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with a single
     * {@link Replicator}, when {@link ReplicationPolicyManager#start()} is called for a
     * second time after the replicator is explicitly stopped, the {@link Replicator#start()} method
     * is called a second time on the {@link Replicator}.
     */
    @Test
    public void testRestartStoppedSingleReplicator() {
        Replicator mockReplicator = mock(Replicator.class);
        EventBus mockEventBus = mock(EventBus.class);
        when(mockReplicator.getEventBus()).thenReturn(mockEventBus);
        ReplicationPolicyManager rpm = new TestPolicy();
        rpm.addReplicators(mockReplicator);
        verify(mockReplicator, never()).start();
        rpm.start();
        rpm.stop();
        rpm.start();
        verify(mockReplicator, times(2)).start();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with multiple
     * {@link Replicator}s, when {@link ReplicationPolicyManager#start()} is called for a
     * second time after the replicator is explicitly stopped, the {@link Replicator#start()} method
     * is called a second time on the {@link Replicator}.
     */
    @Test
    public void testRestartStoppedMultipleReplicators() {
        Replicator mockReplicator1 = mock(Replicator.class);
        Replicator mockReplicator2 = mock(Replicator.class);
        Replicator mockReplicator3 = mock(Replicator.class);
        EventBus mockEventBus = mock(EventBus.class);
        when(mockReplicator1.getEventBus()).thenReturn(mockEventBus);
        when(mockReplicator2.getEventBus()).thenReturn(mockEventBus);
        when(mockReplicator3.getEventBus()).thenReturn(mockEventBus);
        ReplicationPolicyManager rpm = new TestPolicy();
        rpm.addReplicators(mockReplicator1, mockReplicator2, mockReplicator3);
        verify(mockReplicator1, never()).start();
        verify(mockReplicator2, never()).start();
        verify(mockReplicator3, never()).start();
        rpm.start();
        rpm.stop();
        rpm.start();
        verify(mockReplicator1, times(2)).start();
        verify(mockReplicator2, times(2)).start();
        verify(mockReplicator3, times(2)).start();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with a single
     * {@link Replicator}, when {@link ReplicationPolicyManager#start()} is called for a
     * second time after the replicator completed normally, the {@link Replicator#start()} method
     * is called a second time on the {@link Replicator}.
     */
    @Test
    public void testRestartCompletedSingleReplicator() {
        Replicator mockReplicator = mock(Replicator.class);
        EventBus mockEventBus = new EventBus();
        when(mockReplicator.getEventBus()).thenReturn(mockEventBus);
        ReplicationPolicyManager rpm = new TestPolicy();
        rpm.addReplicators(mockReplicator);
        verify(mockReplicator, never()).start();
        rpm.start();
        mockEventBus.post(new ReplicationCompleted(mockReplicator, 1, 1));
        rpm.start();
        verify(mockReplicator, times(2)).start();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with multiple
     * {@link Replicator}s, when {@link ReplicationPolicyManager#start()} is called for a
     * second time after each replicator completed normally, the {@link Replicator#start()} method
     * is called a second time on the {@link Replicator}s.
     */
    @Test
    public void testRestartCompletedMultipleReplicators() {
        Replicator mockReplicator1 = mock(Replicator.class);
        Replicator mockReplicator2 = mock(Replicator.class);
        Replicator mockReplicator3 = mock(Replicator.class);
        EventBus mockEventBus1 = new EventBus();
        EventBus mockEventBus2 = new EventBus();
        EventBus mockEventBus3 = new EventBus();
        when(mockReplicator1.getEventBus()).thenReturn(mockEventBus1);
        when(mockReplicator2.getEventBus()).thenReturn(mockEventBus2);
        when(mockReplicator3.getEventBus()).thenReturn(mockEventBus3);
        ReplicationPolicyManager rpm = new TestPolicy();
        rpm.addReplicators(mockReplicator1, mockReplicator2, mockReplicator3);
        verify(mockReplicator1, never()).start();
        verify(mockReplicator2, never()).start();
        verify(mockReplicator3, never()).start();
        rpm.start();
        mockEventBus1.post(new ReplicationCompleted(mockReplicator1, 1, 1));
        mockEventBus2.post(new ReplicationCompleted(mockReplicator2, 1, 1));
        mockEventBus3.post(new ReplicationCompleted(mockReplicator3, 1, 1));
        rpm.start();
        verify(mockReplicator1, times(2)).start();
        verify(mockReplicator2, times(2)).start();
        verify(mockReplicator3, times(2)).start();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with a single
     * {@link Replicator}, when {@link ReplicationPolicyManager#start()} is called for a
     * second time after the replicator errored, the {@link Replicator#start()} method
     * is called a second time on the {@link Replicator}.
     */
    @Test
    public void testRestartErroredSingleReplicator() {
        Replicator mockReplicator = mock(Replicator.class);
        EventBus mockEventBus = new EventBus();
        when(mockReplicator.getEventBus()).thenReturn(mockEventBus);
        ReplicationPolicyManager rpm = new TestPolicy();
        rpm.addReplicators(mockReplicator);
        verify(mockReplicator, never()).start();
        rpm.start();
        mockEventBus.post(new ReplicationErrored(mockReplicator, null));
        rpm.start();
        verify(mockReplicator, times(2)).start();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with multiple
     * {@link Replicator}s, when {@link ReplicationPolicyManager#start()} is called for a
     * second time after each replicator errored, the {@link Replicator#start()} method
     * is called a second time on the {@link Replicator}s.
     */
    @Test
    public void testRestartErroredMultipleReplicators() {
        Replicator mockReplicator1 = mock(Replicator.class);
        Replicator mockReplicator2 = mock(Replicator.class);
        Replicator mockReplicator3 = mock(Replicator.class);
        EventBus mockEventBus1 = new EventBus();
        EventBus mockEventBus2 = new EventBus();
        EventBus mockEventBus3 = new EventBus();
        when(mockReplicator1.getEventBus()).thenReturn(mockEventBus1);
        when(mockReplicator2.getEventBus()).thenReturn(mockEventBus2);
        when(mockReplicator3.getEventBus()).thenReturn(mockEventBus3);
        ReplicationPolicyManager rpm = new TestPolicy();
        rpm.addReplicators(mockReplicator1, mockReplicator2, mockReplicator3);
        verify(mockReplicator1, never()).start();
        verify(mockReplicator2, never()).start();
        verify(mockReplicator3, never()).start();
        rpm.start();
        mockEventBus1.post(new ReplicationErrored(mockReplicator1, null));
        mockEventBus2.post(new ReplicationErrored(mockReplicator2, null));
        mockEventBus3.post(new ReplicationErrored(mockReplicator3, null));
        rpm.start();
        verify(mockReplicator1, times(2)).start();
        verify(mockReplicator2, times(2)).start();
        verify(mockReplicator3, times(2)).start();
    }

    /** A trivial policy where when you call {@link TestPolicy#start()} it calls
     * {@link ReplicationPolicyManager#startReplications()} and when you call {@link TestPolicy#stop()}
     * it calls {@link ReplicationPolicyManager#stopReplications()}.
     */
    class TestPolicy extends ReplicationPolicyManager {
        public TestPolicy() {
            super();
        }

        @Override
        public void start() {
            startReplications();
        }

        @Override
        public void stop() {
            stopReplications();
        }
    }

}
