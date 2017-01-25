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

package com.cloudant.sync.internal.replication;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cloudant.sync.event.EventBus;
import com.cloudant.sync.event.notifications.ReplicationCompleted;
import com.cloudant.sync.event.notifications.ReplicationErrored;
import com.cloudant.sync.replication.ReplicationPolicyManager;
import com.cloudant.sync.replication.Replicator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

public class ReplicationPolicyManagerTest extends ReplicationTestBase {

    URI source;
    ReplicatorImpl replicator;

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
     * {@link Replicator}, when the replication policy is started
     * the {@link Replicator#start()} method is called on the {@link Replicator}.
     */
    @Test
    public void testSingleStartSingleReplicator() {
        Replicator mockReplicator = mock(Replicator.class);
        EventBus mockEventBus = mock(EventBus.class);
        when(mockReplicator.getEventBus()).thenReturn(mockEventBus);
        TestPolicy replicationPolicy = new TestPolicy();
        replicationPolicy.addReplicators(mockReplicator);
        verify(mockReplicator, never()).start();
        replicationPolicy.start();
        verify(mockReplicator, times(1)).start();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with multiple
     * {@link Replicator}s, when the replication policy is started
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
        TestPolicy replicationPolicy = new TestPolicy();
        replicationPolicy.addReplicators(mockReplicator1, mockReplicator2, mockReplicator3);
        verify(mockReplicator1, never()).start();
        verify(mockReplicator2, never()).start();
        verify(mockReplicator3, never()).start();
        replicationPolicy.start();
        verify(mockReplicator1, times(1)).start();
        verify(mockReplicator2, times(1)).start();
        verify(mockReplicator3, times(1)).start();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with a single
     * {@link Replicator}, when the replication policy is started
     * more than once before the {@link Replicator} has completed replication,
     * the {@link Replicator#start()} method is called only once the {@link Replicator}.
     */
    @Test
    public void testMultipleStartSingleReplicator() {
        Replicator mockReplicator = mock(Replicator.class);
        EventBus mockEventBus = mock(EventBus.class);
        when(mockReplicator.getEventBus()).thenReturn(mockEventBus);
        TestPolicy replicationPolicy = new TestPolicy();
        replicationPolicy.addReplicators(mockReplicator);
        verify(mockReplicator, never()).start();
        replicationPolicy.start();
        replicationPolicy.start();
        verify(mockReplicator, times(1)).start();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with multiple
     * {@link Replicator}s, when the replication policy is started
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
        TestPolicy replicationPolicy = new TestPolicy();
        replicationPolicy.addReplicators(mockReplicator1, mockReplicator2, mockReplicator3);
        verify(mockReplicator1, never()).start();
        verify(mockReplicator2, never()).start();
        verify(mockReplicator3, never()).start();
        replicationPolicy.start();
        replicationPolicy.start();
        verify(mockReplicator1, times(1)).start();
        verify(mockReplicator2, times(1)).start();
        verify(mockReplicator3, times(1)).start();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with a single
     * {@link Replicator}, when the replication policy is started
     * the {@link Replicator#start()} method is called on the {@link Replicator}.
     */
    @Test
    public void testStopSingleReplicator() {
        Replicator mockReplicator = mock(Replicator.class);
        EventBus mockEventBus = mock(EventBus.class);
        when(mockReplicator.getEventBus()).thenReturn(mockEventBus);
        TestPolicy replicationPolicy = new TestPolicy();
        replicationPolicy.addReplicators(mockReplicator);
        verify(mockReplicator, never()).start();
        replicationPolicy.start();
        replicationPolicy.stop();
        verify(mockReplicator, times(1)).stop();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with multiple
     * {@link Replicator}s, when the replication policy is started
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
        TestPolicy replicationPolicy = new TestPolicy();
        replicationPolicy.addReplicators(mockReplicator1, mockReplicator2, mockReplicator3);
        verify(mockReplicator1, never()).start();
        verify(mockReplicator2, never()).start();
        verify(mockReplicator3, never()).start();
        replicationPolicy.start();
        replicationPolicy.stop();
        verify(mockReplicator1, times(1)).stop();
        verify(mockReplicator2, times(1)).stop();
        verify(mockReplicator3, times(1)).stop();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with a single
     * {@link Replicator}, when the replication policy is started for a
     * second time after the replicator is explicitly stopped, the {@link Replicator#start()} method
     * is called a second time on the {@link Replicator}.
     */
    @Test
    public void testRestartStoppedSingleReplicator() {
        Replicator mockReplicator = mock(Replicator.class);
        EventBus mockEventBus = mock(EventBus.class);
        when(mockReplicator.getEventBus()).thenReturn(mockEventBus);
        TestPolicy replicationPolicy = new TestPolicy();
        replicationPolicy.addReplicators(mockReplicator);
        verify(mockReplicator, never()).start();
        replicationPolicy.start();
        replicationPolicy.stop();
        replicationPolicy.start();
        verify(mockReplicator, times(2)).start();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with multiple
     * {@link Replicator}s, when the replication policy is started for a
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
        TestPolicy replicationPolicy = new TestPolicy();
        replicationPolicy.addReplicators(mockReplicator1, mockReplicator2, mockReplicator3);
        verify(mockReplicator1, never()).start();
        verify(mockReplicator2, never()).start();
        verify(mockReplicator3, never()).start();
        replicationPolicy.start();
        replicationPolicy.stop();
        replicationPolicy.start();
        verify(mockReplicator1, times(2)).start();
        verify(mockReplicator2, times(2)).start();
        verify(mockReplicator3, times(2)).start();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with a single
     * {@link Replicator}, when the replication policy is started for a
     * second time after the replicator completed normally, the {@link Replicator#start()} method
     * is called a second time on the {@link Replicator}.
     */
    @Test
    public void testRestartCompletedSingleReplicator() {
        Replicator mockReplicator = mock(Replicator.class);
        EventBus mockEventBus = new EventBus();
        when(mockReplicator.getEventBus()).thenReturn(mockEventBus);
        TestPolicy replicationPolicy = new TestPolicy();
        replicationPolicy.addReplicators(mockReplicator);
        verify(mockReplicator, never()).start();
        replicationPolicy.start();
        mockEventBus.post(new ReplicationCompleted(mockReplicator, 1, 1));
        replicationPolicy.start();
        verify(mockReplicator, times(2)).start();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with multiple
     * {@link Replicator}s, when the replication policy is started for a
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
        TestPolicy replicationPolicy = new TestPolicy();
        replicationPolicy.addReplicators(mockReplicator1, mockReplicator2, mockReplicator3);
        verify(mockReplicator1, never()).start();
        verify(mockReplicator2, never()).start();
        verify(mockReplicator3, never()).start();
        replicationPolicy.start();
        mockEventBus1.post(new ReplicationCompleted(mockReplicator1, 1, 1));
        mockEventBus2.post(new ReplicationCompleted(mockReplicator2, 1, 1));
        mockEventBus3.post(new ReplicationCompleted(mockReplicator3, 1, 1));
        replicationPolicy.start();
        verify(mockReplicator1, times(2)).start();
        verify(mockReplicator2, times(2)).start();
        verify(mockReplicator3, times(2)).start();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with multiple
     * {@link Replicator}s, when {@link ReplicationPolicyManager#start()} is called for a
     * second time after some replicators have completed normally and one is still in progress, the
     * {@link Replicator#start()} method is only called a second time on the {@link Replicator}s
     * that completed normally the first time and not on the one that is still in progress.
     */
    @Test
    public void testRestartCompletedAndInProgressReplicators() {
        Replicator mockReplicator1 = mock(Replicator.class);
        Replicator mockReplicator2 = mock(Replicator.class);
        Replicator mockReplicator3 = mock(Replicator.class);
        EventBus mockEventBus1 = new EventBus();
        EventBus mockEventBus2 = new EventBus();
        EventBus mockEventBus3 = new EventBus();
        when(mockReplicator1.getEventBus()).thenReturn(mockEventBus1);
        when(mockReplicator2.getEventBus()).thenReturn(mockEventBus2);
        when(mockReplicator3.getEventBus()).thenReturn(mockEventBus3);
        TestPolicy replicationPolicy = new TestPolicy();
        replicationPolicy.addReplicators(mockReplicator1, mockReplicator2, mockReplicator3);
        verify(mockReplicator1, never()).start();
        verify(mockReplicator2, never()).start();
        verify(mockReplicator3, never()).start();
        replicationPolicy.start();
        mockEventBus1.post(new ReplicationCompleted(mockReplicator1, 1, 1));
        mockEventBus3.post(new ReplicationCompleted(mockReplicator3, 1, 1));
        replicationPolicy.start();
        verify(mockReplicator1, times(2)).start();
        verify(mockReplicator2, times(1)).start();
        verify(mockReplicator3, times(2)).start();
    }


    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with a single
     * {@link Replicator}, when the replication policy is started for a
     * second time after the replicator errored, the {@link Replicator#start()} method
     * is called a second time on the {@link Replicator}.
     */
    @Test
    public void testRestartErroredSingleReplicator() {
        Replicator mockReplicator = mock(Replicator.class);
        EventBus mockEventBus = new EventBus();
        when(mockReplicator.getEventBus()).thenReturn(mockEventBus);
        TestPolicy replicationPolicy = new TestPolicy();
        replicationPolicy.addReplicators(mockReplicator);
        verify(mockReplicator, never()).start();
        replicationPolicy.start();
        mockEventBus.post(new ReplicationErrored(mockReplicator, null));
        replicationPolicy.start();
        verify(mockReplicator, times(2)).start();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with multiple
     * {@link Replicator}s, when the replication policy is started for a
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
        TestPolicy replicationPolicy = new TestPolicy();
        replicationPolicy.addReplicators(mockReplicator1, mockReplicator2, mockReplicator3);
        verify(mockReplicator1, never()).start();
        verify(mockReplicator2, never()).start();
        verify(mockReplicator3, never()).start();
        replicationPolicy.start();
        mockEventBus1.post(new ReplicationErrored(mockReplicator1, null));
        mockEventBus2.post(new ReplicationErrored(mockReplicator2, null));
        mockEventBus3.post(new ReplicationErrored(mockReplicator3, null));
        replicationPolicy.start();
        verify(mockReplicator1, times(2)).start();
        verify(mockReplicator2, times(2)).start();
        verify(mockReplicator3, times(2)).start();
    }

    /**
     * Check that when the {@link ReplicationPolicyManager} is setup with multiple
     * {@link Replicator}s, when {@link ReplicationPolicyManager#start()} is called for a
     * second time after some replicators errored and one has not completed, the
     * {@link Replicator#start()} method is only called a second time on the
     * {@link Replicator}s that did error the first time and not on the one that is still in
     * progress.
     */
    @Test
    public void testRestartErroredAndInProgressReplicators() {
        Replicator mockReplicator1 = mock(Replicator.class);
        Replicator mockReplicator2 = mock(Replicator.class);
        Replicator mockReplicator3 = mock(Replicator.class);
        EventBus mockEventBus1 = new EventBus();
        EventBus mockEventBus2 = new EventBus();
        EventBus mockEventBus3 = new EventBus();
        when(mockReplicator1.getEventBus()).thenReturn(mockEventBus1);
        when(mockReplicator2.getEventBus()).thenReturn(mockEventBus2);
        when(mockReplicator3.getEventBus()).thenReturn(mockEventBus3);
        TestPolicy replicationPolicy = new TestPolicy();
        replicationPolicy.addReplicators(mockReplicator1, mockReplicator2, mockReplicator3);
        verify(mockReplicator1, never()).start();
        verify(mockReplicator2, never()).start();
        verify(mockReplicator3, never()).start();
        replicationPolicy.start();
        mockEventBus1.post(new ReplicationErrored(mockReplicator1, null));
        mockEventBus2.post(new ReplicationErrored(mockReplicator2, null));
        replicationPolicy.start();
        verify(mockReplicator1, times(2)).start();
        verify(mockReplicator2, times(2)).start();
        verify(mockReplicator3, times(1)).start();
    }

    /** A trivial policy where when you call {@link TestPolicy#start()} it calls
     * {@link ReplicationPolicyManager#startReplications()} and when you call {@link TestPolicy#stop()}
     * it calls {@link ReplicationPolicyManager#stopReplications()}.
     */
    class TestPolicy extends ReplicationPolicyManager {
        public TestPolicy() {
            super();
        }

        public void start() {
            startReplications();
        }

        public void stop() {
            stopReplications();
        }
    }

}
