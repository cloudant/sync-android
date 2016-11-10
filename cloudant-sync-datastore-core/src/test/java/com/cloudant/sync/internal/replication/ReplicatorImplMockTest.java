/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.cloudant.sync.internal.datastore.DatabaseImpl;
import com.cloudant.sync.event.EventBus;
import com.cloudant.sync.event.Subscribe;

import com.cloudant.sync.event.notifications.ReplicationCompleted;
import com.cloudant.sync.event.notifications.ReplicationErrored;
import com.cloudant.sync.replication.Replicator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.net.URI;
import java.net.URISyntaxException;

public class ReplicatorImplMockTest {

    URI uri;
    DatabaseImpl mockDatastore;
    ReplicationStrategy mockStrategy;
    Listener mockListener;
    TestReplicatorImpl replicator;

    @Before
    public void setUp() throws Exception {
        uri = new URI("http://127.0.0.1:5984/db1");
        mockDatastore = mock(DatabaseImpl.class);
        mockStrategy = mock(ReplicationStrategy.class);
        when(mockStrategy.getEventBus()).thenReturn(new EventBus());
        mockListener = mock(Listener.class);
        replicator = new TestReplicatorImpl(
                mockStrategy);

        // mockStrategy always needs to be ready to start
        when(mockStrategy.isReplicationTerminated()).thenReturn(true);
    }

    @Test
    public void constructor() throws Exception {
        Assert.assertEquals(Replicator.State.PENDING, replicator.getState());
        Assert.assertNull(replicator.strategyThread());
    }

    @Test
    public void start_woListener_nothing() {
        // don't subscribe to the eventbus
        replicator.start();
    }

    @Test
    public void complete_woListener_nothing() {
        // don't subscribe to the eventbus
        replicator.start();
        replicator.complete(new ReplicationStrategyCompleted(mockStrategy));

        verify(mockListener, never()).complete(any(ReplicationCompleted.class));
        verify(mockListener, never()).error(any(ReplicationErrored.class));
    }

    @Test
    public void error_woListener_nothing() {
        // don't subscribe to the eventbus
        replicator.start();
        replicator.error(new ReplicationStrategyErrored(mockStrategy, new RuntimeException("Mocked error")));

        verify(mockListener, never()).complete(any(ReplicationCompleted.class));
        verify(mockListener, never()).error(any(ReplicationErrored.class));
    }

    @Test
    public void start_mockStrategyShouldBeCalled() throws Exception{
        startAndVerify();
    }

    private void startAndVerify() throws Exception {
        Assert.assertEquals(Replicator.State.PENDING, replicator.getState());
        Assert.assertNull(replicator.strategyThread());

        replicator.getEventBus().register(mockListener);
        replicator.start();

        Assert.assertNotNull(replicator.strategyThread());
        Assert.assertEquals(Replicator.State.STARTED, replicator.getState());

        // Make sure the strategy returned before move on, other wise
        // the mock strategy might not be called when you try to verify
        replicator.await();
        Assert.assertFalse(replicator.strategyThread().isAlive());

        verify(mockStrategy).run();

        // No interaction to listener yet
        verifyZeroInteractions(mockListener);
    }

    @Test
    public void stopBeforeStart_stopped() throws Exception {
        replicator.stop();
        Assert.assertEquals(Replicator.State.STOPPED, replicator.getState());
        verify(mockStrategy, never()).run();
    }

    @Test
    public void startAgainBeforeComplete_nothing() throws Exception {
        startAndVerify();
        replicator.start();

        verify(mockStrategy).run();
    }

    @Test
    public void startAndThenComplete_completeState() throws Exception {
        startAndVerify();

        ReplicationStrategyCompleted rsc = new ReplicationStrategyCompleted(mockStrategy);
        ReplicationCompleted rc = new ReplicationCompleted(replicator, 0, 0);
        
        replicator.complete(rsc);

        verify(mockListener).complete(rc);
        verify(mockListener, never()).error(any(ReplicationErrored.class));

        Assert.assertEquals(Replicator.State.COMPLETE, replicator.getState());
        Assert.assertFalse(replicator.strategyThread().isAlive());
    }

    @Test
    public void complete_exceptionInCompleteCallback_exceptionShouldCorruptAnything() throws
            Exception {
        startAndVerify();

        ReplicationStrategyCompleted rsc = new ReplicationStrategyCompleted(mockStrategy);
        ReplicationCompleted rc = new ReplicationCompleted(replicator, 0, 0);
        
        doThrow(new RuntimeException("Mocked error")).when(mockListener).complete(rc);
        
        replicator.complete(rsc);

        verify(mockListener).complete(rc);
        verify(mockListener, never()).error(any(ReplicationErrored.class));

        Assert.assertEquals(Replicator.State.COMPLETE, replicator.getState());
        Assert.assertFalse(replicator.strategyThread().isAlive());
    }

    @Test
    public void startAndThenError_errorState() throws Exception {
        startAndVerify();

        Throwable err = new RuntimeException("Mocked error");
        ReplicationStrategyErrored rse = new ReplicationStrategyErrored(mockStrategy, err);
        ReplicationErrored re = new ReplicationErrored(replicator, err);
        
        replicator.error(rse);

        verify(mockListener).error(re);
        verify(mockListener, never()).complete(any(ReplicationCompleted.class));

        Assert.assertEquals(Replicator.State.ERROR, replicator.getState());
        Assert.assertFalse(replicator.strategyThread().isAlive());
    }

    @Test
    public void error_exceptionInErrorCallback_nothingShouldBeCorrupted() throws Exception {
        startAndVerify();

        Throwable err = new RuntimeException("Mocked error");
        ReplicationStrategyErrored rse = new ReplicationStrategyErrored(mockStrategy, err);
        ReplicationErrored re = new ReplicationErrored(replicator, err);

        doThrow(new RuntimeException("Another mocked error"))
                .when(mockListener).error(re);

        replicator.error(rse);

        verify(mockListener).error(re);
        verify(mockListener, never()).complete(any(ReplicationCompleted.class));

        Assert.assertEquals(Replicator.State.ERROR, replicator.getState());
        Assert.assertFalse(replicator.strategyThread().isAlive());
    }

    @Test
    public void startAndThenStopComplete_stoppedState() throws Exception {
        startAndVerify();

        replicator.stop();
        Assert.assertEquals(Replicator.State.STOPPING, replicator.getState());
        Assert.assertFalse(replicator.strategyThread().isAlive());

        ReplicationStrategyCompleted rsc = new ReplicationStrategyCompleted(mockStrategy);
        ReplicationCompleted rc = new ReplicationCompleted(replicator, 0, 0);
        
        replicator.complete(rsc);
        verify(mockListener).complete(rc);
        verify(mockListener, never()).error(any(ReplicationErrored.class));

        Assert.assertEquals(Replicator.State.STOPPED, replicator.getState());
    }

    @Test
    public void startAndThenStopError_errorState() throws Exception {
        startAndVerify();

        replicator.stop();
        Assert.assertEquals(Replicator.State.STOPPING, replicator.getState());
        Assert.assertFalse(replicator.strategyThread().isAlive());

        Throwable err = new RuntimeException("Mocked error");
        ReplicationStrategyErrored rse = new ReplicationStrategyErrored(mockStrategy, err);
        ReplicationErrored re = new ReplicationErrored(replicator, err);
        replicator.error(rse);
        
        verify(mockListener).error(re);
        verify(mockListener, never()).complete(any(ReplicationCompleted.class));

        Assert.assertEquals(Replicator.State.ERROR, replicator.getState());
    }

    @Test(expected = IllegalStateException.class)
    public void start_startWhenStopping_exception() throws Exception {
        startAndVerify();

        replicator.stop();
        Assert.assertEquals(Replicator.State.STOPPING, replicator.getState());
        replicator.start(); // can not restart until full stopped
    }

    @Test
    public void start_stoppedThenRestarted() throws Exception {
        startAndVerify();

        replicator.stop();
        Assert.assertEquals(Replicator.State.STOPPING, replicator.getState());

        ReplicationStrategyCompleted rsc = new ReplicationStrategyCompleted(mockStrategy);
        ReplicationCompleted rc = new ReplicationCompleted(replicator, 0, 0);
        replicator.complete(rsc);
        Assert.assertEquals(Replicator.State.STOPPED, replicator.getState());
        verify(mockListener).complete(rc);
        verify(mockListener, never()).error(any(ReplicationErrored.class));

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(1000); //sleep for a second, will give enough time to check state
                return null;
            }
        }).when(mockStrategy).run();

        replicator.start();
        Assert.assertEquals(Replicator.State.STARTED, replicator.getState());

        Assert.assertTrue(replicator.strategyThread().isAlive());
        replicator.await();
        Assert.assertFalse(replicator.strategyThread().isAlive());
        verify(mockStrategy, times(2)).run();
    }

    @Test
    public void start_errorThenRestarted() throws Exception {
        startAndVerify();

        replicator.stop();
        Assert.assertEquals(Replicator.State.STOPPING, replicator.getState());

        Throwable err = new RuntimeException("Mocked error");
        ReplicationStrategyErrored rse = new ReplicationStrategyErrored(mockStrategy, err);
        ReplicationErrored re = new ReplicationErrored(replicator, err);
        replicator.error(rse);
        Assert.assertEquals(Replicator.State.ERROR, replicator.getState());
        verify(mockListener).error(re);
        verify(mockListener, never()).complete(any(ReplicationCompleted.class));

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(1000); //sleep for a second, will give enough time to check state
                return null;
            }
        }).when(mockStrategy).run();
        replicator.start();
        Assert.assertEquals(Replicator.State.STARTED, replicator.getState());
        Assert.assertTrue(replicator.strategyThread().isAlive());
        replicator.await();
        Assert.assertFalse(replicator.strategyThread().isAlive());
        verify(mockStrategy, times(2)).run();
    }

    public class TestReplicatorImpl extends ReplicatorImpl {

        public TestReplicatorImpl(ReplicationStrategy strategy) {
            super(strategy);
        }

        Thread strategyThread() {
            return this.strategyThread;
        }

        void await() throws InterruptedException {
            this.strategyThread.join();
        }
    }

    /**
     * This ReplicationListener allows us to make sure that a replicator's
     * start() method can be called from the complete/error handler.
     */
    public class Listener {

        boolean errorThrown = false;
        boolean run = false;

        @Subscribe
        public void complete(ReplicationCompleted rc) {
            try {
                rc.replicator.start();
            } catch (Exception ex) {
                errorThrown = true;
            }
            run = true;
        }

        @Subscribe
        public void error(ReplicationErrored re) {
            try {
                re.replicator.start();
            } catch (Exception ex) {
                errorThrown = true;
            }
            run = true;
        }
    }

    @Test
    public void testListenerCompleteCanRestartReplication()
            throws URISyntaxException, InterruptedException {
        Listener listener = new Listener();

        ReplicationStrategy mockStrategy = mock(ReplicationStrategy.class);
        when(mockStrategy.isReplicationTerminated()).thenReturn(true);
        when(mockStrategy.getEventBus()).thenReturn(new EventBus());

        TestReplicatorImpl replicator = new TestReplicatorImpl(mockStrategy);
        replicator.getEventBus().register(listener);
        replicator.start();

        // Waits for callbacks to be run
        ReplicationStrategyCompleted rsc = new ReplicationStrategyCompleted(mockStrategy);
        replicator.complete(rsc);

        Assert.assertTrue(listener.run);
        Assert.assertFalse(listener.errorThrown);

        // Don't leave threads about the place
        replicator.complete(rsc);
    }

    @Test
    public void testListenerErrorCanRestartReplication()
            throws URISyntaxException, InterruptedException {
        Listener listener = new Listener();

        ReplicationStrategy mockStrategy = mock(ReplicationStrategy.class);
        when(mockStrategy.isReplicationTerminated()).thenReturn(true);
        when(mockStrategy.getEventBus()).thenReturn(new EventBus());

        TestReplicatorImpl replicator = new TestReplicatorImpl(mockStrategy);
        replicator.getEventBus().register(listener);
        replicator.start();

        // Waits for callbacks to be run
        replicator.error(new ReplicationStrategyErrored(mockStrategy, new RuntimeException("Mocked error")));

        Assert.assertTrue(listener.run);
        Assert.assertFalse(listener.errorThrown);

        // Don't leave threads about the place
        replicator.complete(new ReplicationStrategyCompleted(mockStrategy));
    }


}
