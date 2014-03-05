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

package com.cloudant.sync.replication;

import com.cloudant.common.RequireRunningCouchDB;
import com.google.common.eventbus.Subscribe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

@Category(RequireRunningCouchDB.class)
public class BasicPullStrategyMockTest extends ReplicationTestBase {

    ExecutorService service = null;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        service = new ThreadPoolExecutor(4, 4, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
    }

    @After
    public void tearDown() throws Exception {
        if(service != null) {
            service.shutdown();
        }
        super.tearDown();
    }

    @Test
    public void call_remoteDbNotExists_errorCallback() throws
            Exception {
        // Prepare
        StrategyListener mockListener = mock(StrategyListener.class);
        CouchDB mockRemoteDb = mock(CouchDB.class);

        PullReplication pullReplication = createPullReplication();

        BasicPullStrategy pullStrategy = new BasicPullStrategy(pullReplication);
        pullStrategy.sourceDb = mockRemoteDb;
        pullStrategy.getEventBus().register(mockListener);

        when(mockRemoteDb.exists()).thenReturn(false);

        // Exec
        pullStrategy.run();

        // Verify
        // Make sure the checkpoint never get updated
        verify(mockRemoteDb, never()).putCheckpoint(anyString(), anyString());
        verify(mockListener).error(any(ReplicationStrategyErrored.class));
        verify(mockListener, never()).complete(any(ReplicationStrategyCompleted.class));

        Assert.assertEquals(0, pullStrategy.getDocumentCounter());
        Assert.assertEquals(0, pullStrategy.getBatchCounter());
    }

    @Test
    public void call_unknownRemoteDBError_pullAbortedAndErrorShouldBeCalled() throws
            Exception {
        // Prepare
        StrategyListener mockListener = mock(StrategyListener.class);
        CouchDB mockRemoteDb = mock(CouchDB.class);

        BasicPullStrategy pullStrategy = new BasicPullStrategy(createPullReplication());
        pullStrategy.sourceDb = mockRemoteDb;
        pullStrategy.getEventBus().register(mockListener);

        doThrow(new RuntimeException("Mocked error.")).when(mockRemoteDb).changes(anyString(),
                anyInt());
        when(mockRemoteDb.exists()).thenReturn(true);

        // Exec
        pullStrategy.run();

        // Verify
        // Make sure the checkpoint never get updated
        verify(mockRemoteDb, never()).putCheckpoint(anyString(), anyString());
        verify(mockListener).error(any(ReplicationStrategyErrored.class));
        verify(mockListener, never()).complete(any(ReplicationStrategyCompleted.class));

        Assert.assertEquals(0, pullStrategy.getDocumentCounter());
        Assert.assertEquals(1, pullStrategy.getBatchCounter());
    }

    @Test
    public void call_executorShutDown_replicatorStopCorrectly() throws Exception {

        // Prepare
        StrategyListener mockListener = mock(StrategyListener.class);
        CouchDB mockRemoteDb = mock(CouchDB.class);

        PullReplication pullReplication = this.createPullReplication();
        BasicPullStrategy pullStrategy = new BasicPullStrategy(pullReplication, this.service, null);
        pullStrategy.sourceDb = mockRemoteDb;
        pullStrategy.getEventBus().register(mockListener);
        service.shutdownNow();
        when(mockRemoteDb.exists()).thenReturn(true);

        // Exec
        pullStrategy.run();

        // Verify
        verify(mockRemoteDb, never()).getCheckpoint(anyString());
        verify(mockListener).error(any(ReplicationStrategyErrored.class));
        verify(mockListener, never()).complete(any(ReplicationStrategyCompleted.class));
    }

    @Test
    public void call_coordinationThreadInterrupted_replicatorStopCorrectly() throws Exception {

        // Prepare
        StrategyListener mockListener = mock(StrategyListener.class);
        CouchDB mockRemoteDb = mock(CouchDB.class);

        final BasicPullStrategy pullStrategy = new BasicPullStrategy(createPullReplication());
        pullStrategy.sourceDb = mockRemoteDb;
        pullStrategy.getEventBus().register(mockListener);
        
        when(mockRemoteDb.exists()).thenReturn(true);
        pullStrategy.setCancel();
        pullStrategy.run();

        // Verify
        verify(mockRemoteDb, never()).getCheckpoint(anyString());
        verify(mockListener).complete(any(ReplicationStrategyCompleted.class));
        verify(mockListener, never()).error(any(ReplicationStrategyErrored.class));
    }
    
    private class StrategyListener {

        @Subscribe
        public void complete(ReplicationStrategyCompleted rc) {
        }

        @Subscribe
        public void error(ReplicationStrategyErrored re) {
        }
    }

}
