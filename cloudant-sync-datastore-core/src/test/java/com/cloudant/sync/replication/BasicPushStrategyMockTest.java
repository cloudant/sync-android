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
import com.cloudant.mazha.CouchClient;
import com.google.common.eventbus.Subscribe;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentMatcher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

@Category(RequireRunningCouchDB.class)
public class BasicPushStrategyMockTest extends ReplicationTestBase {

    @Test
    public void push_dbNotExist_errorCallback() throws
            Exception {
        // Prepare
        StrategyListener mockListener = mock(StrategyListener.class);
        CouchDB mockRemoteDb = mock(CouchDB.class);
        BasicPushStrategy pushStrategy = new BasicPushStrategy(this.createPushReplication());
        pushStrategy.targetDb = mockRemoteDb;
        pushStrategy.eventBus.register(mockListener);
        when(mockRemoteDb.exists()).thenReturn(false);

        // Exec
        pushStrategy.run();

        // Verify
        // Make sure the checkpoint never get updated
        verify(mockRemoteDb, never()).putCheckpoint(anyString(), anyString());
        verify(mockListener).error(any(ReplicationStrategyErrored.class));
        verify(mockListener, never()).complete(any(ReplicationStrategyCompleted.class));

        Assert.assertEquals(0, pushStrategy.getDocumentCounter());
        Assert.assertEquals(0, pushStrategy.getBatchCounter());
    }

    @Test
    public void push_unknownRemoteDBError_pushAbortedAndErrorShouldBeCalled() throws
            Exception {
        // Prepare
        StrategyListener mockListener = mock(StrategyListener.class);
        CouchDB mockRemoteDb = mock(CouchDB.class);
        BasicPushStrategy pushStrategy = new BasicPushStrategy(this.createPushReplication());
        pushStrategy.targetDb = mockRemoteDb;
        pushStrategy.eventBus.register(mockListener);

        BarUtils.createBar(datastore, "Tom", 31);

        doThrow(new RuntimeException("Mocked error.")).when(mockRemoteDb).revsDiff(any(Map.class));
        when(mockRemoteDb.exists()).thenReturn(true);

        // Exec
        pushStrategy.run();

        // Verify
        // Make sure the checkpoint never get updated
        verify(mockRemoteDb, never()).putCheckpoint(anyString(), anyString());
        verify(mockListener).error(any(ReplicationStrategyErrored.class));
        verify(mockListener, never()).complete(any(ReplicationStrategyCompleted.class));

        Assert.assertEquals(0, pushStrategy.getDocumentCounter());
        Assert.assertEquals(1, pushStrategy.getBatchCounter());
    }

    @Test
    public void push_noMissingRevisions_noDataShouldBePushed() throws Exception {
        //Prepare
        StrategyListener mockListener = mock(StrategyListener.class);
        CouchDB mockRemoteDb = mock(CouchDB.class);
        BasicPushStrategy pushStrategy = new BasicPushStrategy(this.createPushReplication());
        pushStrategy.targetDb = mockRemoteDb;
        pushStrategy.eventBus.register(mockListener);
        
        Bar bar = BarUtils.createBar(datastore, "Tom", 31);

        when(mockRemoteDb.getCheckpoint(anyString())).thenReturn("0", "1");
        when(mockRemoteDb.exists()).thenReturn(true);
        when(mockRemoteDb.revsDiff(any(Map.class))).thenReturn(new HashMap<String, CouchClient.MissingRevisions>());

        // Exec
        pushStrategy.run();

        // Verify
        verify(mockRemoteDb).bulkCreateSerializedDocs(argThat(new ArgumentMatcher<List>() {
            @Override
            public boolean matches(Object argument) {
                Assert.assertTrue(argument instanceof List);
                Assert.assertEquals(0, ((List) argument).size());
                return true;
            }
        }));
        verify(mockListener, never()).error(any(ReplicationStrategyErrored.class));
        verify(mockListener).complete(any(ReplicationStrategyCompleted.class));

        Assert.assertEquals(0, pushStrategy.getDocumentCounter());
        Assert.assertEquals(2, pushStrategy.getBatchCounter());
    }


    @Test
    public void push_coordinatorThreadInterrupted_pushAbortedCorrectly() throws
            Exception {
        // Prepare
        StrategyListener mockListener = mock(StrategyListener.class);
        CouchDB mockRemoteDb = mock(CouchDB.class);

        final BasicPushStrategy pushStrategy = new BasicPushStrategy(this.createPushReplication());
        pushStrategy.eventBus.register(mockListener);
        when(mockRemoteDb.exists()).thenReturn(true);

        BarUtils.createBar(datastore, "Tom", 31);

        pushStrategy.setCancel();
        pushStrategy.run();

        // Verify
        // Make sure the checkpoint never get updated
        verify(mockRemoteDb, never()).putCheckpoint(anyString(), anyString());
        verify(mockListener).complete(any(ReplicationStrategyCompleted.class));
        verify(mockListener, never()).error(any(ReplicationStrategyErrored.class));

        Assert.assertEquals(0, pushStrategy.getDocumentCounter());
        Assert.assertEquals(0, pushStrategy.getBatchCounter());
    }
    
    public class StrategyListener {

        @Subscribe
        public void complete(ReplicationStrategyCompleted rc) {
        }

        @Subscribe
        public void error(ReplicationStrategyErrored re) {
        }
    }


}
