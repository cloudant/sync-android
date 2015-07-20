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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.mazha.ChangesResult;
import com.cloudant.mazha.DocumentRevs;
import com.cloudant.mazha.OkOpenRevision;
import com.cloudant.mazha.OpenRevision;
import com.cloudant.mazha.json.JSONHelper;
import com.cloudant.sync.util.TestUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.eventbus.Subscribe;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
        BasicPullStrategy pullStrategy = new BasicPullStrategy(pullReplication,
                this.service,
                BasicPullStrategy.DEFAULT_CHANGES_LIMIT_PER_BATCH,
                BasicPullStrategy.DEFAULT_MAX_BATCH_COUNTER_PER_RUN,
                BasicPullStrategy.DEFAULT_INSERT_BATCH_SIZE,
                BasicPullStrategy.DEFAULT_PULL_ATTACHMENTS_INLINE);
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

    @Test
    public void testReplicationDocWithEmptyId() throws Exception {
        CouchDB mockRemoteDb = mock(CouchDB.class);
        when(mockRemoteDb.changes(null, null, 1000)).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                JSONHelper jsonHelper = new JSONHelper();
                FileReader fr = new FileReader(TestUtils.loadFixture("fixture/testReplicationDocWithEmptyId_changes.json"));
                return jsonHelper.fromJson(fr, ChangesResult.class);
            }
        });
        when(mockRemoteDb.exists()).thenReturn(true);
        Collection<String> revs = new ArrayList<String>();
        revs.add("1-bd42b942b8b672f0289cf3cd1f67044c");
        when(mockRemoteDb.getRevisions("", revs, new HashSet<String>(), false)).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {

                return loadOpenRevsResponseFromFixture("testReplicationDocWithEmptyId_open_revs_1.json");

            }
        });
        revs = new ArrayList<String>();
        revs.add("1-13d33701a0954729ad029adf8fdc5a04");
        when(mockRemoteDb.getRevisions("4d3b3f01362649d79b31d9092799a7e0", revs, new HashSet<String>(),false)).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {

                return loadOpenRevsResponseFromFixture("fixture/testReplicationDocWithEmptyId_open_revs_2.json");

            }
        });

        StrategyListener mockListener = mock(StrategyListener.class);
        final BasicPullStrategy pullStrategy = new BasicPullStrategy(createPullReplication());
        pullStrategy.sourceDb = mockRemoteDb;
        pullStrategy.getEventBus().register(mockListener);
        pullStrategy.run();

        //should have 1 document
        Assert.assertEquals(this.datastore.getDocumentCount(), 1);
        //make sure the correct events were fired
        verify(mockListener).complete(any(ReplicationStrategyCompleted.class));
        verify(mockListener,never()).error(any(ReplicationStrategyErrored.class));
    }
    
    public class StrategyListener {

        @Subscribe
        public void complete(ReplicationStrategyCompleted rc) {
        }

        @Subscribe
        public void error(ReplicationStrategyErrored re) {
        }
    }

    private List<DocumentRevs> loadOpenRevsResponseFromFixture(String fixturePath) throws Exception{
        JSONHelper helper = new JSONHelper();
        FileReader fileReader = new FileReader(TestUtils.loadFixture(fixturePath));
        List<OpenRevision> openRevs = helper.fromJson(fileReader,
                new TypeReference<List<OpenRevision>>() {
                });

        List<DocumentRevs> documentRevs = new ArrayList<DocumentRevs>();

        for (OpenRevision openRev : openRevs) {
            if (openRev instanceof OkOpenRevision) {
                documentRevs.add(((OkOpenRevision) openRev).getDocumentRevs());
            }
        }

        return documentRevs;
    }

}
