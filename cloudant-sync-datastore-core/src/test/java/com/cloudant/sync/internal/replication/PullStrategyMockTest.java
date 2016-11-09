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
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.sync.internal.mazha.ChangesResult;
import com.cloudant.sync.internal.mazha.DocumentRevs;
import com.cloudant.sync.internal.mazha.OkOpenRevision;
import com.cloudant.sync.internal.mazha.OpenRevision;
import com.cloudant.sync.internal.mazha.json.JSONHelper;
import com.cloudant.sync.internal.datastore.DocumentRevsList;
import com.cloudant.sync.event.Subscribe;
import com.cloudant.sync.util.TestUtils;
import com.fasterxml.jackson.core.type.TypeReference;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

@Category(RequireRunningCouchDB.class)
public class PullStrategyMockTest extends ReplicationTestBase {

    // NB these tests call super.getPullStrategy() and then just overwrite the sourceDb with a mock
    // database. Possibly a more pure approach would be to mock the entire strategy, but we still
    // want the eventBus to operate and fire events.

    @Test
    public void call_remoteDbNotExists_errorCallback() throws
            Exception {
        // Prepare
        StrategyListener mockListener = mock(StrategyListener.class);
        CouchDB mockRemoteDb = mock(CouchDB.class);

        PullStrategy pullStrategy = super.getPullStrategy();

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

        PullStrategy pullStrategy = super.getPullStrategy();
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

        PullStrategy pullStrategy = super.getPullStrategy();
        pullStrategy.sourceDb = mockRemoteDb;
        pullStrategy.getEventBus().register(mockListener);
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

        PullStrategy pullStrategy = super.getPullStrategy();
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
                FileReader fr = new FileReader(TestUtils.loadFixture
                        ("fixture/testReplicationDocWithEmptyId_changes.json"));
                return jsonHelper.fromJson(fr, ChangesResult.class);
            }
        });
        when(mockRemoteDb.exists()).thenReturn(true);
        Collection<String> revs = new ArrayList<String>();
        revs.add("1-bd42b942b8b672f0289cf3cd1f67044c");

        // TODO we could assert on these empty string ones not being called
        // bulkGetRevisions flavour of mock
        when(mockRemoteDb.bulkGetRevisions(Collections.singletonList(new BulkGetRequest("", new
                ArrayList<String>(revs), new ArrayList<String>())), false)).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return loadBulkRevsResponseFromFixture("fixture/testReplicationDocWithEmptyId_open_revs_1" +
                        ".json");

            }
        });

        // 'normal' getRevisions flavour of mock
        when(mockRemoteDb.getRevisions("", revs, new HashSet<String>(), false)).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {

                return loadOpenRevsResponseFromFixture("fixture/testReplicationDocWithEmptyId_open_revs_1" +
                        ".json");

            }
        });
        revs = new ArrayList<String>();
        revs.add("1-13d33701a0954729ad029adf8fdc5a04");

        // bulkGetRevisions flavour of mock
        when(mockRemoteDb.bulkGetRevisions(Collections.singletonList(new BulkGetRequest("4d3b3f01362649d79b31d9092799a7e0", new
                ArrayList<String>(revs), new ArrayList<String>())), false)).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return loadBulkRevsResponseFromFixture("fixture/testReplicationDocWithEmptyId_open_revs_2" +
                        ".json");

            }
        });

        // 'normal' getRevisions flavour of mock
        when(mockRemoteDb.getRevisions("4d3b3f01362649d79b31d9092799a7e0", revs, new ArrayList
                        <String>(),
                false)).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {

                return loadOpenRevsResponseFromFixture
                        ("fixture/testReplicationDocWithEmptyId_open_revs_2.json");

            }
        });

        StrategyListener mockListener = mock(StrategyListener.class);
        PullStrategy pullStrategy = super.getPullStrategy();
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

    private Iterable<DocumentRevsList> loadBulkRevsResponseFromFixture(String fixturePath) throws Exception {
        // adapt response from loadOpenRevsResponseFromFixture to look like it came from a bulk response:
        // one revslist presented inside a list of length 1
        List<DocumentRevs> revs = loadOpenRevsResponseFromFixture(fixturePath);
        DocumentRevsList revsList = new DocumentRevsList(revs);
        return Collections.singleton(revsList);
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
