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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URI;

@Category(RequireRunningCouchDB.class)
public class PullReplicatorTest extends ReplicationTestBase {

    URI source;
    BasicReplicator replicator;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        source = getCouchConfig(getDbName()).getRootUri();

        PullReplication pull = createPullReplication();
        replicator = (BasicReplicator)ReplicatorFactory.oneway(pull);
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

    @Test
    public void start_StartedThenComplete() throws InterruptedException {
        TestReplicationListener listener = new TestReplicationListener();
        Assert.assertEquals(Replicator.State.PENDING, replicator.getState());
        replicator.getEventBus().register(listener);
        replicator.start();
        Assert.assertEquals(Replicator.State.STARTED, replicator.getState());

        while(replicator.getState() != Replicator.State.COMPLETE && replicator.getState() != Replicator.State.ERROR) {
            Thread.sleep(1000);
        }

        Assert.assertEquals(Replicator.State.COMPLETE, replicator.getState());
        Assert.assertEquals(2, datastore.getDocumentCount());

        Assert.assertTrue(listener.finishCalled);
        Assert.assertFalse(listener.errorCalled);
    }

    @Test
    public void testRequestFilters() throws Exception {

        FilterCallCounter filterCallCounter = new FilterCallCounter();
        PullReplication pullReplication = createPullReplication();
        pullReplication.requestFilters.add(filterCallCounter);
        runReplicationUntilComplete(pullReplication);
        Assert.assertTrue(filterCallCounter.filterRequestTimesCalled >= 1);

    }

    @Test
    public void testResponseFilters() throws Exception {

        FilterCallCounter filterCallCounter = new FilterCallCounter();
        PullReplication pullReplication = createPullReplication();
        pullReplication.responseFilters.add(filterCallCounter);
        runReplicationUntilComplete(pullReplication);
        Assert.assertTrue(filterCallCounter.filterResponseTimesCalled >= 1);
    }

}
