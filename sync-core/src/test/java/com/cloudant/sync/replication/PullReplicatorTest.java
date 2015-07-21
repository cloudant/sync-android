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
import com.cloudant.mazha.CouchException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
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

        //to test the filters we make an invalid request

        TestReplicationListener listener = new TestReplicationListener();
        PullReplication pullReplication = createPullReplication();
        pullReplication.addRequestFilters(new InvalidJSONFilter());
        Replicator replicator = ReplicatorFactory.oneway(pullReplication);
        replicator.getEventBus().register(listener);
        replicator.start();

        while(replicator.getState() != Replicator.State.COMPLETE && replicator.getState() != Replicator.State.ERROR) {
            Thread.sleep(1000);
        }

        Assert.assertEquals(Replicator.State.ERROR, replicator.getState());
        Assert.assertFalse(listener.finishCalled);
        Assert.assertTrue(listener.errorCalled);
        Assert.assertTrue(listener.exception instanceof CouchException);
        CouchException couchException = (CouchException) listener.exception;
        Assert.assertTrue(couchException.getStatusCode() == 400);
        Assert.assertEquals(couchException.getReason(), "invalid_json");

    }

    @Test
    public void testResponseFilters() throws Exception {

        TestReplicationListener listener = new TestReplicationListener();
        PullReplication pullReplication = createPullReplication();
        pullReplication.addResponseFilters(new ResponseStreamReaderFilter());
        Replicator replicator = ReplicatorFactory.oneway(pullReplication);
        replicator.getEventBus().register(listener);
        replicator.start();

        while(replicator.getState() != Replicator.State.COMPLETE && replicator.getState() != Replicator.State.ERROR) {
            Thread.sleep(1000);
        }

        Assert.assertEquals(Replicator.State.ERROR, replicator.getState());
        Assert.assertFalse(listener.finishCalled);
        Assert.assertTrue(listener.errorCalled);
        Assert.assertTrue(listener.exception instanceof RuntimeException);
        RuntimeException exception = (RuntimeException)listener.exception;
        Assert.assertTrue(exception.getCause() instanceof IOException);
    }
}
