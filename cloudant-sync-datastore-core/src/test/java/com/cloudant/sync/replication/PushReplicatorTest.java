/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.replication;

import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.sync.datastore.DocumentRevision;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Category(RequireRunningCouchDB.class)
public class PushReplicatorTest extends ReplicationTestBase {

    private void prepareTwoDocumentsInLocalDB() throws Exception {
        Bar bar1 = BarUtils.createBar(datastore, "Tom", 31);
        Bar bar2 = BarUtils.createBar(datastore, "Jerry", 52);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void start_StartedThenComplete() throws Exception {
        prepareTwoDocumentsInLocalDB();

        ReplicatorImpl replicator = (ReplicatorImpl) super.getPushBuilder().build();

        TestReplicationListener listener = new TestReplicationListener();
        Assert.assertEquals(Replicator.State.PENDING, replicator.getState());
        replicator.getEventBus().register(listener);
        replicator.start();
        Assert.assertEquals(Replicator.State.STARTED, replicator.getState());

        while (replicator.getState() != Replicator.State.COMPLETE) {
            Thread.sleep(1000);
        }

        Assert.assertEquals(Replicator.State.COMPLETE, replicator.getState());
        Assert.assertEquals(2, remoteDb.changes("0", 100).size());

        listener.assertReplicationCompletedOrThrow();
    }

    @Test
    public void start_StartedThenStopped() throws Exception {

        int count = 5000;
        for (int i = 0; i < count; i++) {
            BarUtils.createBar(datastore, "docnum", i);
        }

        ReplicatorImpl replicator = (ReplicatorImpl) super.getPushBuilder().build();

        TestReplicationListener listener = new TestReplicationListener();
        Assert.assertEquals(Replicator.State.PENDING, replicator.getState());
        replicator.getEventBus().register(listener);
        replicator.start();
        Assert.assertEquals(Replicator.State.STARTED, replicator.getState());
        Thread.sleep(1000); //just to make sure a few docs are pushed
        replicator.stop();

        //force wait for the replicator to finish stopping before making tests.
        int maxTries = 1000 * 60; //60 seconds is the longest we'll wait
        int haveBeenWaiting = 0;
        while (!listener.finishCalled && !listener.errorCalled) {
            Thread.sleep(1000);
            if (haveBeenWaiting >= maxTries) {
                Assert.fail("replicator did not stop after waiting 60 seconds.");
                break;
            }
            haveBeenWaiting += 1000;
        }

        listener.assertReplicationCompletedOrThrow();

        if (count != remoteDb.changes("0", 10000).size()) {
            Assert.assertEquals(Replicator.State.STOPPED, replicator.getState());
        } else {
            Assert.assertEquals(Replicator.State.COMPLETE, replicator.getState());
            Assert.fail("replicator did not stop before all docs were pushed");
        }

    }

    @Test
    public void testRequestInterceptors() throws Exception {

        //to test the interceptors we count if the interceptor gets called
        InterceptorCallCounter interceptorCallCounter = new InterceptorCallCounter();
        Replicator pushReplication = super.getPushBuilder()
                .addRequestInterceptors(interceptorCallCounter).build();

        runReplicationUntilComplete(pushReplication);
        Assert.assertTrue(interceptorCallCounter.interceptorRequestTimesCalled >= 1);

    }

    @Test
    public void testResponseInterceptors() throws Exception {

        //to test the interceptors we count if the interceptor gets called
        InterceptorCallCounter interceptorCallCounter = new InterceptorCallCounter();
        Replicator pushReplication = super.getPushBuilder()
                .addResponseInterceptors(interceptorCallCounter).build();

        runReplicationUntilComplete(pushReplication);
        Assert.assertTrue(interceptorCallCounter.interceptorResponseTimesCalled >= 1);
    }

    @Test
    public void replicatorCanBeReused() throws Exception {
        prepareTwoDocumentsInLocalDB();
        ReplicatorBuilder replicatorBuilder = super.getPushBuilder();
        Replicator replicator = replicatorBuilder.build();
        ReplicationStrategy replicationStrategy = ((ReplicatorImpl) replicator).strategy;
        replicator.start();
        // replicate 2 docs created above
        while (replicator.getState() != Replicator.State.COMPLETE && replicator.getState() !=
                Replicator.State.ERROR) {
            Thread.sleep(50);
        }
        // check document counter has been incremented
        Assert.assertEquals(2, replicationStrategy.getDocumentCounter());
        Bar bar3 = BarUtils.createBar(datastore, "Test", 52);
        replicator.start();
        ReplicationStrategy replicationStrategy2 = ((ReplicatorImpl) replicator).strategy;
        // replicate 3rd doc
        while (replicator.getState() != Replicator.State.COMPLETE && replicator.getState() !=
                Replicator.State.ERROR) {
            Thread.sleep(50);
        }
        // check document counter has been reset since last replication and incremented
        Assert.assertEquals(1, replicationStrategy2.getDocumentCounter());
        Assert.assertEquals(3, remoteDb.couchClient.getDbInfo().getDocCount());
    }


    @Test
    public void replicatorBuilderAddsCookieInterceptorCustomPort() throws Exception {
        ReplicatorBuilder.Push p = ReplicatorBuilder.push().
                from(datastore).
                to(new URI("http://🍶:🍶@some-host:123/path%2Fsome-path-日本"));
        ReplicatorImpl r = (ReplicatorImpl) p.build();
        // check that user/pass has been removed
        Assert.assertEquals("http://some-host:123/path%2Fsome-path-日本",
                (((CouchClientWrapper) (((PushStrategy) r.strategy).targetDb)).
                        getCouchClient().
                        getRootUri()).
                        toString()
        );
        assertCookieInterceptorPresent(p, "name=%F0%9F%8D%B6&password=%F0%9F%8D%B6");
    }

    @Test
    public void replicatorBuilderAddsCookieInterceptorDefaultPort() throws Exception {
        ReplicatorBuilder.Push p = ReplicatorBuilder.push().
                from(datastore).
                to(new URI("http://🍶:🍶@some-host/path%2Fsome-path-日本"));
        ReplicatorImpl r = (ReplicatorImpl) p.build();
        // check that user/pass has been removed
        Assert.assertEquals("http://some-host:80/path%2Fsome-path-日本",
                (((CouchClientWrapper) (((PushStrategy) r.strategy).targetDb)).
                        getCouchClient().
                        getRootUri()).
                        toString()
        );
        assertCookieInterceptorPresent(p, "name=%F0%9F%8D%B6&password=%F0%9F%8D%B6");
    }

    /**
     * Asserts that the last sequence number of a replicator checkpoint matches that of the test
     * datastore.
     */
    private void assertLastSequence(Replicator r) throws Exception {
        // Need to know about the internals to get the replication ID
        ReplicatorImpl replicator = (ReplicatorImpl) r;
        Assert.assertEquals("The checkpoint should match the datstore last sequence.",
                Long.toString(datastore.getLastSequence()),
                remoteDb.getCheckpoint(replicator.strategy.getReplicationId()));
    }

    @Test
    public void testPushReplicationFilter() throws Exception {
        prepareTwoDocumentsInLocalDB();
        ReplicatorBuilder.Push push = this.getPushBuilder();
        push.filter(new PushFilter() {
            @Override
            public boolean shouldReplicateDocument(DocumentRevision revision) {
                return revision.getBody().asMap().get("name").equals("Tom");
            }
        });
        Replicator replicator = push.build();
        replicator.start();
        while (replicator.getState() != Replicator.State.COMPLETE && replicator.getState() !=
                Replicator.State.ERROR) {
            Thread.sleep(50);
        }

        Assert.assertEquals(Replicator.State.COMPLETE, replicator.getState());

        // Check that the remote only contains the single doc we need.
        Assert.assertEquals(1, couchClient.getDbInfo().getDocCount());

        assertLastSequence(replicator);
    }

    @Test
    public void testPushReplicationFilterPushesZeroDocs() throws Exception {
        prepareTwoDocumentsInLocalDB();
        ReplicatorBuilder.Push push = this.getPushBuilder();
        push.filter(new PushFilter() {
            @Override
            public boolean shouldReplicateDocument(DocumentRevision revision) {
                return false;
            }
        });
        Replicator replicator = push.build();
        replicator.start();
        while (replicator.getState() != Replicator.State.COMPLETE && replicator.getState() !=
                Replicator.State.ERROR) {
            Thread.sleep(50);
        }

        Assert.assertEquals(Replicator.State.COMPLETE, replicator.getState());

        // Check that the remote contains no docs.
        Assert.assertEquals(0, couchClient.getDbInfo().getDocCount());

        assertLastSequence(replicator);
    }

    /**
     * This test checks that the replication continues from the correct place.
     * It does this by first replicating with a filter that excludes all docs. This means that the 2
     * local changes will get processed, but no documents will be added to the remote.
     * Then it does another replication <b>without a filter</b>. We have made no more local changes
     * so if the replicator starts from the correct checkpoint then there will be no changes and no
     * more documents will be copied.
     * The test will fail if the replicator starts from the wrong checkpoint because the 2 original
     * changes would be visible after the second start() call.
     *
     * @throws Exception
     */
    @Test
    public void testPushReplicationFilterContinuesFromCorrectPlace() throws Exception {
        prepareTwoDocumentsInLocalDB();
        ReplicatorBuilder.Push push = this.getPushBuilder();
        push.filter(new PushFilter() {
            @Override
            public boolean shouldReplicateDocument(DocumentRevision revision) {
                return false;
            }
        });
        Replicator replicator = push.build();
        replicator.start();
        while (replicator.getState() != Replicator.State.COMPLETE && replicator.getState() !=
                Replicator.State.ERROR) {
            Thread.sleep(50);
        }

        Assert.assertEquals(Replicator.State.COMPLETE, replicator.getState());

        // Check that the remote contains no docs.
        Assert.assertEquals(0, couchClient.getDbInfo().getDocCount());
        replicator = this.getPushBuilder().build();
        replicator.start();

        while (replicator.getState() != Replicator.State.COMPLETE && replicator.getState() !=
                Replicator.State.ERROR) {
            Thread.sleep(50);
        }

        Assert.assertEquals(Replicator.State.COMPLETE, replicator.getState());

        // Check that the remote still contains no docs.
        Assert.assertEquals(0, couchClient.getDbInfo().getDocCount());

        assertLastSequence(replicator);
    }

    /**
     * Tests that the replicator completes when there are no more changes to process even when all
     * changes are being filtered out. Uses a batch size of 1 to ensure that multiple batches get
     * processed.
     *
     * @throws Exception
     */
    @Test
    public void testPushReplicationComplete() throws Exception {
        prepareTwoDocumentsInLocalDB();
        Replicator replicator = getPushBuilder().filter(new PushFilter() {
            @Override
            public boolean shouldReplicateDocument(DocumentRevision revision) {
                return false;
            }
        }).changeLimitPerBatch(1).batchLimitPerRun(5).build();

        // Register a listener for the completion event
        TestReplicationListener listener = new TestReplicationListener();
        replicator.getEventBus().register(listener);

        replicator.start();
        long timeout = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
        while (!listener.finishCalled && System.currentTimeMillis() <= timeout) {
            TimeUnit.MILLISECONDS.sleep(50);
        }

        Assert.assertTrue("The replication should complete not timeout.", listener.finishCalled);

        Assert.assertEquals("The replicator should be in COMPLETE state.", Replicator.State
                .COMPLETE, replicator.getState());

        // Expect 3 batches, 2 with 1 change each, and a third with no changes.
        Assert.assertEquals("There should be three batches processed.", 3, listener
                .batchesReplicated);

        assertLastSequence(replicator);
    }

    @Test
    public void replicatorBuilderAddsCookieInterceptorSpecialCreds() throws Exception {
        String encodedUsername = "user%3B%2F%3F%3A%40%3D%26%3C%3E%23%25%7B%7D%7C%5C%5E%7E%5B%5D" +
                "+%C2%A9%F0%9F%94%92";
        String encodedPassword = "password%3B%2F%3F%3A%40%3D%26%3C%3E%23%25%7B%7D%7C%5C%5E%7E%5B" +
                "%5D+%C2%A9%F0%9F%94%92";
        ReplicatorBuilder.Push p = ReplicatorBuilder.push().
                from(datastore).
                to(new URI("http://" + encodedUsername + ":" + encodedPassword +
                        "@some-host/path%2Fsome-path-日本"));
        ReplicatorImpl r = (ReplicatorImpl) p.build();
        // check that user/pass has been removed
        Assert.assertEquals("http://some-host:80/path%2Fsome-path-日本",
                (((CouchClientWrapper) (((PushStrategy) r.strategy).targetDb)).
                        getCouchClient().
                        getRootUri()).
                        toString()
        );
        assertCookieInterceptorPresent(p, "name="+encodedUsername+"&password=" + encodedPassword);
    }
}
