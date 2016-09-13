/*
 * Copyright (C) 2016 IBM Corp. All rights reserved.
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

import com.cloudant.common.CouchUtils;
import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.http.HttpConnectionInterceptorContext;
import com.cloudant.http.HttpConnectionRequestInterceptor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Category(RequireRunningCouchDB.class)
public class PullReplicationTerminationTest extends ReplicationTestBase {

    // Test configuration fields
    private int docBatches = 3;
    private int docsPerBatch = 700;
    private int expectedDocs = docBatches * docsPerBatch;

    // Test run fields
    private Replicator replicator = null;
    private ThrowingInterceptor throwingInterceptor = null;
    private TestReplicationListener listener = null;

    @Before
    public void customizeReplicatorAndPopulateDb() throws Exception {
        setupTerminationTestReplicator();
        populateRemoteDb(docBatches, docsPerBatch, 512);
    }

    private void setupTerminationTestReplicator() {
        replicator = getPullBuilder().addRequestInterceptors((throwingInterceptor = new
                ThrowingInterceptor())).build();
        replicator.getEventBus().register((listener = new TestReplicationListener()));
    }

    private void populateRemoteDb(int batches, int docsPerBatch, int contentSize) throws Exception {
        // Create documents in the remote db
        Random r = new Random();
        for (int batch = 0; batch < batches; batch++) {
            List<Foo> docs = new ArrayList<Foo>(docsPerBatch);
            for (int i = 0; i < docsPerBatch; i++) {
                Foo f = new Foo();
                String docPrefix = batch + "-" + i + "-";
                f.setId(docPrefix + CouchUtils.generateDocumentId());
                f.setRevision(CouchUtils.getFirstRevisionId());
                byte[] bytes = new byte[contentSize];
                r.nextBytes(bytes);
                f.setFoo("Foo " + docPrefix + " " + new String(bytes, "UTF-8"));
                docs.add(f);
            }
            remoteDb.getCouchClient().bulkCreateDocs(docs);
        }
    }

    /**
     * Wait up to 1 minute for the replicator to reach the desired state and assert that it did.
     *
     * @param state
     * @throws Exception
     */
    private void waitForReplicatorToReachState(Replicator.State state) throws Exception {
        waitForReplicatorToReachState(state, 1, TimeUnit.MINUTES);
    }

    /**
     * Wait for the replicator to reach the desired state
     *
     * @param state        the state to check for
     * @param maxWait      the maximum time to wait
     * @param maxWaitUnits the units of the wait time
     * @throws Exception
     */
    private void waitForReplicatorToReachState(Replicator.State state, long maxWait, TimeUnit
            maxWaitUnits) throws Exception {
        long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(maxWait, maxWaitUnits);
        while (replicator.getState() != state && System.nanoTime() - timeout < 0) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        Assert.assertEquals("The replicator should have reached the state " + state.toString() +
                        " within the timeout.", state,
                replicator.getState());
    }

    private void startReplication() throws Exception {
        // Start the replication
        replicator.start();

        // Wait until the replicator is already started before stopping
        waitForReplicatorToReachState(Replicator.State.STARTED);

        // Let the replicator run for a short time
        TimeUnit.MILLISECONDS.sleep(500);
    }

    private void assertComplete() throws Exception {
        // Now run the replicator to completion
        waitForReplicatorToReachState(Replicator.State.COMPLETE, 5, TimeUnit.MINUTES);

        // Validate that the local datastore contains all the documents
        Assert.assertEquals("The local datastore should contain all the documents", expectedDocs,
                datastore.getDocumentCount());

        // Assert that the listener was called
        Assert.assertTrue("The listener should have been called when the replication " +
                        "completed.",
                listener.finishCalled);
    }

    /**
     * Interceptor that has a flag to enable throwing an exception from an HTTP request.
     * Used for testing exception cases.
     */
    private static final class ThrowingInterceptor implements HttpConnectionRequestInterceptor {

        volatile boolean shouldThrow = false;

        @Override
        public HttpConnectionInterceptorContext interceptRequest
                (HttpConnectionInterceptorContext context) {
            if (shouldThrow == true) {
                throw new RuntimeException("Test exception");
            } else {
                return context;
            }
        }
    }

    /**
     * Test that a replication can be stopped.
     * Uses a lot of large-ish docs to ensure the replication takes some time.
     *
     * @throws Exception
     */
    @Test
    public void stopRunningReplication() throws Exception {

        // Start the replication
        startReplication();

        // Now stop the replicator
        replicator.stop();

        // Wait until the replicator is stopped
        waitForReplicatorToReachState(Replicator.State.STOPPED);

        Assert.assertTrue("The listener should have been called when the replication stopped.",
                listener.finishCalled);
        // Record the number of docs written before stopping
        int docs = listener.docs;

        // Assert that the listener data matches the local datastore
        Assert.assertEquals("The local datastore should contain the expected number of " +
                "documents", docs, datastore.getDocumentCount());
    }

    /**
     * Test that a replication can be stopped.
     * Uses a lot of large-ish docs to ensure the replication takes some time.
     *
     * @throws Exception
     */
    @Test
    public void resumeStoppedReplication() throws Exception {

        // Execute the same steps as stopping the replication
        stopRunningReplication();

        // Record the number of docs written at the time of the stop
        int docs = listener.docs;

        // Assert that the existing replicator is invalid
        replicator.start();
        // This should complete instantly
        waitForReplicatorToReachState(Replicator.State.COMPLETE, 10, TimeUnit.SECONDS);
        // Validate that the listener recorded 0 batches and docs
        Assert.assertEquals("The listener should have recorded 0 batches", 0, listener.batches);
        Assert.assertEquals("The listener should have recorded 0 docs", 0, listener.docs);

        // Validate that no additional documents were added to the local datastore
        Assert.assertEquals("The local datastore should contain all the documents", docs,
                datastore.getDocumentCount());

        // Now start a new replication
        setupTerminationTestReplicator();
        startReplication();

        // Assert it completes and all docs present
        assertComplete();
    }

    @Test
    public void replicatorHttpException() throws Exception {

        // Start the replication
        startReplication();

        // Set the interceptor to throw
        throwingInterceptor.shouldThrow = true;

        // Wait for the error state
        waitForReplicatorToReachState(Replicator.State.ERROR);

        Assert.assertTrue("The listener should have been called when the replication errored.",
                listener.errorCalled);
    }

    @Test
    public void resumeErroredReplication() throws Exception {

        // Execute the same steps as the HttpException case
        replicatorHttpException();

        // Turn off the exception
        throwingInterceptor.shouldThrow = false;

        // Now start it again
        startReplication();

        // Assert it completes and all docs present
        assertComplete();
    }

    @Test
    public void runMultiBatchLargeReplicationToCompletion() throws Exception {
        runReplicationUntilComplete(replicator);
        assertComplete();
    }
}
