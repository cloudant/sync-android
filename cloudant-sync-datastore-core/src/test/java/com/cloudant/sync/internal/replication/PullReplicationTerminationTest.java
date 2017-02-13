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

import com.cloudant.sync.internal.common.CouchUtils;
import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.http.HttpConnectionInterceptorContext;
import com.cloudant.http.HttpConnectionRequestInterceptor;
import com.cloudant.sync.replication.Replicator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Category(RequireRunningCouchDB.class)
public class PullReplicationTerminationTest extends ReplicationTestBase {

    // Test exception to use in error tests
    private static final RuntimeException TEST_EXCEPTION = new RuntimeException("Test Exception");

    // Test configuration fields
    private static final int DOC_BATCHES = 3;
    private static final int DOCS_PER_BATCH = 700;
    private static final int EXPECTED_DOCS = DOC_BATCHES * DOCS_PER_BATCH;
    private static final int CONTENT_SIZE = 512;

    // Generated doc list
    private static List<List<Foo>> GENERATED_FOO_DOC_BATCHES = new ArrayList<List<Foo>>
            (DOC_BATCHES);

    // Test run fields
    private Replicator replicator = null;
    private ThrowingInterceptor throwingInterceptor = null;
    private TestReplicationListener listener = null;

    // Generate some content of the specified size
    @BeforeClass
    public static void generateContent() throws Exception {
        // Generate a string of contentSize bytes so the documents are not tiny
        final String abc123 = "AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789";
        int factor = CONTENT_SIZE / abc123.length() + 1;
        final StringBuilder contentGenerator = new StringBuilder(factor * abc123.length());
        for (int i = 0; i < factor; i++) {
            contentGenerator.append(abc123);
        }
        // Trim any excess
        contentGenerator.setLength(CONTENT_SIZE);
        final String generatedFoo = contentGenerator.toString();

        // Generate the Foo docs
        for (int batch = 0; batch < DOC_BATCHES; batch++) {
            List<Foo> batchDocs = new ArrayList<Foo>(DOCS_PER_BATCH);
            for (int i = 0; i < DOCS_PER_BATCH; i++) {
                Foo f = new Foo();
                String docPrefix = batch + "-" + i + "-";
                f.setId(docPrefix + CouchUtils.generateDocumentId());
                f.setRevision(CouchUtils.getFirstRevisionId());
                f.setFoo(generatedFoo);
                batchDocs.add(f);
            }
            GENERATED_FOO_DOC_BATCHES.add(batchDocs);
        }
    }

    @Before
    public void customizeReplicatorAndPopulateDb() throws Exception {
        setupTerminationTestReplicator();
        populateRemoteDb();
    }

    private void setupTerminationTestReplicator() {
        replicator = getPullBuilder().addRequestInterceptors((throwingInterceptor = new
                ThrowingInterceptor())).build();
        replicator.getEventBus().register((listener = new TestReplicationListener()));
    }


    private void populateRemoteDb() throws Exception {
        // Create documents in the remote db in batches
        for (List<Foo> batch : GENERATED_FOO_DOC_BATCHES) {
            remoteDb.getCouchClient().bulkCreateDocs(batch);
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

        // Assert that the listener was called
        listener.assertReplicationCompletedOrThrow();

        // Validate that the local datastore contains all the documents
        Assert.assertEquals("The local datastore should contain all the documents", EXPECTED_DOCS,
                datastore.getDocumentCount());
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
                throw TEST_EXCEPTION;
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

        listener.assertReplicationCompletedOrThrow();

        // Record the number of docs written before stopping
        int docs = listener.documentsReplicated;

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
        int docs = listener.documentsReplicated;

        // Assert that the existing replicator is invalid
        replicator.start();
        // This should complete instantly
        waitForReplicatorToReachState(Replicator.State.COMPLETE, 10, TimeUnit.SECONDS);
        // Validate that the listener recorded 0 batches and docs
        Assert.assertEquals("The listener should have recorded 0 batches", 0, listener
                .batchesReplicated);
        Assert.assertEquals("The listener should have recorded 0 docs", 0, listener
                .documentsReplicated);

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

        Assert.assertNotNull("There should be errorInfo set on the listener", listener.errorInfo);
        Throwable cause = listener.errorInfo;
        Assert.assertNotNull("There should be an exception set on the listener", cause);

        // Get all the causes and assert that the test exception was one of them.
        List<Throwable> causes = new ArrayList<Throwable>();
        do {
            causes.add(cause);
        } while ((cause = cause.getCause()) != null);
        Assert.assertTrue("The test exception should be a cause of the listener exception",
                causes.contains(TEST_EXCEPTION));
    }

    @Test
    public void resumeErroredReplication() throws Exception {

        // Execute the same steps as the HttpException case
        replicatorHttpException();

        // Turn off the exception and reset the listener's error state
        throwingInterceptor.shouldThrow = false;
        listener.errorCalled = false;
        listener.errorInfo = null;

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
