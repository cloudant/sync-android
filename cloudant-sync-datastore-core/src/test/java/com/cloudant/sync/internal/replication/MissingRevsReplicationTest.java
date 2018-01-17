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


import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.internal.documentstore.DocumentRevisionTree;
import com.cloudant.sync.internal.mazha.ChangesResult;
import com.cloudant.sync.internal.mazha.CouchClient;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


public class MissingRevsReplicationTest extends ReplicationTestBase {

    private CouchClient clientMock;

    @Before
    public void setupMocks() throws Exception {
        // Partially mock the remote and replace it for our test
        clientMock = spy(remoteDb.couchClient);
        remoteDb.couchClient = clientMock;
        couchClient = clientMock;
    }

    @Override
    protected PullStrategy getPullStrategy() {
        PullStrategy s = super.getPullStrategy();
        s.sourceDb = remoteDb;
        return s;
    }

    @Test
    public void testReplicationWithMissingRevision() throws Exception {
        // Create doc
        Bar a = BarUtils.createBar(remoteDb, "testdoc", "alpha", 1);
        // Update doc
        final Bar b = BarUtils.updateBar(remoteDb, "testdoc", "beta", 2);

        Bar c = BarUtils.updateBar(remoteDb, "testdoc", "gamma", 3);
        // We have 3 remote revisions

        doAnswer(new Answer<ChangesResult>() {

            @Override
            public ChangesResult answer(InvocationOnMock invocation) throws Throwable {
                @SuppressWarnings("unchecked")
                ChangesResult changesResult = (ChangesResult) invocation.callRealMethod();

                // modify the changes feed so that it returns a revision that isn't a leaf revision.
                changesResult.getResults().get(0).getChanges().get(0).setRev(b.getRevision());
                return changesResult;

            }
        }).when(clientMock).changes(Matchers.anyObject(), anyInt());

        // Do the pull replication
        pull();
        // Validate the document was created at the correct rev
        DocumentRevision rev = datastore.read("testdoc");
        Assert.assertNotNull("The document should be present", rev);
        Assert.assertEquals("The local revision should be the third generation",
                c.getRevision(),
                rev.getRevision());
        // Validate that there are 3 revisions.
        DocumentRevisionTree tree = datastore.getAllRevisionsOfDocument("testdoc");
        Assert.assertEquals("The 3rd generation current revision should have depth 2", 2, tree.depth
                (tree.getCurrentRevision().getSequence()));
    }
}
