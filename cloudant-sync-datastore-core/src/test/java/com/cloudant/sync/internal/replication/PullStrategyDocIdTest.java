/*
 * Copyright © 2018 IBM Corp. All rights reserved.
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

import com.cloudant.sync.internal.mazha.AnimalDb;
import com.cloudant.sync.internal.mazha.ClientTestUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class PullStrategyDocIdTest extends ReplicationTestBase {

    // we use this utility method rather than ReplicationTestBase.pull() because some
    // methods want to make assertions on the BasicPullStrategy after running the replication
    private void pull(PullStrategy replicator, int expectedDocs) throws Exception {
        TestStrategyListener listener = new TestStrategyListener();
        replicator.getEventBus().register(listener);
        replicator.run();
        listener.assertReplicationCompletedOrThrow();
        Assert.assertEquals(expectedDocs, listener.documentsReplicated);
    }

    @Test
    public void pull_filterDocIdsFromAnimalDb_twoDocShouldBePulled() throws Exception {
        org.junit.Assume.assumeTrue(ClientTestUtils.isCouchDBVersion2or3(remoteDb.couchClient.getRootUri()));
        List<String> docIds = Arrays.asList("snipe","kookaburra");
        PullStrategy replicator = super.getPullStrategy(docIds);

        Assert.assertEquals(0, datastore.getDocumentCount());

        AnimalDb.populate(remoteDb.couchClient);
        this.pull(replicator, 2);

        Assert.assertEquals(2, datastore.getDocumentCount());

        for (String bird : docIds) {
            Assert.assertTrue(datastore.contains(bird));
        }
    }

    @Test
    public void
    pull_filterSelectorMammalFromAnimalDbUsingParameterizedFilter_eightDocShouldBePulled()
            throws Exception {
        org.junit.Assume.assumeTrue(ClientTestUtils.isCouchDBVersion2or3(remoteDb.couchClient.getRootUri()));
        List<String> docIds = Arrays.asList("aardvark", "badger", "elephant", "giraffe", "lemur", "llama",
                "panda", "zebra");
        PullStrategy replicator = super.getPullStrategy(docIds);

        Assert.assertEquals(0, datastore.getDocumentCount());

        AnimalDb.populate(remoteDb.couchClient);
        this.pull(replicator, 8);

        Assert.assertEquals(8, datastore.getDocumentCount());

        for (String mammal : docIds) {
            Assert.assertTrue(datastore.contains(mammal));
        }
    }

}
