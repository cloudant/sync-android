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

public class PullStrategySelectorTest extends ReplicationTestBase {

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
    public void pull_filterSelectorBirdFromAnimalDb_twoDocShouldBePulled() throws Exception {
        org.junit.Assume.assumeTrue(ClientTestUtils.isCouchDBVersion2or3(remoteDb.couchClient.getRootUri()));
        String selector ="{\"selector\":{\"class\":\"bird\"}}";
        PullStrategy replicator = super.getPullStrategy(selector);

        Assert.assertEquals(0, datastore.getDocumentCount());

        AnimalDb.populate(remoteDb.couchClient);
        this.pull(replicator, 2);

        Assert.assertEquals(2, datastore.getDocumentCount());
        String[] birds = {"snipe", "kookaburra"};
        for (String mammal : birds) {
            Assert.assertTrue(datastore.contains(mammal));
        }
    }

    @Test
    public void
    pull_filterSelectorMammalFromAnimalDbUsingParameterizedFilter_eightDocShouldBePulled()
            throws Exception {
        org.junit.Assume.assumeTrue(ClientTestUtils.isCouchDBVersion2or3(remoteDb.couchClient.getRootUri()));
        String selector = "{\"selector\":{\"class\":\"mammal\"}}";
        PullStrategy replicator = super.getPullStrategy(selector);

        Assert.assertEquals(0, datastore.getDocumentCount());

        AnimalDb.populate(remoteDb.couchClient);
        this.pull(replicator, 8);

        Assert.assertEquals(8, datastore.getDocumentCount());
        String[] mammals = {"aardvark", "badger", "elephant", "giraffe", "lemur", "llama",
                "panda", "zebra"};
        for (String mammal : mammals) {
            Assert.assertTrue(datastore.contains(mammal));
        }
    }

    @Test
    public void pull_filterSelectorSmallFromAnimalDbUsingIntegerFilter_eightDocShouldBePulled()
            throws Exception {
        org.junit.Assume.assumeTrue(ClientTestUtils.isCouchDBVersion2or3(remoteDb.couchClient.getRootUri()));
        String selector = "{\"selector\":{\"max_length\":{\"$lte\":2}}}";
        PullStrategy replicator = super.getPullStrategy(selector);

        Assert.assertEquals(0, datastore.getDocumentCount());

        AnimalDb.populate(remoteDb.couchClient);
        this.pull(replicator, 6);

        Assert.assertEquals(6, datastore.getDocumentCount());
        String[] mammals = {"badger", "kookaburra", "lemur", "llama", "panda", "snipe"};
        for (String mammal : mammals) {
            Assert.assertTrue(mammal + " should be in the datastore", datastore.contains(mammal));
        }
    }

    @Test
    public void pull_filterSelectorSmallFromAnimalDbUsingNullFilter_eightDocShouldBePulled()
            throws Exception {
        org.junit.Assume.assumeTrue(ClientTestUtils.isCouchDBVersion2or3(remoteDb.couchClient.getRootUri()));
        String selector = "{\"selector\":{\"chinese_name\":\"\u718a\u732b\"}}";
        PullStrategy replicator = super.getPullStrategy(selector);

        Assert.assertEquals(0, datastore.getDocumentCount());

        AnimalDb.populate(remoteDb.couchClient);
        this.pull(replicator, 1);

        Assert.assertEquals(1, datastore.getDocumentCount());
        String[] mammals = {"panda"};
        for (String mammal : mammals) {
            Assert.assertTrue(mammal + " should be in the datastore", datastore.contains(mammal));
        }
    }
}
