/**
 * Copyright (c) 2015 Cloudant, Inc. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.replication;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.cloudant.sync.datastore.BasicDatastoreTestBase;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.DatastoreTestBase;
import com.cloudant.sync.util.TestUtils;
import com.google.common.collect.ImmutableMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by tomblench on 10/12/14.
 */
public class ReplicatorIdTest {

    // source/target switched, so ids should be different
    @Test
    public void pullNotEqualToPush() throws Exception {
        BasicPullStrategy pull = (BasicPullStrategy) ((BasicReplicator) ReplicatorBuilder.pull()
                .from(new URI
                ("http://default-host/default-database")).to(mock(DatastoreExtended.class)).build
                        ()).strategy;
        BasicPushStrategy push = (BasicPushStrategy) ((BasicReplicator) ReplicatorBuilder.push()
                .to(new URI
                ("http://default-host/default-database")).from(mock(DatastoreExtended.class))
                .build()).strategy;

        Assert.assertNotEquals(pull.getReplicationId(),
                push.getReplicationId());
    }

    // two pull reps identical
    @Test
    public void pullsEqual() throws Exception {
        BasicPullStrategy pull1 = (BasicPullStrategy) ((BasicReplicator) ReplicatorBuilder.pull()
                .from(new URI
                ("http://default-host/default-database")).to(mock(DatastoreExtended.class)).build
                        ()).strategy;
        BasicPullStrategy pull2 = (BasicPullStrategy) ((BasicReplicator) ReplicatorBuilder.pull()
                .from(new URI
                ("http://default-host/default-database")).to(mock(DatastoreExtended.class)).build
                        ()).strategy;

        Assert.assertEquals(pull1.getReplicationId(),
                pull2.getReplicationId());
    }

    // two push reps identical
    @Test
    public void pushesEqual() throws Exception {
        BasicPushStrategy push1 = (BasicPushStrategy) ((BasicReplicator) ReplicatorBuilder.push()
                .to(new URI
                ("http://default-host/default-database")).from(mock(DatastoreExtended.class))
                .build()).strategy;
        BasicPushStrategy push2 = (BasicPushStrategy) ((BasicReplicator) ReplicatorBuilder.push()
                .to(new URI
                ("http://default-host/default-database")).from(mock(DatastoreExtended.class))
                .build()).strategy;
        Assert.assertEquals(push1.getReplicationId(),
                push2.getReplicationId());
    }

    // two push reps with differing target not equal
    @Test
    public void pushesDifferingTargetNotEqual() throws Exception {
        BasicPushStrategy push1 = (BasicPushStrategy) ((BasicReplicator) ReplicatorBuilder.push()
                .to(new URI
                ("http://a-host/a-database")).from(mock(DatastoreExtended.class)).build())
                .strategy;
        BasicPushStrategy push2 = (BasicPushStrategy) ((BasicReplicator) ReplicatorBuilder.push()
                .to(new URI
                ("http://another-host/another-database")).from(mock(DatastoreExtended.class))
                .build()).strategy;

        Assert.assertNotEquals(push1.getReplicationId(),
                push2.getReplicationId());
    }

    // two pull reps with differing source not equal
    @Test
    public void pullsDifferingSourceNotEqual() throws Exception {
        BasicPullStrategy pull1 = (BasicPullStrategy) ((BasicReplicator) ReplicatorBuilder.pull()
                .from(new URI
                ("http://a-host/a-database")).to(mock(DatastoreExtended.class)).build())
                .strategy;
        BasicPullStrategy pull2 = (BasicPullStrategy) ((BasicReplicator) ReplicatorBuilder.pull()
                .from(new URI
                ("http://another-host/another-database")).to(mock(DatastoreExtended.class)).build
                        ()).strategy;

        Assert.assertNotEquals(pull1.getReplicationId(),
                pull2.getReplicationId());
    }

    // two pull reps, one with filter, one without, not equal
    @Test
    public void pullWithFilterNotEqual() throws Exception {
        BasicPullStrategy pull1 = (BasicPullStrategy) ((BasicReplicator) ReplicatorBuilder.pull()
                .from(new URI
                ("http://default-host/default-database")).to(mock(DatastoreExtended.class)).build
                        ()).strategy;
        BasicPullStrategy pull2 = (BasicPullStrategy) ((BasicReplicator) ReplicatorBuilder.pull()
                .from(new URI
                ("http://default-host/default-database")).to(mock(DatastoreExtended.class))
                .filter(new PullFilter("animal/by_class",
                ImmutableMap.of("class", "mammal"))).build()).strategy;

        Assert.assertNotEquals(pull1.getReplicationId(),
                pull2.getReplicationId());
    }

    // two pull reps, both with different filters, not equal
    @Test
    public void pullWithDifferentFiltersNotEqual() throws Exception {
        BasicPullStrategy pull1 = (BasicPullStrategy) ((BasicReplicator) ReplicatorBuilder.pull()
                .from(new URI
                ("http://default-host/default-database")).to(mock(DatastoreExtended.class))
                .filter(new PullFilter("animal/by_class",
                ImmutableMap.of("class", "mammal"))).build()).strategy;
        BasicPullStrategy pull2 = (BasicPullStrategy) ((BasicReplicator) ReplicatorBuilder.pull()
                .from(new URI
                ("http://default-host/default-database")).to(mock(DatastoreExtended.class))
                .filter(new PullFilter("animal/by_class",
                ImmutableMap.of("class", "bird"))).build()).strategy;

        Assert.assertNotEquals(pull1.getReplicationId(),
                pull2.getReplicationId());
    }

}
