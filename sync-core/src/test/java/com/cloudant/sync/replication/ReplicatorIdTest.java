/**
 * Copyright (c) 2015 Cloudant, Inc. All rights reserved.
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
    public void pullNotEqualToPush() throws URISyntaxException {
        PullReplication pull = this.createPullReplication();
        PushReplication push = this.createPushReplication();

        Assert.assertNotEquals(pull.createReplicationStrategy().getReplicationId(),
                push.createReplicationStrategy().getReplicationId());
    }

    // two pull reps identical
    @Test
    public void pullsEqual() throws URISyntaxException {
        PullReplication pull1 = this.createPullReplication();
        PullReplication pull2 = this.createPullReplication();

        Assert.assertEquals(pull1.createReplicationStrategy().getReplicationId(),
                pull2.createReplicationStrategy().getReplicationId());
    }

    // two push reps identical
    @Test
    public void pushesEqual() throws URISyntaxException {
        PushReplication push1 = this.createPushReplication();
        PushReplication push2 = this.createPushReplication();
        Assert.assertEquals(push1.createReplicationStrategy().getReplicationId(),
                push2.createReplicationStrategy().getReplicationId());
    }

    // two push reps with differing target not equal
    @Test
    public void pushesDifferingTargetNotEqual() throws URISyntaxException {
        PushReplication push1 = this.createPushReplication();
        PushReplication push2 = this.createPushReplication();
        push1.target = new URI("http://a-host/a-database");
        push2.target = new URI("http://another-host/another-database");

        Assert.assertNotEquals(push1.createReplicationStrategy().getReplicationId(),
                push2.createReplicationStrategy().getReplicationId());
    }

    // two pull reps with differing source not equal
    @Test
    public void pullsDifferingSourceNotEqual() throws URISyntaxException {
        PullReplication pull1 = this.createPullReplication();
        PullReplication pull2 = this.createPullReplication();
        pull1.source = new URI("http://a-host/a-database");
        pull2.source = new URI("http://another-host/another-database");

        Assert.assertNotEquals(pull1.createReplicationStrategy().getReplicationId(),
                pull2.createReplicationStrategy().getReplicationId());
    }

    // two pull reps, one with filter, one without, not equal
    @Test
    public void pullWithFilterNotEqual() throws URISyntaxException {
        PullReplication pull1 = this.createPullReplication();
        PullReplication pull2 = this.createPullReplication();
        pull2.filter = new Replication.Filter("animal/by_class",
                ImmutableMap.of("class", "mammal"));

        Assert.assertNotEquals(pull1.createReplicationStrategy().getReplicationId(),
                pull2.createReplicationStrategy().getReplicationId());
    }

    // two pull reps, both with different filters, not equal
    @Test
    public void pullWithDifferentFiltersNotEqual() throws URISyntaxException {
        PullReplication pull1 = this.createPullReplication();
        PullReplication pull2 = this.createPullReplication();
        pull1.filter = new Replication.Filter("animal/by_class",
                ImmutableMap.of("class", "mammal"));
        pull2.filter = new Replication.Filter("animal/by_class",
                ImmutableMap.of("class", "bird"));

        Assert.assertNotEquals(pull1.createReplicationStrategy().getReplicationId(),
                pull2.createReplicationStrategy().getReplicationId());
    }

    PullReplication createPullReplication() throws URISyntaxException {
        PullReplication pullReplication = new PullReplication();
        pullReplication.source = new URI("http://default-host/default-database");
        DatastoreExtended datastore = mock(DatastoreExtended.class);
        when(datastore.getPublicIdentifier()).thenReturn("this would be a database GUID");
        pullReplication.target = datastore;
        return pullReplication;
    }

    PushReplication createPushReplication() throws URISyntaxException {
        PushReplication pushReplication = new PushReplication();
        pushReplication.target = new URI("http://default-host/default-database");
        DatastoreExtended datastore = mock(DatastoreExtended.class);
        when(datastore.getPublicIdentifier()).thenReturn("this would be a database GUID");
        pushReplication.source = datastore;
        return pushReplication;
    }
}
