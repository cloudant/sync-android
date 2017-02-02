/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2015 Cloudant, Inc. All rights reserved.
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.cloudant.common.CollectionFactory;
import com.cloudant.sync.documentstore.Database;
import com.cloudant.sync.documentstore.DocumentStore;
import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.replication.PullFilter;
import com.cloudant.sync.replication.ReplicatorBuilder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URI;

/**
 * Created by tomblench on 10/12/14.
 */
public class ReplicatorIdTest {

    DocumentStore mockDocumentStore;

    @Before
    public void before() {
        mockDocumentStore = mock(DocumentStore.class);
        // we have to mock DatabaseImpl because of a down-cast in the call to the DatastoreWrapper
        // constructor
        Database mockDatabase = mock(DatabaseImpl.class);
        when(mockDocumentStore.database()).thenReturn(mockDatabase);
        // the value of getPath() doesn't have to be valid, it just needs to be non-null to satisfy
        // {Pull,Push}Strategy constructors when generating replicatorName
        when(mockDatabase.getPath()).thenReturn(new File("/abc"));
    }

    // source/target switched, so IDs should be different
    @Test
    public void pullNotEqualToPush() throws Exception {
        PullStrategy pull = (PullStrategy) ((ReplicatorImpl) ReplicatorBuilder.pull()
                .from(new URI
                ("http://default-host/default-database")).to(mockDocumentStore).build
                        ()).strategy;
        PushStrategy push = (PushStrategy) ((ReplicatorImpl) ReplicatorBuilder.push()
                .to(new URI
                ("http://default-host/default-database")).from(mockDocumentStore)
                .build()).strategy;

        Assert.assertNotEquals(pull.getReplicationId(),
                push.getReplicationId());
    }

    // two pull reps identical
    @Test
    public void pullsEqual() throws Exception {
        PullStrategy pull1 = (PullStrategy) ((ReplicatorImpl) ReplicatorBuilder.pull()
                .from(new URI
                ("http://default-host/default-database")).to(mockDocumentStore).build
                        ()).strategy;
        PullStrategy pull2 = (PullStrategy) ((ReplicatorImpl) ReplicatorBuilder.pull()
                .from(new URI
                ("http://default-host/default-database")).to(mockDocumentStore).build
                        ()).strategy;

        Assert.assertEquals(pull1.getReplicationId(),
                pull2.getReplicationId());
    }

    // two push reps identical
    @Test
    public void pushesEqual() throws Exception {
        PushStrategy push1 = (PushStrategy) ((ReplicatorImpl) ReplicatorBuilder.push()
                .to(new URI
                ("http://default-host/default-database")).from(mockDocumentStore)
                .build()).strategy;
        PushStrategy push2 = (PushStrategy) ((ReplicatorImpl) ReplicatorBuilder.push()
                .to(new URI
                ("http://default-host/default-database")).from(mockDocumentStore)
                .build()).strategy;
        Assert.assertEquals(push1.getReplicationId(),
                push2.getReplicationId());
    }

    // two push reps with differing target not equal
    @Test
    public void pushesDifferingTargetNotEqual() throws Exception {
        PushStrategy push1 = (PushStrategy) ((ReplicatorImpl) ReplicatorBuilder.push()
                .to(new URI
                ("http://a-host/a-database")).from(mockDocumentStore).build())
                .strategy;
        PushStrategy push2 = (PushStrategy) ((ReplicatorImpl) ReplicatorBuilder.push()
                .to(new URI
                ("http://another-host/another-database")).from(mockDocumentStore)
                .build()).strategy;

        Assert.assertNotEquals(push1.getReplicationId(),
                push2.getReplicationId());
    }

    // two pull reps with differing source not equal
    @Test
    public void pullsDifferingSourceNotEqual() throws Exception {
        PullStrategy pull1 = (PullStrategy) ((ReplicatorImpl) ReplicatorBuilder.pull()
                .from(new URI
                ("http://a-host/a-database")).to(mockDocumentStore).build())
                .strategy;
        PullStrategy pull2 = (PullStrategy) ((ReplicatorImpl) ReplicatorBuilder.pull()
                .from(new URI
                ("http://another-host/another-database")).to(mockDocumentStore).build
                        ()).strategy;

        Assert.assertNotEquals(pull1.getReplicationId(),
                pull2.getReplicationId());
    }

    // two pull reps, one with filter, one without, not equal
    @Test
    public void pullWithFilterNotEqual() throws Exception {
        PullStrategy pull1 = (PullStrategy) ((ReplicatorImpl) ReplicatorBuilder.pull()
                .from(new URI
                ("http://default-host/default-database")).to(mockDocumentStore).build
                        ()).strategy;
        PullStrategy pull2 = (PullStrategy) ((ReplicatorImpl) ReplicatorBuilder.pull()
                .from(new URI
                ("http://default-host/default-database")).to(mockDocumentStore)
                .filter(new PullFilter("animal/by_class",
                CollectionFactory.MAP.of("class", "mammal"))).build()).strategy;

        Assert.assertNotEquals(pull1.getReplicationId(),
                pull2.getReplicationId());
    }

    // two pull reps, both with different filters, not equal
    @Test
    public void pullWithDifferentFiltersNotEqual() throws Exception {
        PullStrategy pull1 = (PullStrategy) ((ReplicatorImpl) ReplicatorBuilder.pull()
                .from(new URI
                ("http://default-host/default-database")).to(mockDocumentStore)
                .filter(new PullFilter("animal/by_class",
                CollectionFactory.MAP.of("class", "mammal"))).build()).strategy;
        PullStrategy pull2 = (PullStrategy) ((ReplicatorImpl) ReplicatorBuilder.pull()
                .from(new URI
                ("http://default-host/default-database")).to(mockDocumentStore)
                .filter(new PullFilter("animal/by_class",
                CollectionFactory.MAP.of("class", "bird"))).build()).strategy;

        Assert.assertNotEquals(pull1.getReplicationId(),
                pull2.getReplicationId());
    }

}
