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

import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreExtended;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import static org.mockito.Mockito.mock;

public class ReplicatorFactoryTest {

    URI uri;
    Datastore mockDatastore;

    @Before
    public void setUp() throws Exception {
        mockDatastore = mock(DatastoreExtended.class);
        uri = new URI("http://127.0.0.1:5984/test");
    }

    @Test
    public void oneway_datastoreAndURI_pullReplicatorReturned() {
        Replicator replicator = ReplicatorFactory.oneway(mockDatastore, uri);
        Assert.assertTrue(replicator instanceof BasicReplicator);
    }

    @Test
    public void oneway_datastoreAndURI_pushReplicatorReturned() {
        Replicator replicator = ReplicatorFactory.oneway(uri, mockDatastore);
        Assert.assertTrue(replicator instanceof BasicReplicator);
    }
}
