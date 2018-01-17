/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.internal.mazha;

import static org.hamcrest.Matchers.hasSize;

import com.cloudant.common.CollectionFactory;
import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.sync.replication.PullFilter;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(RequireRunningCouchDB.class)
public class CouchClientFilteredChangesTest extends CouchClientTestBase {

    @Before
    public void setUp() {
        super.setUp();
        AnimalDb.populate(client);
    }

    @Test
    public void changes_filteredWoParameter() {
        PullFilter filter = new PullFilter("animal/bird");
        ChangesResult changes = client.changes(filter, null, 5);
        Assert.assertThat(changes.getResults(), hasSize(2));
    }

    @Test
    public void changes_filteredWoParameterAndMoreThanLimitNumberOfDocs() {
        PullFilter filter = new PullFilter("animal/mammal");
        ChangesResult firstChangeSet = client.changes(filter, null, 5);
        Assert.assertThat(firstChangeSet.getResults(), hasSize(5));

        ChangesResult secondChangeSet = client.changes(filter, firstChangeSet.getLastSeq(), 5);
        Assert.assertThat(secondChangeSet.getResults(), hasSize(3));
    }

    @Test
    public void changes_filteredWithParameter() {
        PullFilter filter = new PullFilter("animal/by_class",
                CollectionFactory.MAP.of("class", "mammal"));
        ChangesResult changes = client.changes(filter, null, 100);
        Assert.assertThat(changes.getResults(), hasSize(8));
    }
}
