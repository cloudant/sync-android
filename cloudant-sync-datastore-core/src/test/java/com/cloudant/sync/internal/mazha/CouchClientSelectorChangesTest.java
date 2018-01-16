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

package com.cloudant.sync.internal.mazha;

import static org.hamcrest.Matchers.hasSize;

import com.cloudant.common.RequireCloudant;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(RequireCloudant.class)
public class CouchClientSelectorChangesTest extends CouchClientTestBase {

    @Before
    public void setUp() {
        super.setUp();
        AnimalDb.populate(client);
    }

    @Test
    public void changes_selector() {
        String animalBirdSelector = "{\"selector\":{\"class\":\"bird\"}}";
        ChangesResult changes = client.changes(animalBirdSelector, null, 5);
        Assert.assertThat(changes.getResults(), hasSize(2));
    }

    @Test
    public void changes_selectorAndMoreThanLimitNumberOfDocs() {
        String animalMammalSelector = "{\"selector\":{\"class\":\"mammal\"}}";
        ChangesResult firstChangeSet = client.changes(animalMammalSelector, null, 5);
        Assert.assertThat(firstChangeSet.getResults(), hasSize(5));

        ChangesResult secondChangeSet = client.changes(animalMammalSelector, firstChangeSet
                .getLastSeq(), 5);
        Assert.assertThat(secondChangeSet.getResults(), hasSize(3));
    }

}
