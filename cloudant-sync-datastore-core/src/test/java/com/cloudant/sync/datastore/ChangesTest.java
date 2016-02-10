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

package com.cloudant.sync.datastore;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.hasItems;

public class ChangesTest extends BasicDatastoreTestBase {

    @Test
    public void changes() throws Exception {
        DocumentRevision[] docs = createThreeDocuments();
        List<DocumentRevision> docsList = new ArrayList<DocumentRevision>();
        docsList.add(docs[0]);
        docsList.add(docs[1]);

        Changes c = new Changes(101L, docsList);
        Assert.assertEquals(101L, c.getLastSequence());
        Assert.assertEquals(2, c.size());
        Assert.assertThat(c.getIds(), hasItems(docs[0].getId(), docs[1].getId()));
        Assert.assertEquals(2, c.getResults().size());
    }
}
