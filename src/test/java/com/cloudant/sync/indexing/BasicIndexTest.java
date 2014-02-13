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

package com.cloudant.sync.indexing;

import junit.framework.Assert;
import org.junit.Test;

public class BasicIndexTest {

    @Test
    public void constructor() {
        {
            BasicIndex index1 = new BasicIndex("indexName");
            Assert.assertEquals("indexName", index1.getName());
            Assert.assertEquals(IndexType.STRING, index1.getIndexType());
            Assert.assertTrue(index1.getLastSequence().equals(-1l));
        }

        {
            BasicIndex index2 = new BasicIndex("indexName", IndexType.INTEGER);
            Assert.assertEquals("indexName", index2.getName());
            Assert.assertEquals(IndexType.INTEGER, index2.getIndexType());
            Assert.assertTrue(index2.getLastSequence().equals(-1l));
        }
    }

    @Test
    public void equals() {
        BasicIndex index1 = new BasicIndex("indexName");
        BasicIndex index2 = new BasicIndex("indexName");
        Assert.assertEquals(index1, index2);
    }
}
