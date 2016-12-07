/*
 * Copyright © 2015, 2017 IBM Corp. All rights reserved.
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

import com.cloudant.common.UnreliableProxyTestBase;
import com.cloudant.common.RequireRunningProxy;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tomblench on 15/07/15.
 */

@Category(RequireRunningProxy.class)
public class UnreliableNetworkPullTest extends UnreliableProxyTestBase {

    @Test
    public void unreliableNetworkPullTest() throws Exception {
        // test needs to create documents before adding the toxic
        // otherwise document creation may fail or at best take a long time
        int nDocs = 500;
        for (int i=0; i<nDocs; i++) {
            createRemoteDocument("doc" + i);
        }
        addTimeoutToxic();
        super.pull();
        Assert.assertEquals(nDocs, this.datastore.getIds().size());
        // TODO a number of extra document updates and pulls to ensure checkpointing is correct
    }

    private void createRemoteDocument(String docid) {
        Map<String, Object> doc = new HashMap<String, Object>();
        doc.put("_id", docid);
        // TODO make a much more complex document
        int nKeys = 50;
        for (int i=0; i<nKeys; i++) {
            doc.put("key_"+i, "value_"+i);
        }
        this.remoteDb.create(doc);
    }
}
