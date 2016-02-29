/*
 * Copyright (c) 2015 IBM Corp. All rights reserved.
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

import com.cloudant.common.ProxyTestBase;
import com.cloudant.common.RequireRunningProxy;
import com.cloudant.http.Http;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentException;
import com.cloudant.sync.datastore.DocumentRevision;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tomblench on 09/09/15.
 */


@Category(RequireRunningProxy.class)
public class UnreliableNetworkPushTest extends ProxyTestBase {


    String jsonAddToxic = "{\"enabled\" :true, \"timeout\":50, \"sometimesToxic\": true, \"toxicity\" :0.5}";

    @Test
    public void unreliableNetworkPullTest() throws Exception {
        this.addToxic();
        int nDocs = 500;
        for (int i=0; i<nDocs; i++) {
            createLocalDocument("doc" + i);
        }
        PushResult result = super.push();
        Assert.assertEquals(nDocs, result.pushStrategy.getDocumentCounter());
        for (int i=0; i<nDocs; i++) {
            try {
                Map<String, Object> doc = this.remoteDb.get(Map.class, "doc" + i);
                Assert.assertEquals("doc" + i, doc.get("_id"));
            } catch(Exception e) {
                System.out.println(e);
                Assert.fail("Couldn't get doc with exception "+e);
            }
        }
        // TODO a number of extra document updates and pulls to ensure checkpointing is correct
    }

    @Before
    @Override
    public void setUp() throws Exception {
        this.startProxy();
        super.setUp();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        this.removeToxic();
        super.tearDown();
        this.stopProxy();
    }

    private void createLocalDocument(String docid) throws DocumentException {
        DocumentRevision mdr = new DocumentRevision(docid);
        Map<String, Object> doc = new HashMap<String, Object>();
        // TODO make a much more complex document
        int nKeys = 50;
        for (int i=0; i<nKeys; i++) {
            doc.put("key_"+i, "value_"+i);
        }
        mdr.setBody(DocumentBodyFactory.create(doc));
        datastore.createDocumentFromRevision(mdr);
    }

    private void addToxic() throws Exception {
        Http.POST(new URL(String.format("http://%s:%d/proxies/%s/downstream/toxics/timeout",
                proxyHost, proxyAdminPort, proxyName)), "application/json").setRequestBody(jsonAddToxic).execute();
    }

    private void removeToxic() throws Exception {
        Http.DELETE(new URL(String.format("http://%s:%d/proxies/%s/downstream/toxics/timeout",
                proxyHost, proxyAdminPort, proxyName))).execute();
    }

}
