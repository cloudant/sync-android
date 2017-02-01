/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2014 Cloudant, Inc. All rights reserved.
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

import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.common.TestOptions;
import com.cloudant.sync.internal.documentstore.InternalDocumentRevision;
import com.cloudant.sync.internal.mazha.ClientTestUtils;
import com.cloudant.sync.internal.mazha.CouchDbInfo;
import com.cloudant.sync.internal.mazha.Response;
import com.cloudant.sync.internal.documentstore.DocumentRevisionTree;
import com.cloudant.sync.internal.util.AbstractTreeNode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


@Category(RequireRunningCouchDB.class)
public class CompactedDBReplicationTest extends ReplicationTestBase {

    ReplicatorImpl replicator;

    @Before
    public void setUp() throws Exception {
       super.setUp();
    }

    @Test
    public void replicationFromCompactedDB() throws Exception{
        // if the test case is running against Cloudant, this test should not execute since
        // Cloudant returns 403 - Forbidden when attempting to call _compact
        if(TestOptions.IGNORE_COMPACTION){return;}
        // skip test if we are doing cookie auth, we don't have the interceptor chain to do it
        // when we call ClientTestUtils.executeHttpPostRequest
        if(TestOptions.COOKIE_AUTH){return;}

        String documentName;
        Bar bar = BarUtils.createBar(remoteDb, "Bob", 12);
        Response res = couchClient.create(bar);

        documentName = res.getId();

        // create revisions to the document

        for (int i=1; i<11; i++) {
            bar.setRevision(res.getRev());
            bar.setAge(bar.getAge() + i);
            res = couchClient.update(bar.getId(), bar);
            Assert.assertTrue("Failure during DB creation", res.getOk());
        }

        // compact database

        URI postURI = new URI(couchClient.getRootUri().toString() + "/_compact");

        Assert.assertEquals(202, ClientTestUtils.executeHttpPostRequest(postURI, ""));
        CouchDbInfo info = couchClient.getDbInfo();

        while(info.isCompactRunning()) {
            Thread.sleep(1000);
            info = couchClient.getDbInfo();
        };

        // replicate with compacted database

        super.pull();
        Assert.assertEquals(1, datastore.getDocumentCount());

        // compare remote revisions with local revisions

        URI getURI = new URI(couchClient.getRootUri().toString() + "/" + documentName + "?revs_info=true");

        List<String> remoteRevs = ClientTestUtils.getRemoteRevisionIDs(getURI, getCouchConfig(this.getDbName()));
        List<String> localRevs = new ArrayList<String>();
        DocumentRevisionTree localRevsTree = datastore.getAllRevisionsOfDocument(bar.getId());

        Map<Long, DocumentRevisionTree.DocumentRevisionNode> roots = localRevsTree.roots();
        Set<Long> rootSet = roots.keySet();
        for(Long l:rootSet){
            DocumentRevisionTree.DocumentRevisionNode node = roots.get(l);
            localRevs.add(node.getData().getRevision());
            extractRevisionIDsFromChildren(node, localRevs);

        }

        for(String rev: remoteRevs){
            Assert.assertTrue("Remote revision missing from local replica, rev missing: " + rev,
                    localRevs.contains(rev));
        }


    }


    private void extractRevisionIDsFromChildren(AbstractTreeNode<InternalDocumentRevision> node,
                                                List<String> documentRevisions){

        if(node.hasChildren()) {
            Iterator<AbstractTreeNode<InternalDocumentRevision>> iterator = node.iterateChildren();
            while(iterator.hasNext()){
                AbstractTreeNode<InternalDocumentRevision> absNode = iterator.next();
                documentRevisions.add(absNode.getData().getRevision());
                extractRevisionIDsFromChildren(absNode, documentRevisions);
            }
        }

    }
}
