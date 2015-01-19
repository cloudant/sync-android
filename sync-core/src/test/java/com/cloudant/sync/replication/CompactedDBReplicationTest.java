/*
 * Copyright (c) 2014 Cloudant, Inc. All rights reserved.
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

import com.cloudant.mazha.ClientTestUtils;
import com.cloudant.mazha.CouchDbInfo;
import com.cloudant.mazha.Response;
import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.DocumentRevisionTree;
import com.cloudant.sync.util.AbstractTreeNode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CompactedDBReplicationTest extends ReplicationTestBase {

    BasicReplicator replicator;
    boolean testWithCloudant = false;

    @Before
    public void setUp() throws Exception {
       super.setUp();

       String cloudantTest = System.getProperty("test.with.cloudant");
       testWithCloudant = Boolean.parseBoolean(cloudantTest);
    }



    @Test
    public void replicationFromCompactedDB() throws Exception{
        // if the test case is running against Cloudant, this test should not execute since
        // Cloudant returns 403 - Forbidden when attempting to call _compact
        if(testWithCloudant){
            return;
        }

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

        Assert.assertEquals(ClientTestUtils.executeHttpPostRequest(couchClient, postURI, ""), 202);
        CouchDbInfo info = couchClient.getDbInfo();

        while(info.isCompactRunning()) {
            Thread.sleep(1000);
            info = couchClient.getDbInfo();
        };

        // replicate with compacted database

        PullReplication pull = createPullReplication();
        replicator = (BasicReplicator)ReplicatorFactory.oneway(pull);

        TestReplicationListener listener = new TestReplicationListener();
        Assert.assertEquals(Replicator.State.PENDING, replicator.getState());
        replicator.getEventBus().register(listener);
        replicator.start();
        Assert.assertEquals(Replicator.State.STARTED, replicator.getState());

        while(replicator.getState() != Replicator.State.COMPLETE) {
            Thread.sleep(1000);
        }

        Assert.assertEquals(Replicator.State.COMPLETE, replicator.getState());
        Assert.assertEquals(1, datastore.getDocumentCount());

        Assert.assertTrue(listener.finishCalled);
        Assert.assertFalse(listener.errorCalled);

        // compare remote revisions with local revisions

        URI getURI = new URI(couchClient.getRootUri().toString() + "/" + documentName + "?revs_info=true");

        List<String> remoteRevs = ClientTestUtils.getRemoteRevisionIDs(couchClient, getURI);
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


    private void extractRevisionIDsFromChildren(AbstractTreeNode<BasicDocumentRevision> node,
                                                List<String> documentRevisions){

        if(node.hasChildren()) {
            Iterator<AbstractTreeNode<BasicDocumentRevision>> iterator = node.iterateChildren();
            while(iterator.hasNext()){
                AbstractTreeNode<BasicDocumentRevision> absNode = iterator.next();
                documentRevisions.add(absNode.getData().getRevision());
                extractRevisionIDsFromChildren(absNode, documentRevisions);
            }
        }

    }
}
