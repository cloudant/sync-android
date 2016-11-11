/*
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

import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.common.TestOptions;
import com.cloudant.sync.internal.mazha.ClientTestUtils;
import com.cloudant.sync.internal.mazha.Response;
import com.cloudant.sync.documentstore.DocumentBodyFactory;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.internal.datastore.DocumentRevisionTree;
import com.cloudant.sync.internal.util.AbstractTreeNode;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by rhys on 28/11/14.
 */

@Category(RequireRunningCouchDB.class)
public class DBWithSlashReplicationTest extends ReplicationTestBase {

    // NB although the user has to encode the / themselves as %2F, this is still a valuable test
    // as it shows we don't double-encode eg encode %2F as %252F

    ReplicatorImpl replicator;

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void replicationPullAndPushDbWithSlash() throws Exception {

        Assume.assumeFalse("Not running test as 'getRemoteRevisionIDs' can't retrieve revisions " +
                "with cookie authentication enabled", TestOptions.COOKIE_AUTH);

        String documentName;
        Bar bar = BarUtils.createBar(remoteDb, "Bob", 12);
        Response res = couchClient.create(bar);

        documentName = res.getId();

        // create revisions to the document

        for (int i = 1; i < 11; i++) {
            bar.setRevision(res.getRev());
            bar.setAge(bar.getAge() + i);
            res = couchClient.update(bar.getId(), bar);
            Assert.assertTrue("Failure during DB creation", res.getOk());
        }

        // replicate with compacted database

        super.pull();

        // compare remote revisions with local revisions
        String dbURI = couchClient.getRootUri().toASCIIString();
        URI getURI = new URI(dbURI + "/" + documentName + "?revs_info=true");

        List<String> remoteRevs = ClientTestUtils.getRemoteRevisionIDs(getURI);
        List<String> localRevs = new ArrayList<String>();
        DocumentRevisionTree localRevsTree = datastore.getAllRevisionsOfDocument(bar.getId());

        Map<Long, DocumentRevisionTree.DocumentRevisionNode> roots = localRevsTree.roots();
        Set<Long> rootSet = roots.keySet();
        for (Long l : rootSet) {
            DocumentRevisionTree.DocumentRevisionNode node = roots.get(l);
            localRevs.add(node.getData().getRevision());
            extractRevisionIDsFromChildren(node, localRevs);

        }

        for (String rev : remoteRevs) {
            Assert.assertTrue("Remote revision missing from local replica, rev missing: " + rev,
                    localRevs.contains(rev));
        }

        //now create some local revs
        DocumentRevision revision = datastore.getDocument(documentName);

        for (int i = 0; i < 10; i++) {
            Map<String, Object> body = revision.getBody().asMap();
            Number age = (Number) body.get("age");
            age = age.intValue() + 1;
            body.put("age", age);
            revision.setBody(DocumentBodyFactory.create(body));
            revision = datastore.updateDocumentFromRevision(revision);
        }

        // push the changes to the remote
        super.push();

        //compare local revs to remote
        remoteRevs = ClientTestUtils.getRemoteRevisionIDs(getURI);
        localRevs = new ArrayList<String>();
        localRevsTree = datastore.getAllRevisionsOfDocument(bar.getId());

        roots = localRevsTree.roots();
        rootSet = roots.keySet();
        for (Long l : rootSet) {
            DocumentRevisionTree.DocumentRevisionNode node = roots.get(l);
            localRevs.add(node.getData().getRevision());
            extractRevisionIDsFromChildren(node, localRevs);

        }

        for (String rev : localRevs) {
            Assert.assertTrue("Local revision missing from remote replica, rev missing: " + rev,
                    remoteRevs.contains(rev));
        }


    }

    private void extractRevisionIDsFromChildren(AbstractTreeNode<DocumentRevision> node,
                                                List<String> documentRevisions) {

        if (node.hasChildren()) {
            Iterator<AbstractTreeNode<DocumentRevision>> iterator = node.iterateChildren();
            while (iterator.hasNext()) {
                AbstractTreeNode<DocumentRevision> absNode = iterator.next();
                documentRevisions.add(absNode.getData().getRevision());
                extractRevisionIDsFromChildren(absNode, documentRevisions);
            }
        }

    }

    @Override
    String getDbName() {
        return "dbwith%2Faslash";
    }
}
