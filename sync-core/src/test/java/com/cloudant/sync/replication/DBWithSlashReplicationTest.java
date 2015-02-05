package com.cloudant.sync.replication;

import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.mazha.ClientTestUtils;
import com.cloudant.mazha.Response;
import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentRevisionTree;
import com.cloudant.sync.datastore.MutableDocumentRevision;
import com.cloudant.sync.util.AbstractTreeNode;

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

/**
 * Created by rhys on 28/11/14.
 */

@Category(RequireRunningCouchDB.class)
public class DBWithSlashReplicationTest extends ReplicationTestBase {

    // NB although the user has to encode the / themselves as %2F, this is still a valuable test
    // as it shows we don't double-encode eg encode %2F as %252F

    BasicReplicator replicator;

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void replicationPullAndPushDbWithSlash() throws Exception{
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

        // replicate with compacted database

        PullReplication pull = createPullReplication();
        replicator = (BasicReplicator)ReplicatorFactory.oneway(pull);

        TestReplicationListener listener = new TestReplicationListener();
        Assert.assertEquals(Replicator.State.PENDING, replicator.getState());
        replicator.getEventBus().register(listener);
        replicator.start();
        Assert.assertEquals(Replicator.State.STARTED, replicator.getState());

        while(replicator.getState() != Replicator.State.COMPLETE || replicator.getState() == Replicator.State.ERROR) {
            Thread.sleep(1000);
        }

        Assert.assertEquals(Replicator.State.COMPLETE, replicator.getState());
        Assert.assertEquals(1, datastore.getDocumentCount());

        Assert.assertTrue(listener.finishCalled);
        Assert.assertFalse(listener.errorCalled);

        // compare remote revisions with local revisions
        String dbURI =  couchClient.getRootUri().toASCIIString();
        URI getURI = new URI( dbURI + "/" + documentName + "?revs_info=true");

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

        //now create some local revs
        BasicDocumentRevision revision = datastore.getDocument(documentName);

        for(int i=0;i<10;i++){
            MutableDocumentRevision mutableDocumentRevision = revision.mutableCopy();
            Map<String,Object> body = mutableDocumentRevision.body.asMap();
            Number age = (Number)body.get("age");
            age =  age.intValue() + 1;
            body.put("age",age);
            mutableDocumentRevision.body = DocumentBodyFactory.create(body);
            revision = datastore.updateDocumentFromRevision(mutableDocumentRevision);
        }

        // push the changes to the remote
        PushReplication push = createPushReplication();
        replicator = (BasicReplicator)ReplicatorFactory.oneway(push);
        replicator.getEventBus().register(listener);
        replicator.start();
        Assert.assertEquals(Replicator.State.STARTED, replicator.getState());

        while(replicator.getState() != Replicator.State.COMPLETE){
            Thread.sleep(1000);
        }

        Assert.assertEquals(Replicator.State.COMPLETE, replicator.getState());;

        Assert.assertTrue(listener.finishCalled);
        Assert.assertFalse(listener.errorCalled);


        //compare local revs to remote
        remoteRevs = ClientTestUtils.getRemoteRevisionIDs(couchClient, getURI);
        localRevs = new ArrayList<String>();
        localRevsTree = datastore.getAllRevisionsOfDocument(bar.getId());

        roots = localRevsTree.roots();
        rootSet = roots.keySet();
        for(Long l:rootSet){
            DocumentRevisionTree.DocumentRevisionNode node = roots.get(l);
            localRevs.add(node.getData().getRevision());
            extractRevisionIDsFromChildren(node, localRevs);

        }

        for(String rev: localRevs){
            Assert.assertTrue("Local revision missing from remote replica, rev missing: " + rev,
                    remoteRevs.contains(rev));
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

    @Override
    String getDbName() {
        return "dbwith%2Faslash";
    }
}
