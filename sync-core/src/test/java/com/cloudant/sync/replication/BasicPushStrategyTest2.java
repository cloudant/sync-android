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

package com.cloudant.sync.replication;

import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.mazha.Response;
import com.cloudant.sync.datastore.ConflictException;
import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentRevisionTree;
import com.cloudant.sync.datastore.MutableDocumentRevision;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Category(RequireRunningCouchDB.class)
public class BasicPushStrategyTest2 extends ReplicationTestBase {

    String id1;
    String id2;
    String id3;

    /**
     * After all documents are created:
     *
     * Doc 1: 1 -> 2 -> 3
     * Doc 2: 1 -> 2
     * Doc 3: 1 -> 2 -> 3
     */
    private void populateSomeDataInLocalDatastore() throws ConflictException, IOException {

        id1 = createDocInDatastore("Doc 1");
        Assert.assertNotNull(id1);
        updateDocInDatastore(id1, "Doc 1");
        updateDocInDatastore(id1, "Doc 1");

        id2 = createDocInDatastore("Doc 2");
        Assert.assertNotNull(id2);
        updateDocInDatastore(id2, "Doc 2");

        id3 = createDocInDatastore("Doc 3");
        Assert.assertNotNull(id3);
        updateDocInDatastore(id3, "Doc 3");
        updateDocInDatastore(id3, "Doc 3");
    }

    public String createDocInDatastore(String d) throws IOException {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        Map<String, String> m = new HashMap<String, String>();
        m.put("data", d);
        rev.body = DocumentBodyFactory.create(m);
        return datastore.createDocumentFromRevision(rev).getId();
    }

    public String updateDocInDatastore(String id, String data) throws ConflictException, IOException {
        MutableDocumentRevision rev = datastore.getDocument(id).mutableCopy();
        Map<String, String> m = new HashMap<String, String>();
        m.put("data", data);
        rev.body = DocumentBodyFactory.create(m);
        return datastore.updateDocumentFromRevision(rev).getId();
    }

    @Test
    public void replicate_fullTest() throws Exception {

        BasicPushStrategy push = push();
        BasicPullStrategy pull = pull();
        
        populateSomeDataInLocalDatastore();
        waitForPushToFinish(push);
        Assert.assertEquals("8", remoteDb.getCheckpoint(push.getReplicationId()));

        updateDataInLocalDatastore();
        updateDataInRemoteDatabase();

        waitForPushToFinish(push);
        Assert.assertEquals("12", remoteDb.getCheckpoint(push.getReplicationId()));

        waitForPullToFinish(pull);

        // After sync, all doc should be following:

        // Doc 1: 1 -> 2 -> 3 -> 4L ->5L
        //                   \-> 4R
        //
        // Doc 2: 1 -> 2 -> 3L
        //               \-> 3R
        //
        // Doc 3: 1 -> 2 -> 3 -> 4L
        //                   \-> 4R -> 5R

        checkAllDocumentAreSynced();

        {
            DocumentRevisionTree t1 = datastore.getAllRevisionsOfDocument(id1);
            Assert.assertEquals(2, t1.leafs().size());
            BasicDocumentRevision c = t1.getCurrentRevision();
            Assert.assertEquals(4, t1.depth(c.getSequence()));
        }

        {
            DocumentRevisionTree t2 = datastore.getAllRevisionsOfDocument(id2);
            Assert.assertEquals(2, t2.leafs().size());
            BasicDocumentRevision c = t2.getCurrentRevision();
            Assert.assertEquals(2, t2.depth(c.getSequence()));
        }

        {
            DocumentRevisionTree t3 = datastore.getAllRevisionsOfDocument(id3);
            Assert.assertEquals(2, t3.leafs().size());
            BasicDocumentRevision c = t3.getCurrentRevision();
            Assert.assertEquals(4, t3.depth(c.getSequence()));
        }
    }

    private void checkAllDocumentAreSynced() {
        checkDocumentIsSynced(id1);
        checkDocumentIsSynced(id2);
        checkDocumentIsSynced(id3);
    }

    public void checkDocumentIsSynced(String id) {
        BasicDocumentRevision fooLocal = this.datastore.getDocument(id);
        Map fooRemote = remoteDb.get(Map.class, id);
        Assert.assertEquals(fooLocal.getId(), fooRemote.get("_id"));
        Assert.assertEquals(fooLocal.getRevision(), fooRemote.get("_rev"));
    }

    /**
     * After all documents are updated:
     *
     * Doc 1: 1 -> 2 -> 3 -> 4L -> 5L
     * Doc 2: 1 -> 2 -> 3L
     * Doc 3: 1 -> 2 -> 3 -> 4L
     *
     * where "L" means local version.
     */
    private void updateDataInLocalDatastore() throws ConflictException, IOException {
        // Doc 1: 1 -> 2 -> 3 -> 4L -> 5L
        updateDocInDatastore(id1, "Doc 1");
        updateDocInDatastore(id1, "Doc 1");

        //
        updateDocInDatastore(id2, "Doc 2");

        // Doc 3: 1 -> 2 -> 3 -> 4L
        updateDocInDatastore(id3, "Doc 3");
    }

    /**
     * After all documents are updated:
     *
     * Doc 1: 1 -> 2 -> 3 -> 4R
     * Doc 2: 1 -> 2 -> 3R
     * Doc 3: 1 -> 2 -> 3 -> 4R -> 5R
     *
     * where "R" means local version.
     */
    private void updateDataInRemoteDatabase() {
        updateDocInRemoteDatabase(id1, "Doc 1");

        updateDocInRemoteDatabase(id2, "Doc 2");

        updateDocInRemoteDatabase(id3, "Doc 3");
        updateDocInRemoteDatabase(id3, "Doc 3");
    }

    @SuppressWarnings("unchecked")
    private void updateDocInRemoteDatabase(String id, String data) {
        Map<String, Object> foo1 = (Map<String, Object>)remoteDb.get(Map.class, id);
        foo1.put("foo", data + " (from remoteDb)");
        Response response = remoteDb.update(id, foo1);
        Assert.assertNotNull(response);
    }

    private BasicPushStrategy push() throws Exception {
        return new BasicPushStrategy(this.createPushReplication());
    }

    private void waitForPushToFinish(BasicPushStrategy push) throws Exception{
        TestStrategyListener listener = new TestStrategyListener();
        push.eventBus.register(listener);
        Thread t = new Thread(push);
        t.start();
        t.join();
        Assert.assertTrue(listener.finishCalled);
        Assert.assertFalse(listener.errorCalled);
        push.eventBus.unregister(listener);
    }

    private BasicPullStrategy pull() throws Exception {
        return new BasicPullStrategy(this.createPullReplication());
    }

    private void waitForPullToFinish(BasicPullStrategy pull) throws Exception {
        TestStrategyListener listener = new TestStrategyListener();
        pull.getEventBus().register(listener);
        Thread t = new Thread(pull);
        t.start();
        t.join();
        Assert.assertTrue(listener.finishCalled);
        Assert.assertFalse(listener.errorCalled);
        pull.getEventBus().unregister(listener);
    }

    }
