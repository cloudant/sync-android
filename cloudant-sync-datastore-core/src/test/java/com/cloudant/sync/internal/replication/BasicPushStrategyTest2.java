/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
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
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.internal.documentstore.InternalDocumentRevision;
import com.cloudant.sync.internal.mazha.Response;
import com.cloudant.sync.documentstore.DocumentBodyFactory;
import com.cloudant.sync.internal.documentstore.DocumentRevisionTree;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
    private void populateSomeDataInLocalDatastore() throws Exception{

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

    public String createDocInDatastore(String d) throws Exception {
        DocumentRevision rev = new DocumentRevision();
        Map<String, String> m = new HashMap<String, String>();
        m.put("data", d);
        rev.setBody(DocumentBodyFactory.create(m));
        return datastore.create(rev).getId();
    }

    public String updateDocInDatastore(String id, String data) throws Exception {
        DocumentRevision rev = datastore.read(id);
        Map<String, String> m = new HashMap<String, String>();
        m.put("data", data);
        rev.setBody(DocumentBodyFactory.create(m));
        return datastore.update(rev).getId();
    }

    // check that we can correctly push after compaction
    @Test
    public void replicate_compactedTest() throws Exception {

        populateSomeDataInLocalDatastore();
        super.push();

        datastore.delete(id1);
        datastore.compact();

        super.push();
    }

    @Test
    public void replicate_fullTest() throws Exception {


        populateSomeDataInLocalDatastore();
        PushResult result = super.push();
        Assert.assertEquals("8", remoteDb.getCheckpoint(result.pushStrategy.getReplicationId()));

        updateDataInLocalDatastore();
        updateDataInRemoteDatabase();

        PushResult result2 = super.push();
        Assert.assertEquals("12", remoteDb.getCheckpoint(result2.pushStrategy.getReplicationId()));

        super.pull();

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
            DocumentRevision c = t1.getCurrentRevision();
            Assert.assertEquals(4, t1.depth(((InternalDocumentRevision)c).getSequence()));
        }

        {
            DocumentRevisionTree t2 = datastore.getAllRevisionsOfDocument(id2);
            Assert.assertEquals(2, t2.leafs().size());
            DocumentRevision c = t2.getCurrentRevision();
            Assert.assertEquals(2, t2.depth(((InternalDocumentRevision)c).getSequence()));
        }

        {
            DocumentRevisionTree t3 = datastore.getAllRevisionsOfDocument(id3);
            Assert.assertEquals(2, t3.leafs().size());
            DocumentRevision c = t3.getCurrentRevision();
            Assert.assertEquals(4, t3.depth(((InternalDocumentRevision)c).getSequence()));
        }
    }

    private void checkAllDocumentAreSynced() throws Exception {
        checkDocumentIsSynced(id1);
        checkDocumentIsSynced(id2);
        checkDocumentIsSynced(id3);
    }

    public void checkDocumentIsSynced(String id) throws Exception{
        DocumentRevision fooLocal = this.datastore.read(id);
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
    private void updateDataInLocalDatastore() throws Exception {
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

    private void waitForPushToFinish(PushStrategy push) throws Exception{
        TestStrategyListener listener = new TestStrategyListener();
        push.eventBus.register(listener);
        Thread t = new Thread(push);
        t.start();
        t.join();
        listener.assertReplicationCompletedOrThrow();
        push.eventBus.unregister(listener);
    }

    private void waitForPullToFinish(PullStrategy pull) throws Exception {
        TestStrategyListener listener = new TestStrategyListener();
        pull.getEventBus().register(listener);
        Thread t = new Thread(pull);
        t.start();
        t.join();
        listener.assertReplicationCompletedOrThrow();
        pull.getEventBus().unregister(listener);
    }

    }
