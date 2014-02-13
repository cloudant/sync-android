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
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.common.Log;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Category(RequireRunningCouchDB.class)
public class BasicPullStrategyTest2 extends ReplicationTestBase {

    public static final String LOG_TAG = "ReplicationSystemTest";

    DocumentCache cache ;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        cache = new DocumentCache();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void replicate_fullTest() throws Exception {

        createBatchOneTestData();
        sync();
        assertDataSynced();

        createBatchTwoTestData();
        sync();
        assertDataSynced();

        deleteSomeData();
        sync();
        assertDataDeleted();
    }

    /**
     * DocumentRevisionTree tree should look like this after the call:
     *
     * Doc 1: 1 -> 2 - 3
     *
     * Doc 2: 1
     *
     * Doc 3: 1 -> 2
     *
     * Doc 4: 1
     */
    private void createBatchTwoTestData() {
        // update an existing doc
        Bar bar1 = remoteDb.get(Bar.class, cache.getAllDocumentIds().get(0));
        updateBarInSourceDb(bar1, "hoho");

        // create some new data
        createBatchOneTestData();
    }

    private void assertDataDeleted() {
        for(String id : cache.getAllDeletedIds()) {
            Log.i(LOG_TAG, "Deleted id: " + id);
            DocumentRevision obj = datastore.getDocument(id);
            Log.i(LOG_TAG, "Deleted: " + obj.isDeleted());
            Assert.assertTrue(obj.isDeleted());
        }
    }

    private void assertDataSynced() {
        for(String id : cache.getAllDocumentIds()) {
            Bar bar = cache.getData(id);
            Log.i(LOG_TAG, "Id: " + bar);
            DocumentRevision obj = datastore.getDocument(bar.getId(), bar.getRevision());
            Assert.assertNotNull(obj);
        }
    }

    private void sync() throws Exception {
        TestStrategyListener listener = new TestStrategyListener();
        BasicPullStrategy replicator = new BasicPullStrategy(remoteDb, datastore,
                null, "name");
        replicator.getEventBus().register(listener);

        Executors.newSingleThreadExecutor().submit(replicator).get();
        Assert.assertTrue(listener.finishCalled);
        Assert.assertFalse(listener.errorCalled);
    }

    /**
     * DocumentRevisionTree tree should look like this after the call:
     *
     * Doc 1: 1 -> 2 -> 3 -> 4 (deleted)
     *
     * Doc 2: 1 -> 2 (deleted)
     *
     * Doc 3: 1 -> 2
     *
     * Doc 4: 1
     */
    private void deleteSomeData() {
        deleteBarInSourceDb(cache.getAllDocumentIds().get(0));
        deleteBarInSourceDb(cache.getAllDocumentIds().get(1));
    }

    public void deleteBarInSourceDb(String id) {
        Bar bar = remoteDb.get(Bar.class, id);
        Response r1 = remoteDb.delete(bar.getId(), bar.getRevision());
        Assert.assertTrue(r1.getRev().compareTo(bar.getRevision()) > 0);
        cache.docDeleted(bar);
    }

    /**
     * DocumentRevisionTree tree should look like this after the call:
     *
     * Doc 1: 1 -> 2
     *
     * Doc 2: 1
     */
    private void createBatchOneTestData() {
        Bar bar1 = createdBarInSourceDb("haha", 10);
        updateBarInSourceDb(bar1, "hihi");
        createdBarInSourceDb("hehe", 20);
    }

    private Bar createdBarInSourceDb(String name, int age) {
        Bar bar = new Bar();
        bar.setName(name);
        bar.setAge(age);

        Response res = remoteDb.create(bar);
        bar = remoteDb.get(Bar.class, res.getId());
        cache.docUpserted(bar);
        return bar;
    }

    private Bar updateBarInSourceDb(Bar bar, String anotherName) {
        bar.setName(anotherName);
        Response res = remoteDb.update(bar.getId(), bar);
        bar = remoteDb.get(Bar.class, res.getId());
        cache.docUpserted(bar);
        return bar;
    }


    public class DocumentCache {

        private Map<String, Bar> cache;
        private List<String> data;
        private List<String> deleted ;

        public DocumentCache() {
            cache = new HashMap<String, Bar>();
            data = new ArrayList<String>();
            deleted = new ArrayList<String>();
        }

        public void docUpserted(Bar bar) {
            if(!data.contains(bar.getId())) {
                data.add(bar.getId());
            }
            cache.put(bar.getId(), bar);
        }

        public void docDeleted(Bar bar) {
            if(!deleted.contains(bar.getId())) {
                deleted.add(bar.getId());
            }
            deleted.add(bar.getId());
            cache.put(bar.getId(), bar);
        }

        public List<String> getAllDocumentIds() {
            return data;
        }

        public List<String> getAllDeletedIds() {
            return deleted;
        }

        public Bar getData(String id) {
            return cache.get(id);
        }

        public List<String> getAllLiveDocumentIds() {
            List<String> l = new ArrayList<String>();
            for(String id : data) {
                if(!deleted.contains(id)) {
                    l.add(id);
                }
            }
            return l;
        }

    }
}
