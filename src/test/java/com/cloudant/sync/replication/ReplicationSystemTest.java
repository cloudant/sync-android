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

import com.cloudant.common.SystemTest;
import com.cloudant.mazha.ChangesResult;
import com.cloudant.mazha.CloudantConfig;
import com.cloudant.mazha.CouchConfig;
import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.common.Log;
import com.cloudant.sync.util.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.URI;

public class ReplicationSystemTest {

    public static final String LOG_TAG = "ReplicationSystemTest";

    public static String datastoreManagerPath = null;
    public DatastoreManager manager = null;

    @Before
    public void setUp() throws IOException {
        datastoreManagerPath = TestUtils.createTempTestingDir(ReplicationSystemTest.class.getName());
        manager = new DatastoreManager(datastoreManagerPath);
    }

    @After
    public void tearDown() throws Exception {
//        DatastoreTestUtils.deleteTempTestingDir(datastoreManagerPath);
    }

    @Category(SystemTest.class)
    @Test
    public void getChangesTest() {
        String db = "registry";
        CouchClientWrapper remoteDatabase = new CouchClientWrapper(db, this.getIrisCouchConfig());
        String lastSequence = null;
        while(true) {
            ChangesResult changeFeeds = remoteDatabase.changes(lastSequence, 10000);

            Log.i(LOG_TAG, " = = = ");
            Log.i(LOG_TAG, "Last seq: " + changeFeeds.getLastSeq());
            Log.i(LOG_TAG, "Change feed size: " + changeFeeds.size());
            Log.i(LOG_TAG, " = = = ");

            lastSequence = changeFeeds.getLastSeq();
            if(changeFeeds.size() == 0) {
                break;
            }
        }
    }

    @Category(SystemTest.class)
    @Test
    public void pull_npm_from_cloudant() throws Exception {
        Log.i(LOG_TAG, "Pull npm from cloudant");
        pull("npm", "cloudant_npm", CloudantConfig.defaultConfig());
        assert_pulled("npm", "cloudant_npm", CloudantConfig.defaultConfig());
    }

    @Category(SystemTest.class)
    @Test
    public void assert_pull_npm_from_cloudant() throws Exception {
        Log.i(LOG_TAG, "Assert pull npm from cloudant");
        assert_pulled("npm", "cloudant_npm", CloudantConfig.defaultConfig());
    }


    @Category(SystemTest.class)
    @Test
    public void push_npm_to_cloudant() throws Exception {
        Log.i(LOG_TAG, "Push npm to cloudant");
        push("cloudant_npm", "npm_pushed", CloudantConfig.defaultConfig());
        assert_pushed("cloudant_npm", "npm_pushed", CloudantConfig.defaultConfig());
    }

    @Category(SystemTest.class)
    @Test
    public void assert_push_npm_to_cloudant() throws Exception {
        Log.i(LOG_TAG, "Assert push npm to cloudant");
        assert_pushed("cloudant_npm", "npm_pushed", CloudantConfig.defaultConfig());
    }

    @Category(SystemTest.class)
    @Test
    public void pull_npm_from_localCouchdb() throws Exception {
        Log.i(LOG_TAG, "Pull npm from local");
        pull("npm", "local_npm", CouchConfig.defaultConfig());
        assert_pulled("npm", "local_npm", CouchConfig.defaultConfig());
    }

    @Category(SystemTest.class)
    @Test
    public void push_npm_to_localCouchDB() throws Exception {
        Log.i(LOG_TAG, "Push npm to local");
        push("local_npm", "npm_pushed", CouchConfig.defaultConfig());
        assert_pushed("local_npm", "npm_pushed", CouchConfig.defaultConfig());
    }

    @Category(SystemTest.class)
    @Test
    public void assert_npm_pulled_correctly() throws Exception {
        Log.i(LOG_TAG, "Assert npm pulled to local correctly");
        assert_pulled("npm", "local_npm", CouchConfig.defaultConfig());
    }

    @Category(SystemTest.class)
    @Test
    public void assert_npm_pushed_correctly() throws Exception {
        Log.i(LOG_TAG, "Assert npm pushed to local correctly");
        assert_pushed("local_npm", "npm_pushed", CouchConfig.defaultConfig());
    }

    @Category(SystemTest.class)
    @Test
    public void pullReallyBigDatabase() throws Exception {
        Log.d(LOG_TAG, "pullReallyBigDatabase");
        String remoteDatabaseName = "mbta_stops";
        manager = new DatastoreManager(datastoreManagerPath);
        DatastoreExtended datastoreExtended = (DatastoreExtended) manager.openDatastore("mbta_trips");
        URI uri = CloudantConfig.defaultConfig().getURI(remoteDatabaseName);

        pull(uri, datastoreExtended);
    }

    public void pull(String pullSource, String pullTarget, CouchConfig dbConfig) throws Exception {
        URI uri = dbConfig.getURI(pullSource);
        DatastoreExtended datastoreExtended = (DatastoreExtended) manager.openDatastore(pullTarget);

        Log.i(LOG_TAG, "Pulling ...");
        pull(uri, datastoreExtended);
        Log.i(LOG_TAG, "Pull done!");
    }

    public void assert_pulled(String pullSource, String pullTarget, CouchConfig dbConfig) throws Exception {
        CouchClientWrapper remoteDatabase = new CouchClientWrapper(pullSource, dbConfig);
        DatastoreExtended datastoreExtended = (DatastoreExtended) manager.openDatastore(pullTarget);
        DatabaseAssert.assertPulled(remoteDatabase.getCouchClient(), datastoreExtended);
        Log.i(LOG_TAG, "Assert done!");
    }

    public void push(String pushSource, String pushTarget, CouchConfig dbConfig) throws Exception {
        DatastoreExtended datastoreExtended = (DatastoreExtended) manager.openDatastore(pushSource);
        URI uri = dbConfig.getURI(pushTarget);

        Log.i(LOG_TAG, "Pushing ...");
        push(datastoreExtended, uri);
        Log.i(LOG_TAG, "Push done!");
    }

    public void assert_pushed(String pushSource, String pushTarget, CouchConfig dbConfig) throws Exception {
        DatastoreExtended datastoreExtended = (DatastoreExtended) manager.openDatastore(pushSource);
        CouchClientWrapper remoteDatabase = new CouchClientWrapper(pushTarget, dbConfig);
        DatabaseAssert.assertPushed(datastoreExtended, remoteDatabase.getCouchClient());
        Log.i(LOG_TAG, "Assert done!");
    }

    private void pull(URI uri, DatastoreExtended datastoreExtended) throws
            InterruptedException {
        TestReplicationListener listener = new TestReplicationListener();
        Replicator pullReplicator = ReplicatorFactory.oneway(uri, datastoreExtended);
        pullReplicator.getEventBus().register(listener);
        pullReplicator.start();

        while(pullReplicator.getState() != Replicator.State.COMPLETE) {
            Thread.sleep(1000);
        }
        Assert.assertTrue(listener.finishCalled);
        Assert.assertFalse(listener.errorCalled);
    }

    private void push(DatastoreExtended datastoreExtended, URI uri) throws
            InterruptedException {
        TestReplicationListener listener = new TestReplicationListener();
        Replicator pushReplicator = ReplicatorFactory.oneway(datastoreExtended, uri);
        pushReplicator.getEventBus().register(listener);
        pushReplicator.start();

        while(pushReplicator.getState() != Replicator.State.COMPLETE) {
            Thread.sleep(1000);
        }
        Assert.assertTrue(listener.finishCalled);
        Assert.assertFalse(listener.errorCalled);
    }

    public CouchConfig getIrisCouchConfig() {
        return new CouchConfig("http", "isaacs.iriscouch.com", 80, "", "");
    }

}