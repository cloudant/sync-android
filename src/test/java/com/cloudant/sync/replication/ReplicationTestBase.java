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

import com.cloudant.mazha.CouchClient;
import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.sqlite.sqlite4java.SQLiteWrapper;
import com.cloudant.sync.util.TestUtils;
import org.junit.After;
import org.junit.Before;

import java.net.URI;
import java.net.URISyntaxException;

public abstract class ReplicationTestBase extends CouchTestBase {

    public String datastoreManagerPath = null;

    DatastoreManager datastoreManager = null;
    DatastoreExtended datastore = null;
    SQLiteWrapper database = null;
    DatastoreWrapper datastoreWrapper = null;

    CouchClientWrapper remoteDb = null;
    CouchClient couchClient = null;

    @Before
    public void setUp() throws Exception {
        this.createDatastore();
        this.createRemoteDB();
    }

    @After
    public void tearDown() throws Exception {
        cleanUpTempFiles();
    }


    private void createDatastore() {
        datastoreManagerPath = TestUtils.createTempTestingDir(this.getClass().getName());
        datastoreManager = new DatastoreManager(this.datastoreManagerPath);
        datastore = (DatastoreExtended) datastoreManager.openDatastore(getClass().getSimpleName());
        database = (SQLiteWrapper) datastore.getSQLDatabase();
        datastoreWrapper = new DatastoreWrapper(datastore);
    }

    private void createRemoteDB() {
        remoteDb = new CouchClientWrapper(getDbName(), super.getCouchConfig());
        remoteDb.createDatabase();
        couchClient = remoteDb.getCouchClient();
    }

    private void cleanUpTempFiles() {
        TestUtils.deleteTempTestingDir(datastoreManagerPath);
        CouchClientWrapperDbUtils.deleteDbQuietly(remoteDb);
    }

    String getDbName() {
        String dbName = getClass().getSimpleName();
        String regex = "([a-z])([A-Z])";
        String replacement = "$1_$2";
        return dbName.replaceAll(regex, replacement).toLowerCase();
    }

    public URI getURI() throws URISyntaxException {
        return this.getCouchConfig().getURI(getDbName());
    }

    PullReplication createPullReplication() throws URISyntaxException {
        PullReplication pullReplication = new PullReplication();
        pullReplication.source = this.getURI();
        pullReplication.target = this.datastore;
        return pullReplication;
    }

    PushReplication createPushReplication() throws URISyntaxException {
        PushReplication pushReplication = new PushReplication();
        pushReplication.target = this.getURI();
        pushReplication.source = this.datastore;
        return pushReplication;
    }
}
