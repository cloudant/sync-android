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

import com.cloudant.common.CouchTestBase;
import com.cloudant.mazha.CouchClient;
import com.cloudant.mazha.CouchConfig;
import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.TestUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.net.URISyntaxException;
import java.util.logging.Filter;

public abstract class ReplicationTestBase extends CouchTestBase {

    public String datastoreManagerPath = null;

    protected DatastoreManager datastoreManager = null;
    protected DatastoreExtended datastore = null;
    protected SQLDatabase database = null;
    protected DatastoreWrapper datastoreWrapper = null;

    protected CouchClientWrapper remoteDb = null;
    protected CouchClient couchClient = null;

    protected CouchConfig couchConfig = null;

    private long dbSuffix = System.currentTimeMillis();

    @Before
    public void setUp() throws Exception {
        this.createDatastore();
        this.createRemoteDB();
    }

    @After
    public void tearDown() throws Exception {
        datastore.close();
        TestUtils.deleteDatabaseQuietly(database);
        cleanUpTempFiles();
    }


    protected void createDatastore() throws Exception {
        datastoreManagerPath = TestUtils.createTempTestingDir(this.getClass().getName());
        datastoreManager = new DatastoreManager(this.datastoreManagerPath);
        datastore = (DatastoreExtended) datastoreManager.openDatastore(getClass().getSimpleName());
        datastoreWrapper = new DatastoreWrapper(datastore);
    }

    protected void createRemoteDB() {
        couchConfig = super.getCouchConfig(getDbName());
        remoteDb = new CouchClientWrapper(new CouchClient(
                couchConfig.getRootUri(),
                couchConfig.getRequestInterceptors(),
                couchConfig.getResponseInterceptors()));
        remoteDb.createDatabase();
        couchClient = remoteDb.getCouchClient();
    }

    protected void cleanUpTempFiles() {
        TestUtils.deleteTempTestingDir(datastoreManagerPath);
        CouchClientWrapperDbUtils.deleteDbQuietly(remoteDb);
    }

    String getDbName() {
        String dbName = getClass().getSimpleName()+ dbSuffix;
        String regex = "([a-z])([A-Z])";
        String replacement = "$1_$2";
        return dbName.replaceAll(regex, replacement).toLowerCase();
    }

    protected void runReplicationUntilComplete(Replicator replicator) throws Exception {
        TestReplicationListener listener = new TestReplicationListener();
        replicator.getEventBus().register(listener);
        replicator.start();

        while(replicator.getState() != Replicator.State.COMPLETE && replicator.getState() != Replicator.State.ERROR) {
            Thread.sleep(50);
        }

        Assert.assertEquals(Replicator.State.COMPLETE, replicator.getState());
        Assert.assertFalse(listener.errorCalled);
        Assert.assertTrue(listener.finishCalled);
    }

    protected ReplicatorBuilder.Push getPushBuilder() {
        return ReplicatorBuilder.push().
                from(this.datastore).
                to(this.couchConfig.getRootUri()).
                addRequestInterceptors(couchConfig.getRequestInterceptors()).
                addResponseInterceptors(couchConfig.getResponseInterceptors());
    }

    protected ReplicatorBuilder.Pull getPullBuilder() {
        return ReplicatorBuilder.pull().
                to(this.datastore).
                from(this.couchConfig.getRootUri()).
                addRequestInterceptors(couchConfig.getRequestInterceptors()).
                addResponseInterceptors(couchConfig.getResponseInterceptors());
    }

    protected ReplicatorBuilder.Pull getPullBuilder(PullFilter filter) {
        return ReplicatorBuilder.pull().
                to(this.datastore).
                from(this.couchConfig.getRootUri()).
                addRequestInterceptors(couchConfig.getRequestInterceptors()).
                addResponseInterceptors(couchConfig.getResponseInterceptors()).
                filter(filter);
    }

    protected BasicPushStrategy getPushStrategy() {
        return (BasicPushStrategy)((BasicReplicator)this.getPushBuilder().build()).strategy;
    }

    protected BasicPullStrategy getPullStrategy() {
        return (BasicPullStrategy)((BasicReplicator)this.getPullBuilder().build()).strategy;
    }

    protected BasicPullStrategy getPullStrategy(PullFilter filter) {
        return (BasicPullStrategy)((BasicReplicator)this.getPullBuilder(filter).build()).strategy;
    }

    protected PushResult push() throws Exception {
        TestStrategyListener listener = new TestStrategyListener();
        BasicPushStrategy replicator = this.getPushStrategy();
        replicator.getEventBus().register(listener);
        replicator.run();
        Assert.assertTrue(listener.finishCalled);
        Assert.assertFalse(listener.errorCalled);
        return new PushResult(replicator, listener);
    }

    protected PullResult pull() throws Exception {
        TestStrategyListener listener = new TestStrategyListener();
        BasicPullStrategy replicator = this.getPullStrategy();
        replicator.getEventBus().register(listener);
        replicator.run();
        Assert.assertTrue(listener.finishCalled);
        Assert.assertFalse(listener.errorCalled);
        return new PullResult(replicator, listener);
    }

    protected PullResult pull(PullFilter filter) throws Exception {
        TestStrategyListener listener = new TestStrategyListener();
        BasicPullStrategy replicator = this.getPullStrategy(filter);
        replicator.getEventBus().register(listener);
        replicator.run();
        Assert.assertTrue(listener.finishCalled);
        Assert.assertFalse(listener.errorCalled);
        return new PullResult(replicator, listener);
    }

    protected class PushResult {
        public PushResult(BasicPushStrategy pushStrategy, TestStrategyListener listener) {
            this.pushStrategy = pushStrategy;
            this.listener = listener;
        }

        BasicPushStrategy pushStrategy;
        TestStrategyListener listener;
    }

    protected class PullResult {
        public PullResult(BasicPullStrategy pullStrategy, TestStrategyListener listener) {
            this.pullStrategy = pullStrategy;
            this.listener = listener;
        }

        BasicPullStrategy pullStrategy;
        TestStrategyListener listener;
    }

}
