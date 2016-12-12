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
import com.cloudant.http.HttpConnectionRequestInterceptor;
import com.cloudant.http.interceptors.CookieInterceptor;
import com.cloudant.mazha.CouchClient;
import com.cloudant.mazha.CouchConfig;
import com.cloudant.sync.datastore.DatastoreImpl;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.TestUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.util.List;

public abstract class ReplicationTestBase extends CouchTestBase {

    // A String of all the URI reserved and "unsafe" characters, plus © and an emoji. Encodes to:
    // %21*%27%28%29%3B%3A%40%26%3D%2B%24%2C%2F%3F%23%5B%5D+%22%25
    // -.%3C%3E%5C%5E_%60%7B%7C%7D%7E%C2%A9%F0%9F%94%92
    protected static final String PERCENT_ENCODED_URI_CHARS;

    static {
        try {
            PERCENT_ENCODED_URI_CHARS = URLEncoder.encode("!*'();:@&=+$,/?#[] \"%-" +
                    ".<>\\^_`{|}~©\uD83D\uDD12", "UTF-8");

        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public String datastoreManagerPath = null;

    protected DatastoreManager datastoreManager = null;
    protected DatastoreImpl datastore = null;
    protected SQLDatabase database = null;
    protected DatastoreWrapper datastoreWrapper = null;

    protected CouchClientWrapper remoteDb = null;
    protected CouchClient couchClient = null;

    protected CouchConfig couchConfig = null;

    private String dbSuffix;

    @Before
    public void setUp() throws Exception {
        // Use a unique suffix for each test
        dbSuffix = System.currentTimeMillis() + "" + System.nanoTime();
        this.createDatastore();
        this.createRemoteDB();
    }

    @After
    public void tearDown() throws Exception {
        datastore.close();
        TestUtils.deleteDatabaseQuietly(database);
        cleanUpTempFilesAndDeleteRemote();
    }


    protected void createDatastore() throws Exception {
        datastoreManagerPath = TestUtils.createTempTestingDir(this.getClass().getName());
        datastoreManager = DatastoreManager.getInstance(this.datastoreManagerPath);
        datastore = (DatastoreImpl) datastoreManager.openDatastore(getClass().getSimpleName());
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

    protected void cleanUpTempFilesAndDeleteRemote() {
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
        listener.assertReplicationCompletedOrThrow();
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

    protected PushStrategy getPushStrategy() {
        return (PushStrategy)((ReplicatorImpl)this.getPushBuilder().build()).strategy;
    }

    protected PullStrategy getPullStrategy() {
        return (PullStrategy)((ReplicatorImpl)this.getPullBuilder().build()).strategy;
    }

    protected PullStrategy getPullStrategy(PullFilter filter) {
        return (PullStrategy)((ReplicatorImpl)this.getPullBuilder(filter).build()).strategy;
    }

    protected PushResult push() throws Exception {
        TestStrategyListener listener = new TestStrategyListener();
        PushStrategy replicator = this.getPushStrategy();
        replicator.getEventBus().register(listener);
        replicator.run();
        listener.assertReplicationCompletedOrThrow();
        return new PushResult(replicator, listener);
    }

    protected PullResult pull() throws Exception {
        TestStrategyListener listener = new TestStrategyListener();
        PullStrategy replicator = this.getPullStrategy();
        replicator.getEventBus().register(listener);
        replicator.run();
        listener.assertReplicationCompletedOrThrow();
        return new PullResult(replicator, listener);
    }

    protected PullResult pull(PullFilter filter) throws Exception {
        TestStrategyListener listener = new TestStrategyListener();
        PullStrategy replicator = this.getPullStrategy(filter);
        replicator.getEventBus().register(listener);
        replicator.run();
        listener.assertReplicationCompletedOrThrow();
        return new PullResult(replicator, listener);
    }

    protected void assertCookieInterceptorPresent(ReplicatorBuilder p, String expectedRequestBody)
            throws NoSuchFieldException, IllegalAccessException {
        // peek inside these private fields to see that interceptors have been set
        Field reqI = ReplicatorBuilder.class.getDeclaredField("requestInterceptors");
        Field respI = ReplicatorBuilder.class.getDeclaredField("responseInterceptors");
        reqI.setAccessible(true);
        respI.setAccessible(true);
        List<HttpConnectionRequestInterceptor> reqIList = (List)reqI.get(p);
        List<HttpConnectionRequestInterceptor> respIList = (List)respI.get(p);
        // This relies on the UserAgentInterceptor being added before the CookieInterceptor.
        Assert.assertEquals(2, reqIList.size());
        Assert.assertEquals(CookieInterceptor.class, reqIList.get(1).getClass());
        Assert.assertEquals(1, respIList.size());
        Assert.assertEquals(CookieInterceptor.class, respIList.get(0).getClass());
        CookieInterceptor ci = (CookieInterceptor)reqIList.get(1);
        Field srbField = CookieInterceptor.class.getDeclaredField("sessionRequestBody");
        srbField.setAccessible(true);
        byte[] srb = (byte[])srbField.get(ci);
        String srbString = new String(srb);
        Assert.assertEquals(expectedRequestBody, srbString);
    }

    protected class PushResult {
        public PushResult(PushStrategy pushStrategy, TestStrategyListener listener) {
            this.pushStrategy = pushStrategy;
            this.listener = listener;
        }

        PushStrategy pushStrategy;
        TestStrategyListener listener;
    }

    protected class PullResult {
        public PullResult(PullStrategy pullStrategy, TestStrategyListener listener) {
            this.pullStrategy = pullStrategy;
            this.listener = listener;
        }

        PullStrategy pullStrategy;
        TestStrategyListener listener;
    }
}
