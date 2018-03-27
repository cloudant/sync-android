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

import com.cloudant.common.CouchTestBase;
import com.cloudant.http.HttpConnectionRequestInterceptor;
import com.cloudant.http.HttpConnectionResponseInterceptor;
import com.cloudant.http.internal.interceptors.CookieInterceptor;
import com.cloudant.http.internal.interceptors.CookieInterceptorBase;
import com.cloudant.http.internal.interceptors.IamCookieInterceptor;
import com.cloudant.sync.documentstore.DocumentStore;
import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.internal.mazha.CouchClient;
import com.cloudant.sync.internal.mazha.CouchConfig;
import com.cloudant.sync.internal.mazha.CouchException;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.replication.PullFilter;
import com.cloudant.sync.replication.Replicator;
import com.cloudant.sync.replication.ReplicatorBuilder;
import com.cloudant.sync.util.TestUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
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
    protected DatabaseImpl datastore = null;
    protected DocumentStore documentStore = null;
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
        documentStore = DocumentStore.getInstance(new File(this.datastoreManagerPath, this.getClass().getSimpleName()));
        datastore = (DatabaseImpl) documentStore.database();
        datastoreWrapper = new DatastoreWrapper(datastore);
    }

    protected void createRemoteDB() throws MalformedURLException, URISyntaxException {
        couchConfig = super.getCouchConfig(getDbName());

        List<HttpConnectionResponseInterceptor> responseInterceptors = new ArrayList
                <HttpConnectionResponseInterceptor>();
        List<HttpConnectionRequestInterceptor> requestInterceptors = new ArrayList
                <HttpConnectionRequestInterceptor>();

        responseInterceptors.addAll(couchConfig.getResponseInterceptors());
        requestInterceptors.addAll(couchConfig.getRequestInterceptors());


        remoteDb = new CouchClientWrapper(new CouchClient(
                couchConfig.getRootUri(),
                requestInterceptors,
                responseInterceptors));
        try {
            remoteDb.createDatabase();
        } catch (CouchException e) {
            // Suppress 412 exceptions in the event a retry has caused us to create twice, but
            // usually throw
            if (e.getStatusCode() != 412) throw e;
        }
        couchClient = remoteDb.getCouchClient();
    }

    protected void cleanUpTempFilesAndDeleteRemote() {
        TestUtils.deleteTempTestingDir(datastoreManagerPath);
        CouchClientWrapperDbUtils.deleteDbQuietly(remoteDb);
    }

    protected String getDbName() {
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
        ReplicatorBuilder.Push push = ReplicatorBuilder.push().
                from(this.documentStore).
                to(this.couchConfig.getRootUri())
                .addRequestInterceptors(couchConfig.getRequestInterceptors(false))
                .addResponseInterceptors(couchConfig.getResponseInterceptors(false));
        if (couchConfig.getUsername() != null && couchConfig.getPassword() != null) {

            push.username(couchConfig.getUsername())
                    .password(couchConfig.getPassword());
        }

        return push;
    }

    protected ReplicatorBuilder.Pull getPullBuilder() {
        return this.getPullBuilder((PullFilter)null);

    }

    protected ReplicatorBuilder.Pull getPullBuilder(PullFilter filter) {
        ReplicatorBuilder.Pull pull = ReplicatorBuilder.pull().
                from(this.couchConfig.getRootUri()).
                to(this.documentStore)
                .filter(filter)
                .addRequestInterceptors(couchConfig.getRequestInterceptors(false))
                .addResponseInterceptors(couchConfig.getResponseInterceptors(false));
        if (couchConfig.getUsername() != null && couchConfig.getPassword() != null) {

            pull.username(couchConfig.getUsername())
                    .password(couchConfig.getPassword());
        }

        return pull;
    }

    protected ReplicatorBuilder.Pull getPullBuilder(String selector) {
        ReplicatorBuilder.Pull pull = ReplicatorBuilder.pull().
                from(this.couchConfig.getRootUri()).
                to(this.documentStore)
                .selector(selector)
                .addRequestInterceptors(couchConfig.getRequestInterceptors(false))
                .addResponseInterceptors(couchConfig.getResponseInterceptors(false));
        if (couchConfig.getUsername() != null && couchConfig.getPassword() != null) {

            pull.username(couchConfig.getUsername())
                    .password(couchConfig.getPassword());
        }

        return pull;
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

    protected PullStrategy getPullStrategy(String selector) {
        return (PullStrategy)((ReplicatorImpl)this.getPullBuilder(selector).build()).strategy;
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
        // Note this introspection happens before the interceptors are passed to the CouchClient so
        // excludes any other interceptors (e.g. UserAgentInterceptor that might be added there).
        Assert.assertEquals(1, reqIList.size());
        Assert.assertEquals(CookieInterceptor.class, reqIList.get(0).getClass());
        Assert.assertEquals(1, respIList.size());
        Assert.assertEquals(CookieInterceptor.class, respIList.get(0).getClass());
        CookieInterceptorBase ci = (CookieInterceptorBase)reqIList.get(0);
        Field srbField = CookieInterceptorBase.class.getDeclaredField("sessionRequestBody");
        srbField.setAccessible(true);
        byte[] srb = (byte[])srbField.get(ci);
        String srbString = new String(srb);
        Assert.assertEquals(expectedRequestBody, srbString);
    }

    protected void assertIamCookieInterceptorPresent(ReplicatorBuilder p)
            throws NoSuchFieldException, IllegalAccessException {
        // peek inside these private fields to see that interceptors have been set
        Field reqI = ReplicatorBuilder.class.getDeclaredField("requestInterceptors");
        Field respI = ReplicatorBuilder.class.getDeclaredField("responseInterceptors");
        reqI.setAccessible(true);
        respI.setAccessible(true);
        List<HttpConnectionRequestInterceptor> reqIList = (List)reqI.get(p);
        List<HttpConnectionRequestInterceptor> respIList = (List)respI.get(p);
        // Note this introspection happens before the interceptors are passed to the CouchClient so
        // excludes any other interceptors (e.g. UserAgentInterceptor that might be added there).
        Assert.assertEquals(1, reqIList.size());
        Assert.assertEquals(IamCookieInterceptor.class, reqIList.get(0).getClass());
        Assert.assertEquals(1, respIList.size());
        Assert.assertEquals(IamCookieInterceptor.class, respIList.get(0).getClass());
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
