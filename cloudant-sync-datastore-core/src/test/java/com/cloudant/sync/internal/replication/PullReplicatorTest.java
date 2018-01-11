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
import com.cloudant.common.TestOptions;
import com.cloudant.http.internal.interceptors.CookieInterceptor;
import com.cloudant.sync.replication.PullFilter;
import com.cloudant.sync.replication.PullSelector;
import com.cloudant.sync.replication.Replicator;
import com.cloudant.sync.replication.ReplicatorBuilder;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

@Category(RequireRunningCouchDB.class)
public class PullReplicatorTest extends ReplicationTestBase {

    URI source;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        source = getCouchConfig(getDbName()).getRootUri();

        prepareTwoDocumentsInRemoteDB();
    }

    private void prepareTwoDocumentsInRemoteDB() {
        Bar bar1 = BarUtils.createBar(remoteDb, "Tom", 31);
        couchClient.create(bar1);
        Bar bar2 = BarUtils.createBar(remoteDb, "Jerry", 52);
        couchClient.create(bar2);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void start_StartedThenComplete() throws Exception {
        Replicator replicator = super.getPullBuilder().build();

        TestReplicationListener listener = new TestReplicationListener();
        Assert.assertEquals(Replicator.State.PENDING, replicator.getState());
        replicator.getEventBus().register(listener);
        replicator.start();
        Assert.assertEquals(Replicator.State.STARTED, replicator.getState());

        while(replicator.getState() != Replicator.State.COMPLETE && replicator.getState() != Replicator.State.ERROR) {
            Thread.sleep(1000);
        }

        Assert.assertEquals(Replicator.State.COMPLETE, replicator.getState());
        Assert.assertEquals(2, datastore.getDocumentCount());

        listener.assertReplicationCompletedOrThrow();
    }

    @Test
    public void testRequestInterceptors() throws Exception {

        InterceptorCallCounter interceptorCallCounter = new InterceptorCallCounter();
        Replicator replicator = super.getPullBuilder()
                .addRequestInterceptors(interceptorCallCounter).build();

        runReplicationUntilComplete(replicator);
        Assert.assertTrue(interceptorCallCounter.interceptorRequestTimesCalled >= 1);

    }

    @Test
    public void testResponseInterceptors() throws Exception {

        InterceptorCallCounter interceptorCallCounter = new InterceptorCallCounter();
        Replicator replicator = super.getPullBuilder()
                .addResponseInterceptors(interceptorCallCounter).build();

        runReplicationUntilComplete(replicator);
        Assert.assertTrue(interceptorCallCounter.interceptorResponseTimesCalled >= 1);
    }

    @Test
    public void testPullReplicationCreatedSuccessfullyWithoutFilter() throws Exception {

        Replicator replicator = ReplicatorBuilder.pull()
                .from(this.source)
                .to(this.documentStore)
                .build();

        Assert.assertNotNull(replicator);
    }

    @Test
    public void testPullReplicationCreatedSuccessfullyWithSelector() throws Exception {

        Replicator replicator = ReplicatorBuilder.pull()
                .from(this.source)
                .to(this.documentStore)
                .selector(new PullSelector("{\"selector\":{\"class\":\"a_class\"}}"))
                .build();

        Assert.assertNotNull(replicator);
    }

    @Test
    public void testPullReplicationCreatedSuccessfullyWithFilter() throws Exception {

        Replicator replicator = ReplicatorBuilder.pull()
                .from(this.source)
                .to(this.documentStore)
                .filter(new PullFilter("a_filter"))
                .build();

        Assert.assertNotNull(replicator);
    }

    @Test
    public void testPullReplicationCreatedSuccessfullyWithFilterAndParams() throws Exception {

        Map<String,String> params = new HashMap<String,String>();
        params.put("a","parameter");
        Replicator replicator = ReplicatorBuilder.pull()
                .from(this.source)
                .to(this.documentStore)
                .filter(new PullFilter("a_filter",params))
                .build();

        Assert.assertNotNull(replicator);
    }

    @Test
    public void testPullReplicationCreatedSuccessfullyWithFilterAndEmptyParams() throws Exception {

        Map<String,String> params = new HashMap<String,String>();
        Replicator replicator = ReplicatorBuilder.pull()
                .from(this.source)
                .to(this.documentStore)
                .filter(new PullFilter("a_filter",params))
                .build();

        Assert.assertNotNull(replicator);
    }

    @Test
    public void testRequestInterceptorsThroughBuilder() throws Exception {

        InterceptorCallCounter interceptorCallCounter = new InterceptorCallCounter();

        TestReplicationListener listener = new TestReplicationListener();
        ReplicatorBuilder replicatorBuilder = ReplicatorBuilder.pull()
                .to(this.documentStore)
                .from(this.remoteDb.couchClient.getRootUri())
                .addRequestInterceptors(interceptorCallCounter)
                .addRequestInterceptors(couchConfig.getRequestInterceptors())
                .addResponseInterceptors(interceptorCallCounter)
                .addResponseInterceptors(couchConfig.getResponseInterceptors());
        Replicator replicator = replicatorBuilder.build();

        replicator.getEventBus().register(listener);
        replicator.start();

        while(replicator.getState() != Replicator.State.COMPLETE && replicator.getState() != Replicator.State.ERROR) {
            Thread.sleep(50);
        }

        Assert.assertEquals(Replicator.State.COMPLETE, replicator.getState());
        listener.assertReplicationCompletedOrThrow();

        //check that the response and request interceptors have been called.
        Assert.assertTrue(interceptorCallCounter.interceptorResponseTimesCalled >= 1);
        Assert.assertTrue(interceptorCallCounter.interceptorRequestTimesCalled >= 1);

    }

    @Test
    public void replicatorCanBeReused() throws Exception {
        ReplicatorBuilder replicatorBuilder = super.getPullBuilder();
        Replicator replicator = replicatorBuilder.build();
        ReplicationStrategy replicationStrategy = ((ReplicatorImpl)replicator).strategy;
        replicator.start();
        // replicate 2 docs created at test setup
        while(replicator.getState() != Replicator.State.COMPLETE && replicator.getState() != Replicator.State.ERROR) {
            Thread.sleep(50);
        }
        // check document counter has been incremented
        Assert.assertEquals(2, replicationStrategy.getDocumentCounter());
        Bar bar3 = BarUtils.createBar(remoteDb, "Test", 52);
        couchClient.create(bar3);
        replicator.start();
        ReplicationStrategy replicationStrategy2 = ((ReplicatorImpl)replicator).strategy;
        // replicate 3rd doc
        while(replicator.getState() != Replicator.State.COMPLETE && replicator.getState() != Replicator.State.ERROR) {
            Thread.sleep(50);
        }
        // check document counter has been reset since last replication and incremented
        Assert.assertEquals(1, replicationStrategy2.getDocumentCounter());
        Assert.assertEquals(3, this.datastore.getDocumentCount());
    }

    @Test
    public void replicatorBuilderAddsCookieInterceptorCustomPort() throws Exception {
        ReplicatorBuilder.Pull p = ReplicatorBuilder.pull().
                from(new URI("http://🍶:🍶@some-host:123/path%2Fsome-path-日本")).
                to(documentStore);
        ReplicatorImpl r = (ReplicatorImpl)p.build();
        // check that user/pass has been removed
        Assert.assertEquals("http://some-host:123/path%2Fsome-path-日本",
                (((CouchClientWrapper)(((PullStrategy)r.strategy).sourceDb)).
                        getCouchClient().
                        getRootUri()).
                        toString()
                );
        assertCookieInterceptorPresent(p, "name=%F0%9F%8D%B6&password=%F0%9F%8D%B6");
    }

    @Test
    public void testCredsAPIOverridesURL() throws Exception {
        ReplicatorBuilder.Pull pull =  ReplicatorBuilder.pull().to(documentStore)
                .from(new URI("http://example:password@example.invalid"))
                .username("user")
                .password("examplePass");
        ReplicatorImpl replicator = (ReplicatorImpl) pull.build();

        assertCookieInterceptorPresent(pull, "name=user&password=examplePass");
    }

    @Test
    public void testCredsAPIOverridesURLWithPath() throws Exception {
        ReplicatorBuilder.Pull pull =  ReplicatorBuilder.pull().to(documentStore)
                .from(new URI("http://example:password@example.invalid/proxy"))
                .username("user")
                .password("examplePass");
        ReplicatorImpl replicator = (ReplicatorImpl) pull.build();

        assertCookieInterceptorPresent(pull, "name=user&password=examplePass");
    }

    @Test
    public void replicatorBuilderAddsCookieInterceptorDefaultPort() throws Exception {
        ReplicatorBuilder.Pull p = ReplicatorBuilder.pull().
                from(new URI("http://🍶:🍶@some-host/path%2Fsome-path-日本")).
                to(documentStore);
        ReplicatorImpl r = (ReplicatorImpl)p.build();
        // check that user/pass has been removed
        Assert.assertEquals("http://some-host:80/path%2Fsome-path-日本",
                (((CouchClientWrapper) (((PullStrategy) r.strategy).sourceDb)).
                        getCouchClient().
                        getRootUri()).
                        toString()
        );
       assertCookieInterceptorPresent(p, "name=%F0%9F%8D%B6&password=%F0%9F%8D%B6");
    }

    /**
     * Test that a username and password combination where both parts contain a series of URI
     * reserved and other percent encoded characters is correctly encoded and not double encoded
     * after going through the ReplicatorBuilder and CookieInterceptor.
     *
     * @throws Exception
     */
    @Test
    public void replicatorBuilderAddsCookieInterceptorCredsPercentEncoded() throws Exception {
        String encodedUsername = "user" + PERCENT_ENCODED_URI_CHARS;
        String encodedPassword = "password" + PERCENT_ENCODED_URI_CHARS;
        ReplicatorBuilder.Pull p = ReplicatorBuilder.pull().
                from(new URI("http://" + encodedUsername + ":" + encodedPassword +
                        "@some-host/path%2Fsome-path-日本")).
                to(documentStore);
        ReplicatorImpl r = (ReplicatorImpl) p.build();
        // check that user/pass has been removed
        Assert.assertEquals("http://some-host:80/path%2Fsome-path-日本",
                (((CouchClientWrapper) (((PullStrategy) r.strategy).sourceDb)).
                        getCouchClient().
                        getRootUri()).
                        toString()
        );
        assertCookieInterceptorPresent(p, "name=" + encodedUsername + "&password=" +
                encodedPassword);
    }
    
    @Test(expected = IllegalStateException.class)
    public void replicatorBuilderNoSource() {
        ReplicatorBuilder.Pull p = ReplicatorBuilder.pull().
                from(null).
                to(documentStore);
        p.build();
    }

    @Test(expected = IllegalStateException.class)
    public void replicatorBuilderNoTarget() throws URISyntaxException {
        ReplicatorBuilder.Pull p = ReplicatorBuilder.pull().
                from(new URI("http://localhost/abc")).
                to(null);
        p.build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void replicatorBuilderUnknownProtocol() throws URISyntaxException {
        ReplicatorBuilder.Pull p = ReplicatorBuilder.pull().
                from(new URI("gopher://localhost/abc")).
                to(documentStore);
        p.build();
    }

    @Test
    public void replicatorBuilderAddsIamInterceptor() throws Exception {
        String apiKey = "abc123";
        ReplicatorBuilder.Pull p = ReplicatorBuilder.pull().from(new URI("http://example.com/path")).
                to(documentStore).
                iamApiKey(apiKey);
        // although the replicator isn't used, the interceptor check expects the presence of the
        // header interceptor, which only gets added if build() is called
        ReplicatorImpl r = (ReplicatorImpl) p.build();
        assertIamCookieInterceptorPresent(p);
    }

    // as above test, but ensure IAM API key takes precedence over username/password
    @Test
    public void replicatorBuilderAddsIamInterceptorWhenUsernamePasswordPresent() throws Exception {
        String apiKey = "abc123";
        ReplicatorBuilder.Pull p = ReplicatorBuilder.pull().from(new URI("http://example.com/path")).
                to(documentStore).
                username("username").
                password("password").
                iamApiKey(apiKey);
        // although the replicator isn't used, the interceptor check expects the presence of the
        // header interceptor, which only gets added if build() is called
        ReplicatorImpl r = (ReplicatorImpl) p.build();
        assertIamCookieInterceptorPresent(p);
    }

}
