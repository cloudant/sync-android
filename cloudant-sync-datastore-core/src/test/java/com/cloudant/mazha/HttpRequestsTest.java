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

package com.cloudant.mazha;


import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.common.TestOptions;
import com.cloudant.http.HttpConnectionInterceptorContext;
import com.cloudant.http.HttpConnectionRequestInterceptor;
import com.google.common.base.Strings;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Category(RequireRunningCouchDB.class)
public class HttpRequestsTest extends CouchClientTestBase {

    private HttpConnectionRequestInterceptor makeHeaderInterceptor(final String headerKey,
                                                                   final String headerValue)
    {
        HttpConnectionRequestInterceptor interceptor = new HttpConnectionRequestInterceptor() {
            @Override
            public HttpConnectionInterceptorContext interceptRequest
            (HttpConnectionInterceptorContext context) {
                HttpURLConnection connection = context.connection.getConnection();
                connection.setRequestProperty(headerKey, headerValue);
                return context;
            }
        };
        return interceptor;
    };

    // test we can set a custom header and make a request
    // (we don't check the header is received at the server!)
    @Test
    public void customHeader() throws Exception {
        CouchConfig customCouchConfig = getCouchConfig(testDb);
        ArrayList<HttpConnectionRequestInterceptor> customInterceptors = new ArrayList<HttpConnectionRequestInterceptor>();
        customInterceptors.addAll(customCouchConfig.getRequestInterceptors());
        // add interceptor to set header
        customInterceptors.add(makeHeaderInterceptor("x-good-header", "test"));
        // add interceptor to check that the header has been set
        customInterceptors.add(new HttpConnectionRequestInterceptor() {
            @Override
            public HttpConnectionInterceptorContext interceptRequest(HttpConnectionInterceptorContext context) {
                Assert.assertEquals(context.connection.getConnection().getRequestProperty("x-good-header"), "test");
                return context;
            }
        });
        customCouchConfig.setRequestInterceptors(customInterceptors);
        CouchClient customClient = new CouchClient(customCouchConfig.getRootUri(),
                customCouchConfig.getRequestInterceptors(),
                customCouchConfig.getResponseInterceptors());
        CouchDbInfo dbInfo = customClient.getDbInfo();
        Assert.assertNotNull(dbInfo);
        Assert.assertTrue(dbInfo.getDbName().contains(testDb));
    }

    // test we can override the auth header if it is already set from user/pass
    // NB this test only works with a local CouchDB in admin party mode and relies
    // on the fact that any auth string other than "basic: <user:pass>" is accepted
    @Test
    public void customHeaderAuthOverride() throws Exception {

        // check that we are running in a configuration where there is no username/password set:
        // (most commonly this would be the default config of running against a local couch instance
        // in admin party mode)
        org.junit.Assume.assumeTrue("Test skipped because Basic Auth credentials are required to " +
                        "access this server",
                !TestOptions.COOKIE_AUTH && Strings.isNullOrEmpty(couchConfig.getRootUri().getUserInfo()));
        URI root = couchConfig.getRootUri();

        // copy the basic test url but add user:foo, pass:bar as credentials
        URI rootWithUserCreds = new URI(root.getScheme(),
                "foo:bar",
                root.getHost(),
                root.getPort(),
                root.getPath(),
                null,
                null);
        CouchConfig customCouchConfig = new CouchConfig(rootWithUserCreds);

        // first we check that foo/bar is unauthorized
        try {
            CouchClient customClient = new CouchClient(customCouchConfig.getRootUri(),
                    customCouchConfig.getRequestInterceptors(),
                    customCouchConfig.getResponseInterceptors());
            customClient.getDbInfo();
            Assert.fail("Expected CouchException to be thrown");
        } catch (CouchException ce) {
            Assert.assertEquals("unauthorized", ce.getError());
        }

        // now put in a string which isn't basic auth, it will be ignored by the server
        ArrayList<HttpConnectionRequestInterceptor> customInterceptors = new ArrayList<HttpConnectionRequestInterceptor>();
        customInterceptors.add(makeHeaderInterceptor("Authorization", "test"));
        customCouchConfig.setRequestInterceptors(customInterceptors);
        CouchClient clientAuthHeader = new CouchClient(customCouchConfig.getRootUri(),
                customCouchConfig.getRequestInterceptors(),
                customCouchConfig.getResponseInterceptors());
        CouchDbInfo dbInfo = clientAuthHeader.getDbInfo();
        Assert.assertNotNull(dbInfo);
        Assert.assertTrue(dbInfo.getDbName().contains(testDb));
    }

    // test we can set the auth header when user and pass weren't set
    // NB this test relies on the fact that foo/bar is not a working username/password
    @Test
    public void customHeaderAuth() throws Exception {

        CouchConfig customCouchConfig = getCouchConfig(testDb);

        // check that we are running in a configuration where there is no username/password set:
        // (most commonly this would be the default config of running against a local couch instance
        // in admin party mode)
        org.junit.Assume.assumeTrue("Test skipped because Basic Auth credentials are required to " +
                        "access this server",
                TestOptions.COOKIE_AUTH && Strings.isNullOrEmpty(customCouchConfig.getRootUri().getUserInfo()));

        try {
            String authString = "foo:bar";
            Base64 base64 = new Base64();
            String authHeaderValue = "Basic " + new String(base64.encode(authString.getBytes()));
            ArrayList<HttpConnectionRequestInterceptor> customInterceptors = new ArrayList<HttpConnectionRequestInterceptor>();
            customInterceptors.add(makeHeaderInterceptor("Authorization", authHeaderValue));
            customCouchConfig.setRequestInterceptors(customInterceptors);
            CouchClient customClient = new CouchClient(customCouchConfig.getRootUri(),
                    customCouchConfig.getRequestInterceptors(),
                    customCouchConfig.getResponseInterceptors());
            customClient.getDbInfo();
            Assert.fail("Expected CouchException to be thrown");
        } catch (CouchException ce) {
            Assert.assertEquals("unauthorized", ce.getError());
        }
    }
}
