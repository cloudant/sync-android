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
import org.apache.http.client.params.HttpClientParams;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.cloudant.common.RequireRunningCouchDB;
import com.google.common.base.Strings;

import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


@Category(RequireRunningCouchDB.class)
public class HttpRequestsTest extends CouchClientTestBase {

    CouchURIHelper uriHelper;
    HttpRequests client;

    @Before
    public void setup() {
        super.setUp();
        this.uriHelper = new CouchURIHelper(
                couchConfig.getProtocol(),
                couchConfig.getHost(),
                couchConfig.getPort()
        );
        this.client = super.client.getHttpClient();
    }

    @Test
    public void get_resourceExist_success() throws Exception {
        URI allDbUri = this.uriHelper.allDbsUri();
        InputStream is = client.get(allDbUri);
        Assert.assertNotNull(is);
        String s = IOUtils.toString(is);
        Assert.assertTrue(s.contains(TEST_DB));
    }

    @Test
    public void prohibitedCustomHeaderFails() throws Exception {
        // prohibited headers, in mixed case
        String[] prohibitedHeaders = {"WWW-Authenticate", "Host", "Connection", "Content-Type",
                "Accept", "Content-Length"};
        for (String h : prohibitedHeaders) {
            try {
                CouchConfig customCouchConfig = getCouchConfig();
                Map<String, String> customHeaders = new HashMap<String, String>();
                customHeaders.put(h, "test");
                customCouchConfig.setCustomHeaders(customHeaders);
                HttpRequests client = new CouchClient(customCouchConfig, TEST_DB).getHttpClient();
                URI allDbUri = this.uriHelper.allDbsUri();
                client.get(allDbUri);
                Assert.fail("Expected IllegalArgumentException");
            } catch (IllegalArgumentException iae) {
                ;
            }
        }
    }

    // test we can set a custom header and make a request
    // (we don't check the header is received at the server!)
    @Test
    public void customHeader() throws Exception {
        CouchConfig customCouchConfig = getCouchConfig();
        Map<String, String> customHeaders = new HashMap<String, String>();
        customHeaders.put("x-good-header", "test");
        customCouchConfig.setCustomHeaders(customHeaders);
        Assert.assertEquals("test", customCouchConfig.getCustomHeaders().get("x-good-header"));
        HttpRequests client = new CouchClient(customCouchConfig, TEST_DB).getHttpClient();
        URI allDbUri = this.uriHelper.allDbsUri();
        InputStream is = client.get(allDbUri);
        Assert.assertNotNull(is);
        String s = IOUtils.toString(is);
        Assert.assertTrue(s.contains(TEST_DB));
    }

    // test we can override the auth header if it is already set from user/pass
    // NB this test only works with a local CouchDB in admin party mode and relies
    // on the fact that any auth string other than "basic: <user:pass>" is accepted
    @Test
    public void customHeaderAuthOverride() throws Exception {
        // skip if not running against local CouchDB
        org.junit.Assume.assumeTrue(!TEST_WITH_CLOUDANT);

        CouchConfig customCouchConfig = new CouchConfig(couchConfig.getProtocol(),
                couchConfig.getHost(),
                couchConfig.getPort(),
                "foo", "bar");
        Map<String, String> customHeaders = new HashMap<String, String>();
        URI allDbUri = this.uriHelper.allDbsUri();

        // first we check that foo/bar is unauthorized
        try {
            HttpRequests clientNoAuthHeader = new CouchClient(customCouchConfig, TEST_DB).getHttpClient();
            clientNoAuthHeader.get(allDbUri);
            Assert.fail("Expected CouchException to be thrown");
        } catch (CouchException ce) {
            Assert.assertEquals("unauthorized", ce.getError());
        }

        // now put in a string which isn't basic auth, it will be ignored by the server
        customHeaders.put("Authorization", "test");
        customCouchConfig.setCustomHeaders(customHeaders);
        Assert.assertEquals("test", customCouchConfig.getCustomHeaders().get("Authorization"));
        HttpRequests clientAuthHeader = new CouchClient(customCouchConfig, TEST_DB).getHttpClient();
        InputStream is = clientAuthHeader.get(allDbUri);
        Assert.assertNotNull(is);
        String s = IOUtils.toString(is);
        Assert.assertTrue(s.contains(TEST_DB));
    }

    // test we can set the auth header when user and pass weren't set
    // NB this test relies on the fact that foo/bar is not a working username/password
    @Test
    public void customHeaderAuth() throws Exception {
        // skip if not running against local CouchDB
        org.junit.Assume.assumeTrue(!TEST_WITH_CLOUDANT);

        CouchConfig customCouchConfig = getCouchConfig();
        // we're not testing on Cloudant, so we can be sure user/pass is not set - double check:
        Assert.assertTrue(Strings.isNullOrEmpty(customCouchConfig.getUsername()));
        Assert.assertTrue(Strings.isNullOrEmpty(customCouchConfig.getPassword()));

        Map<String, String> customHeaders = new HashMap<String, String>();
        URI allDbUri = this.uriHelper.allDbsUri();

        try {
            String authString = "foo:bar";
            Base64 base64 = new Base64();
            String authHeaderValue = "Basic " + new String(base64.encode(authString.getBytes()));
            customHeaders.put("Authorization", authHeaderValue);
            customCouchConfig.setCustomHeaders(customHeaders);
            HttpRequests clientAuthHeader = new CouchClient(customCouchConfig, TEST_DB).getHttpClient();
            clientAuthHeader.get(allDbUri);
            Assert.fail("Expected CouchException to be thrown");
        } catch (CouchException ce) {
            Assert.assertEquals("unauthorized", ce.getError());
        }
    }

    @Test
    public void shutdown() {
        client.shutdown();
    }

}
