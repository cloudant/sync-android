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
import com.google.common.base.Strings;

import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


@Category(RequireRunningCouchDB.class)
public class HttpRequestsTest extends CouchClientTestBase {

    // test we can set a custom header and make a request
    // (we don't check the header is received at the server!)
    @Test
    public void customHeader() throws Exception {
        CouchConfig customCouchConfig = getCouchConfig(testDb);
        Map<String, String> customHeaders = new HashMap<String, String>();
        customHeaders.put("x-good-header", "test");
        customCouchConfig.setCustomHeaders(customHeaders);
        Assert.assertEquals("test", customCouchConfig.getCustomHeaders().get("x-good-header"));
        CouchClient customClient = new CouchClient(customCouchConfig);
        CouchDbInfo dbInfo = customClient.getDbInfo();
        Assert.assertNotNull(dbInfo);
        Assert.assertTrue(dbInfo.getDbName().contains(testDb));
    }

    // test we can override the auth header if it is already set from user/pass
    // NB this test only works with a local CouchDB in admin party mode and relies
    // on the fact that any auth string other than "basic: <user:pass>" is accepted
    @Test
    public void customHeaderAuthOverride() throws Exception {
        // skip if not running against local CouchDB
        org.junit.Assume.assumeTrue(!IGNORE_AUTH_HEADERS);

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
        Map<String, String> customHeaders = new HashMap<String, String>();

        // first we check that foo/bar is unauthorized
        try {
            CouchClient customClient = new CouchClient(customCouchConfig);
            customClient.getDbInfo();
            Assert.fail("Expected CouchException to be thrown");
        } catch (CouchException ce) {
            Assert.assertEquals("unauthorized", ce.getError());
        }

        // now put in a string which isn't basic auth, it will be ignored by the server
        customHeaders.put("Authorization", "test");
        customCouchConfig.setCustomHeaders(customHeaders);
        Assert.assertEquals("test", customCouchConfig.getCustomHeaders().get("Authorization"));
        CouchClient clientAuthHeader = new CouchClient(customCouchConfig);
        CouchDbInfo dbInfo = clientAuthHeader.getDbInfo();
        Assert.assertNotNull(dbInfo);
        Assert.assertTrue(dbInfo.getDbName().contains(testDb));
    }

    // test we can set the auth header when user and pass weren't set
    // NB this test relies on the fact that foo/bar is not a working username/password
    @Test
    public void customHeaderAuth() throws Exception {
        // skip if not running against local CouchDB
        org.junit.Assume.assumeTrue(!IGNORE_AUTH_HEADERS);

        CouchConfig customCouchConfig = getCouchConfig(testDb);
        // we're not testing on Cloudant, so we can be sure user/pass is not set - double check:
        Assert.assertTrue(Strings.isNullOrEmpty(customCouchConfig.getRootUri().getUserInfo()));

        Map<String, String> customHeaders = new HashMap<String, String>();
        URI thisDbUri = couchConfig.getRootUri();

        try {
            String authString = "foo:bar";
            Base64 base64 = new Base64();
            String authHeaderValue = "Basic " + new String(base64.encode(authString.getBytes()));
            customHeaders.put("Authorization", authHeaderValue);
            customCouchConfig.setCustomHeaders(customHeaders);
            CouchClient customClient = new CouchClient(customCouchConfig);
            customClient.getDbInfo();
            Assert.fail("Expected CouchException to be thrown");
        } catch (CouchException ce) {
            Assert.assertEquals("unauthorized", ce.getError());
        }
    }

}
