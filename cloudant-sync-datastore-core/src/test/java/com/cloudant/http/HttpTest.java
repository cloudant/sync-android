//  Copyright (c) 2015 IBM Cloudant. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.cloudant.http;

import com.cloudant.common.CouchTestBase;
import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.common.TestOptions;
import com.cloudant.http.interceptors.CookieInterceptor;
import com.cloudant.mazha.CouchClient;
import com.cloudant.mazha.CouchConfig;
import com.cloudant.mazha.json.JSONHelper;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * Created by tomblench on 06/05/15.
 */

@Category(RequireRunningCouchDB.class)
public class HttpTest extends CouchTestBase {

    private String data = "{\"hello\":\"world\"}";

    /*
     * Test "Expect: 100-Continue" header works as expected
     * See "8.2.3 Use of the 100 (Continue) Status" in http://tools.ietf.org/html/rfc2616
     * We expect the precondition of having a valid DB name to have failed, and therefore, the body
     * data will not have been written.
     *
     * NB this behaviour is only supported on certain JDKs - so we have to make a weaker set of
     * asserts. If it is supported, we expect execute() to throw an exception and then nothing will
     * have been read from the stream. If it is not supported, execute() will not throw and we
     * cannot make any assumptions about how much of the stream has been read (remote side may close
     * whilst we are still writing).
     */
    @Test
    public void testExpect100Continue() throws IOException {
        CouchConfig config = getCouchConfig("no_such_database");
        HttpConnection conn = new HttpConnection("POST", config.getRootUri().toURL(),
                "application/json");
        final ByteArrayInputStream bis = new ByteArrayInputStream(data.getBytes());

        // nothing read from stream
        Assert.assertEquals(bis.available(), data.getBytes().length);

        conn.setRequestBody(new HttpConnection.InputStreamGenerator() {
            @Override
            public InputStream getInputStream() {
                return bis;
            }
        });
        boolean thrown = false;
        try {
            conn.execute();
        } catch (IOException ioe) {
            // ProtocolException with message "Server rejected operation" on JDK 1.7
            thrown = true;
        }

        if (thrown) {
            // still nothing read from stream
            Assert.assertEquals(bis.available(), data.getBytes().length);
        }
    }

    /*
     * Basic test that we can write a document body by POSTing to a known database
     */
    @Test
    public void testWriteToServerOk() throws IOException {
        CouchConfig config = getCouchConfig("httptest" + System.currentTimeMillis());
        CouchClient client = new CouchClient(config.getRootUri(), config.getRequestInterceptors()
                , config.getResponseInterceptors());
        client.createDb();
        HttpConnection conn = new HttpConnection("POST", config.getRootUri().toURL(),
                "application/json");
        final ByteArrayInputStream bis = new ByteArrayInputStream(data.getBytes());

        // nothing read from stream
        Assert.assertEquals(bis.available(), data.getBytes().length);

        conn.setRequestBody(new HttpConnection.InputStreamGenerator() {
            @Override
            public InputStream getInputStream() {
                return bis;
            }
        });
        conn.execute();

        // stream was read to end
        Assert.assertEquals(bis.available(), 0);
        client.deleteDb();
    }

    /*
     * Basic test to check that an IOException is thrown when we attempt to get the response
     * without first calling execute()
     */
    @Test
    public void testReadBeforeExecute() throws IOException {
        CouchConfig config = getCouchConfig("httptest" + System.currentTimeMillis());
        CouchClient client = new CouchClient(config.getRootUri(), config.getRequestInterceptors()
                , config.getResponseInterceptors());
        client.createDb();
        HttpConnection conn = new HttpConnection("POST", config.getRootUri().toURL(),
                "application/json");
        final ByteArrayInputStream bis = new ByteArrayInputStream(data.getBytes());

        // nothing read from stream
        Assert.assertEquals(bis.available(), data.getBytes().length);

        conn.setRequestBody(new HttpConnection.InputStreamGenerator() {
            @Override
            public InputStream getInputStream() {
                return bis;
            }
        });
        try {
            conn.responseAsString();
            Assert.fail("IOException not thrown as expected");
        } catch (IOException ioe) {
            ; // "Attempted to read response from server before calling execute()"
        }

        // stream was not read because execute() was not called
        Assert.assertEquals(bis.available(), data.getBytes().length);
        client.deleteDb();
    }


    //NOTE: This test doesn't work with specified couch servers,
    // the URL will always include the creds specified for the test
    //
    // A couchdb server needs to be set and running with the correct
    // security settings, the database *must* not be public, it *must*
    // be named cookie_test
    //
    @Test
    public void testCookieAuthWithoutRetry() throws IOException {

        if (TestOptions.IGNORE_AUTH_HEADERS) {
            return;
        }

        CookieInterceptor interceptor = new CookieInterceptor(TestOptions.COUCH_USERNAME,
                TestOptions.COUCH_PASSWORD,
                getCouchConfig("httptest" + System
                        .currentTimeMillis()).getRootUri().toString());

        CouchConfig config = getCouchConfig("cookie_test");
        HttpConnection conn = new HttpConnection("POST", config.getRootUri().toURL(),
                "application/json");
        conn.responseInterceptors.add(interceptor);
        conn.requestInterceptors.add(interceptor);
        final ByteArrayInputStream bis = new ByteArrayInputStream(data.getBytes());

        // nothing read from stream
        Assert.assertEquals(bis.available(), data.getBytes().length);

        conn.setRequestBody(new HttpConnection.InputStreamGenerator() {
            @Override
            public InputStream getInputStream() {
                return bis;
            }
        });
        conn.execute();

        // stream was read to end
        Assert.assertEquals(bis.available(), 0);
        Assert.assertEquals(2, conn.getConnection().getResponseCode() / 100);

        //check the json
        JSONHelper helper = new JSONHelper();
        Map<String, Object> jsonRes = helper.fromJson(new InputStreamReader(conn.getConnection()
                .getInputStream()));

        Assert.assertTrue(jsonRes.containsKey("ok"));
        Assert.assertTrue((Boolean) jsonRes.get("ok"));
        Assert.assertTrue(jsonRes.containsKey("id"));
        Assert.assertTrue(jsonRes.containsKey("rev"));

    }
}
