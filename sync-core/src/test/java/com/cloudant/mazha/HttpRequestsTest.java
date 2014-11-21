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

import java.io.InputStream;
import java.net.URI;


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
    public void shutdown() {
        client.shutdown();
    }

}
