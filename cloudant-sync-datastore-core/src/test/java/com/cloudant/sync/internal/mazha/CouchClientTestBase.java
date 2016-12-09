/*
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

package com.cloudant.sync.internal.mazha;

import com.cloudant.common.CouchTestBase;

import org.junit.After;
import org.junit.Before;

public abstract class CouchClientTestBase extends CouchTestBase {

    String testDb;
    CouchConfig couchConfig;
    CouchClient client;

    public CouchClientTestBase() {
        testDb = "mazha-test"+System.currentTimeMillis();
        couchConfig = getCouchConfig(testDb);
        client = new CouchClient(couchConfig.getRootUri(), couchConfig.getRequestInterceptors(),
                couchConfig.getResponseInterceptors());
    }

    @Before
    public void setUp() {
        makeSureTestDbExistOnServer();
    }

    @After
    public void tearDown() {
        ClientTestUtils.deleteQuietly(client);
    }

    private void makeSureTestDbExistOnServer() {
        ClientTestUtils.deleteQuietly(client);
        client.createDb();
    }
}
