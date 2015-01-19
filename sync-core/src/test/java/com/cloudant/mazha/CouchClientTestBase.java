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

import java.lang.reflect.Field;

import com.cloudant.common.CouchTestBase;
import com.cloudant.mazha.json.JSONHelper;
import com.cloudant.sync.util.Misc;
import com.google.common.base.Strings;
import org.junit.Before;

public abstract class CouchClientTestBase extends CouchTestBase {

    String TEST_DB = "mazha-test"+System.currentTimeMillis();

    static {
        if(TEST_WITH_CLOUDANT) {
            CouchConfig config = CloudantConfig.defaultConfig(TEST_DB);
            if(Strings.isNullOrEmpty(config.getRootUri().getUserInfo())) {
                throw new IllegalStateException("Cloudant account info" +
                        " is required to run tests with Cloudant.");
            }
        }
    }

    CouchConfig couchConfig;
    CouchClient client;
    JSONHelper jsonHelper;

    public CouchClientTestBase() {
        couchConfig = getCouchConfig(TEST_DB);
        client = new CouchClient(couchConfig);
        jsonHelper = new JSONHelper();
    }

    @Before
    public void setUp() {
        makeSureTestDbExistOnServer();
    }

    private void makeSureTestDbExistOnServer() {
        ClientTestUtils.deleteQuietly(client);
        client.createDb();
    }
}
