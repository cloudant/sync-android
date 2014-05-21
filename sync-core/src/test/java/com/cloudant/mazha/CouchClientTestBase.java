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

import com.cloudant.mazha.json.JSONHelper;
import com.google.common.base.Strings;
import org.junit.Before;

public abstract class CouchClientTestBase {

    static final String TEST_DB = "mazha-test";

    public static final Boolean TEST_WITH_CLOUDANT = Boolean.valueOf(
            System.getProperty("test.with.cloudant",Boolean.FALSE.toString()));

    static {
        if(TEST_WITH_CLOUDANT) {
            CouchConfig config = CloudantConfig.defaultConfig();
            if(Strings.isNullOrEmpty(config.getUsername()) ||
                    Strings.isNullOrEmpty(config.getPassword())) {
                throw new IllegalStateException("Cloudant account info" +
                        " is required to run tests with Cloudant.");
            }
        }
    }

    CouchConfig couchConfig;
    CouchClient client;
    JSONHelper jsonHelper;

    public CouchClientTestBase() {
        couchConfig = getCouchConfig();
        client = new CouchClient(couchConfig, TEST_DB);
        jsonHelper = new JSONHelper();
    }

    public CouchConfig getCouchConfig() {
        if(TEST_WITH_CLOUDANT) {
            return CloudantConfig.defaultConfig();
        } else {
            return CouchConfig.defaultConfig();
        }
    }

    @Before
    public void setUp() {
        makeSureTestDbExistOnServer();
    }

    private void makeSureTestDbExistOnServer() {
        ClientTestUtils.deleteQuietly(client, TEST_DB);
        client.createDb(TEST_DB);
    }
}