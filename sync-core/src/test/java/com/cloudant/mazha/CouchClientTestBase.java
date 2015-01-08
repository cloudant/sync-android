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

import com.cloudant.mazha.json.JSONHelper;
import com.cloudant.sync.util.Misc;
import com.google.common.base.Strings;
import org.junit.Before;

public abstract class CouchClientTestBase {

    String TEST_DB = "mazha-test"+System.currentTimeMillis();

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

            // on an android emulator connecting to 127.0.0.1 connects to the emulated device not
            // the host machine, so if the test is running on android the ip address of the machine
            // hosting the emulator needs to be used, 10.0.2.2 points to the loop back interface of
            // the host machine
            // the best way to do this change is editing the default host value at runtime via reflection
            // getting the correct host from BuildConfig for the test application
            if(Misc.isRunningOnAndroid()){

                try {
                    CouchConfig config = CouchConfig.defaultConfig();
                    Field host = config.getClass().getDeclaredField("couchdb_host");
                    host.setAccessible(true);

                    Class buildConfigClass = Class.forName("cloudant.com.androidtest.BuildConfig");

                    host.set(config, buildConfigClass.getField("ip").get(buildConfigClass
                            .newInstance()));
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
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
