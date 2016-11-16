/*
 * Copyright © 2015 Cloudant, Inc. All rights reserved.
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

package com.cloudant.common;

import com.cloudant.sync.internal.mazha.CouchConfig;
import com.cloudant.sync.internal.mazha.SpecifiedCouch;
import com.cloudant.sync.internal.util.Misc;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by tomblench on 20/01/15.
 */
public abstract class CouchTestBase {


    static {
        try {

            //  When running on android we need to load the test config from the BuildConfig class
            //  which is only available on android. As a result we get the TEST_CONFIG field via
            //  reflection and convert the 2D array into System properties to be used by the tests.

            if (Misc.isRunningOnAndroid()) {
                Class klass = Class.forName("cloudant.com.androidtest.test.BuildConfig");
                Field testConfig = klass.getField("TEST_CONFIG");
                String[][] testConfigHash  = (String[][])testConfig.get(null);
                for(String[] configPair : testConfigHash){
                    System.setProperty(configPair[0],configPair[1]);
                }
            }
        } catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }



    public CouchConfig getCouchConfig(String db) {

        // setting COUCH_URI implies SPECIFIED_COUCH but means we are giving the full URI rather
        // than the component parts
        if (TestOptions.COUCH_URI != null || TestOptions.SPECIFIED_COUCH) {
            return SpecifiedCouch.defaultConfig(db);
        }
        else {
            String host;
             // If we're running on the Android emulator, 127.0.0.1 is the emulated device, rather
             // than the host machine. Instead we connect to 10.0.2.2.
            if(Misc.isRunningOnAndroid()){
                host = "10.0.2.2";
            } else {
                host = "127.0.0.1";
            }
            return this.defaultConfig(host, db);
        }
    }

    public CouchConfig defaultConfig(String host, String databasePath) {
        try {
            // we use String.format rather than the multi-arg URI constructor to avoid database
            // names being (double) escaped
            String urlString = String.format("http://%s:5984/%s", host, databasePath);
            return new CouchConfig(new URI(urlString));
        } catch (URISyntaxException use) {
            use.printStackTrace();
            return null;
        }
    }
}
