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

package com.cloudant.sync.replication;


import com.cloudant.mazha.CloudantConfig;
import com.cloudant.mazha.CouchClientTestBase;
import com.cloudant.mazha.CouchConfig;
import com.cloudant.sync.util.Misc;

import java.lang.reflect.Field;

/**
 * Test base for tests that need to run against different CouchDb instances (local CouchDB, remote CouchDB,
 * Cloudant etc.)
 */
public abstract class CouchTestBase {

    protected CouchConfig getCouchConfig() {
        if(CouchClientTestBase.TEST_WITH_CLOUDANT) {
            return CloudantConfig.defaultConfig();
        } else {

            if(Misc.isRunningOnAndroid()){
                //lots of nasty reflection goes here :(

                try {
                    CouchConfig config = CouchConfig.defaultConfig();

                    Field host = config.getClass().getDeclaredField("couchdb_host");
                    host.setAccessible(true);

                    Class buildConfigClass = Class.forName("cloudant.com.androidtest.BuildConfig");
                    Field ip = buildConfigClass.getField("ip");
                    String ipAddress = (String)ip.get(null);
                    host.set(config,ipAddress);

                } catch (Exception e){
                    e.printStackTrace();
                } catch (Throwable t){
                    t.printStackTrace();
                }


            }


            return CouchConfig.defaultConfig();
        }
    }
}
