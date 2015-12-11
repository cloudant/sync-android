/*
 * Copyright (c) 2015 IBM Corp. All rights reserved.
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

import com.cloudant.http.Http;
import com.cloudant.sync.replication.ReplicationTestBase;

import java.net.URL;

/**
 * Created by tomblench on 23/07/15.
 */
public class ProxyTestBase extends ReplicationTestBase {

    // this is where toxiproxy listens
    protected int proxyPort = Integer.parseInt(System.getProperty("test.couch.port", "8000"));
    // this is the toxiproxy admin port
    protected int proxyAdminPort = Integer.parseInt(System.getProperty("test.couch.proxy.admin.port",
            "8474"));
    // this is where couchdb listens
    protected int targetPort = Integer.parseInt(System.getProperty("test.couch.proxy.target.port",
            "5984"));
    protected String proxyName = "default";
    protected String proxyHost = "127.0.0.1";

    protected String jsonAddProxy = String.format("{\"name\": \"%s\", \"upstream\": \"localhost:%d\", \"listen\": \"localhost:%d\"}",
            proxyName, targetPort, proxyPort);


    protected void startProxy() throws Exception {
        // clear proxy
        try {
            Http.DELETE(new URL(String.format("http://%s:%d/proxies/%s", proxyHost,
                    proxyAdminPort, proxyName))).execute().responseAsString();
        } catch (Exception e) {
            // 404?
        }
        // set up fresh proxy
        Http.POST(new URL(String.format("http://%s:%d/proxies", proxyHost, proxyAdminPort)), "application/json")
                .setRequestBody(jsonAddProxy).execute().responseAsString();
    }

    protected void stopProxy() throws Exception {
        // clear proxy
        Http.DELETE(new URL(String.format("http://%s:%d/proxies/%s", proxyHost, proxyAdminPort, proxyName)))
                .execute().responseAsString();
    }

}
