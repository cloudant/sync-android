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

import com.cloudant.mazha.CouchConfig;
import com.google.common.base.Preconditions;

import java.net.URI;

class ReplicatorURIUtils {

    public static CouchConfig extractCouchConfig(URI uri) {
        Preconditions.checkArgument(uri.getScheme() != null, "Protocol must be provided.");
        Preconditions.checkArgument(uri.getHost() != null, "Host must be provided.");
        Preconditions.checkArgument(uri.getScheme().equalsIgnoreCase("http")
                || uri.getScheme().equalsIgnoreCase("https"), "Only http/https are supported.");

        String user = getUsername(uri.getUserInfo());
        String password = getPassword(uri.getUserInfo());
        int port = uri.getPort() < 0 ? getDefaultPort(uri.getScheme()) : uri.getPort();

        return new CouchConfig(uri.getScheme(), uri.getHost(), port, user, password);
    }

    static int getDefaultPort(String protocol) {
        if(protocol.equalsIgnoreCase("http")) {
            return 80;
        } else if(protocol.equalsIgnoreCase("https")) {
            return 443;
        } else {
            throw new IllegalArgumentException("Unsupported protocol: " + protocol);
        }
    }

    static String getUsername(String userInfo) {
        if(userInfo == null || userInfo.indexOf(':') < 0) {
            return "";
        } else {
            return userInfo.substring(0, userInfo.indexOf(':'));
        }
    }

    static String getPassword(String userInfo) {
        if(userInfo == null || userInfo.indexOf(':') < 0) {
            return "";
        } else {
            return userInfo.substring(userInfo.indexOf(':') + 1);
        }
    }

    public static String extractDatabaseName(URI uri) {
        String db =  uri.getPath().substring(1);
        if(db.indexOf("/") >= 0) {
            throw new IllegalArgumentException("DB name can not contain slash: '/'");
        }
        return db;
    }
}
