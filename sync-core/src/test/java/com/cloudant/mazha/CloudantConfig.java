/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.mazha;

import java.net.URI;
import java.net.URISyntaxException;

public class CloudantConfig {

    public static String CLOUDANT_HOST ;
    public static String CLOUDANT_USERNAME;
    public static String CLOUDANT_PASSWORD;

    static {
        String username = System.getProperty("test.cloudant.username");
        String password = System.getProperty("test.cloudant.password");

        CLOUDANT_HOST = String.format("%s.cloudant.com", username);
        CLOUDANT_USERNAME = username;
        CLOUDANT_PASSWORD = password;
    }

    private CloudantConfig() {}

    public static CouchConfig defaultConfig(String dbName) {
        try {
            String uriString = String.format("https://%s:%s@%s:443/%s", CLOUDANT_USERNAME, CLOUDANT_PASSWORD, CLOUDANT_HOST, dbName);
            CouchConfig config = new CouchConfig(new URI(uriString));
            return config;
        } catch (URISyntaxException use) {
            throw new IllegalArgumentException(use);
        }
    }
}