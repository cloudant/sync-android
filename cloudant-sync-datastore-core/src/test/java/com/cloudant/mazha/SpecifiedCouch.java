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

package com.cloudant.mazha;

import static com.cloudant.common.TestOptions.COOKIE_AUTH;
import static com.cloudant.common.TestOptions.COUCH_HOST;
import static com.cloudant.common.TestOptions.COUCH_PASSWORD;
import static com.cloudant.common.TestOptions.COUCH_PORT;
import static com.cloudant.common.TestOptions.COUCH_URI;
import static com.cloudant.common.TestOptions.COUCH_USERNAME;
import static com.cloudant.common.TestOptions.HTTP_PROTOCOL;

import com.cloudant.http.interceptors.CookieInterceptor;
import com.cloudant.http.HttpConnectionRequestInterceptor;
import com.cloudant.http.HttpConnectionResponseInterceptor;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

/**
 * Created by Rhys Short on 30/01/15.
 */
public class SpecifiedCouch {

    private SpecifiedCouch() {
        //empty
     }

    public static CouchConfig defaultConfig(String dbName){
        try {
            String uriString;

            // if full URI specified, then we don't need to build it up from components
            if (COUCH_URI != null) {
                uriString = String.format("%s/%s", COUCH_URI, dbName);
            }
            // otherwise build the URI up, but skip username/password from URL if they aren't there
            // or we are doing cookie auth
            else {
                if (COOKIE_AUTH || COUCH_USERNAME == null || COUCH_PASSWORD == null) {
                    uriString = String.format("%s://%s:%s/%s", HTTP_PROTOCOL, COUCH_HOST, COUCH_PORT, dbName);
                } else {
                    uriString = String.format("%s://%s:%s@%s:%s/%s", HTTP_PROTOCOL, COUCH_USERNAME, COUCH_PASSWORD, COUCH_HOST, COUCH_PORT, dbName);
                }
            }
            CouchConfig config = new CouchConfig(new URI(uriString));
            if (COOKIE_AUTH) {
                ArrayList<HttpConnectionRequestInterceptor> requestInterceptors = new ArrayList<HttpConnectionRequestInterceptor>();
                ArrayList<HttpConnectionResponseInterceptor> responseInterceptors = new ArrayList<HttpConnectionResponseInterceptor>();
                CookieInterceptor cookieInterceptor = new CookieInterceptor(COUCH_USERNAME, COUCH_PASSWORD);
                requestInterceptors.add(cookieInterceptor);
                responseInterceptors.add(cookieInterceptor);
                config.setRequestInterceptors(requestInterceptors);
                config.setResponseInterceptors(responseInterceptors);
            }
            return config;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }

    }
}
