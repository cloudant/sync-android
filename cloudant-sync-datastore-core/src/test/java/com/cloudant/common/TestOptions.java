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

/**
 * Created by tomblench on 05/10/15.
 */

/**
 * Convenience methods for accessing various test options which can be set configured through JVM
 * system properties
 */
public class TestOptions {

    /*
     * These options describe how to access the remote CouchDB/Cloudant server
     */

    /**
     * If specified, tests will run against an instance described by this URI.
     * This will over-ride any of the other COUCH_ options given.
     * This URI can include all of the components necessary including username and password for
     * basic auth.
     * It is not necessary to set SPECIFIED_COUCH to true in order to use this option.
     */
    public static final String COUCH_URI = System.getProperty("test.couch.uri");

    /**
     * If true, tests will run against an instance described by
     * COUCH_USERNAME, COUCH_PASSWORD, COUCH_HOST, COUCH_PORT, HTTP_PROTOCOL with default values
     * supplied as appropriate.
     * If false, tests will run against a local couch instance on the default port over http.
     */
    public static final Boolean SPECIFIED_COUCH = Boolean.valueOf(
            System.getProperty("test.with.specified.couch", Boolean.FALSE.toString()));

    /**
     * If specified, tests will use this username to access the instance with basic auth.
     * Alternatively, if COOKIE_AUTH is true, tests will use this username to refresh cookies.
     */
    public static final String COUCH_USERNAME = System.getProperty("test.couch.username");

    /**
     * If specified, tests will use this password to access the instance with basic auth.
     * Alternatively, if COOKIE_AUTH is true, tests will use this password to refresh cookies.
     */
    public static final String COUCH_PASSWORD = System.getProperty("test.couch.password");

    /**
     * If specified, tests will use this hostname or IP address to access the instance.
     * Otherwise, a default value of "localhost" is used.
     */
    public static final String COUCH_HOST = System.getProperty("test.couch.host", "localhost");

    /**
     * If specified, tests will use this port to access the instance.
     * Otherwise, a default value of 5984 is used.
     */
    public static final String COUCH_PORT = System.getProperty("test.couch.port", "5984");

    /**
     * If specified, tests will use this protocol to access the instance. Valid values are "http"
     * and "https".
     * Otherwise, a default value of "http" is used.
     */
    public static final String HTTP_PROTOCOL = System.getProperty("test.couch.http", "http");

    /**
     * If true, use cookie authentication (via the HTTP interceptors)
     * instead of basic authentication to access the instance.
     */
    public static final Boolean COOKIE_AUTH = Boolean.valueOf(System.getProperty("test.couch.use" +
            ".cookies", Boolean.FALSE.toString()));

    /*
     * These options control whether we should ignore certain tests because they are not valid
     * in all contexts.
     */

    /**
     * If true, do not run tests which expect a /_compact endpoint to exist. The Cloudant service
     * does not expose this endpoint as compaction is performed automatically.
     */
    public static final Boolean IGNORE_COMPACTION = Boolean.valueOf(
            System.getProperty("test.couch.ignore.compaction", Boolean.FALSE.toString()));

    /**
     * If true, do not run tests like testCookieAuthWithoutRetry.
     */
    public static final Boolean IGNORE_AUTH_HEADERS = Boolean.valueOf(
            System.getProperty("test.couch.ignore.auth.headers", Boolean.TRUE.toString()));

}
