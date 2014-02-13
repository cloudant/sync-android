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
import com.cloudant.sync.replication.ReplicatorURIUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;

public class ReplicatorURIUtilsTest {

    @Test
    public void extractCouchConfig_noPort_useDefaultPort()
            throws
            Exception {
        URI uri = new URI("https://127.0.0.1/db1");
        CouchConfig config = ReplicatorURIUtils.extractCouchConfig(uri);
        Assert.assertEquals("https", config.getProtocol());
        Assert.assertEquals("", config.getUsername());
        Assert.assertEquals("", config.getPassword());
        Assert.assertEquals("127.0.0.1", config.getHost());
        Assert.assertEquals(443, config.getPort());
    }

    @Test(expected = IllegalArgumentException.class)
    public void extractCouchConfig_noProtocol_exception()
            throws
            Exception {
        URI uri = new URI("127.0.0.1/db1");
        ReplicatorURIUtils.extractCouchConfig(uri);
    }

    @Test
    public void extractCouchConfig_slashInDbName_allInfoShouldBeExtractedCorrect()
            throws
            Exception {
        URI uri = new URI("http://127.0.0.1:5984/db1/db2");
        CouchConfig config = ReplicatorURIUtils.extractCouchConfig(uri);
        Assert.assertEquals("http", config.getProtocol());
        Assert.assertEquals("", config.getUsername());
        Assert.assertEquals("", config.getPassword());
        Assert.assertEquals("127.0.0.1", config.getHost());
        Assert.assertEquals(5984, config.getPort());
    }

    @Test
    public void extractCouchConfig_localHostWoUserPassword_allInfoShouldBeExtractedCorrect()
            throws
            Exception {
        URI uri = new URI("http://localhost:5984/db1");
        CouchConfig config = ReplicatorURIUtils.extractCouchConfig(uri);
        Assert.assertEquals("http", config.getProtocol());
        Assert.assertEquals("", config.getUsername());
        Assert.assertEquals("", config.getPassword());
        Assert.assertEquals("localhost", config.getHost());
        Assert.assertEquals(5984, config.getPort());
    }

    @Test
    public void extractCouchConfig_localHostWithUserPassword_allInfoShouldBeExtractedCorrect()
            throws
            Exception {
        URI uri = new URI("http://username:password@127.0.0.1:5984/db1");
        CouchConfig config = ReplicatorURIUtils.extractCouchConfig(uri);
        Assert.assertEquals("http", config.getProtocol());
        Assert.assertEquals("username", config.getUsername());
        Assert.assertEquals("password", config.getPassword());
        Assert.assertEquals("127.0.0.1", config.getHost());
        Assert.assertEquals(5984, config.getPort());
    }

    @Test
    public void extractCouchConfig_cloudant_allInfoShouldBeExtractedCorrect() throws Exception {
        URI uri = new URI("https://username:password@username.cloudant.com:443/db1");
        CouchConfig config = ReplicatorURIUtils.extractCouchConfig(uri);
        Assert.assertEquals("https", config.getProtocol());
        Assert.assertEquals("username", config.getUsername());
        Assert.assertEquals("password", config.getPassword());
        Assert.assertEquals("username.cloudant.com", config.getHost());
        Assert.assertEquals(443, config.getPort());
    }

    @Test
    public void extractCouchConfig_colonInPassword_allInfoShouldBeExtractedCorrect() throws
            Exception {
        URI uri = new URI("https://username:pass:word@username.cloudant.com:443/db1");
        CouchConfig config = ReplicatorURIUtils.extractCouchConfig(uri);
        Assert.assertEquals("https", config.getProtocol());
        Assert.assertEquals("username", config.getUsername());
        Assert.assertEquals("pass:word", config.getPassword());
        Assert.assertEquals("username.cloudant.com", config.getHost());
        Assert.assertEquals(443, config.getPort());
    }

    @Test
    public void extractCouchConfig_defaultHttpPort_80() throws Exception{
        URI uri = new URI("http://username:password@username.cloudant.com/db1");
        CouchConfig config = ReplicatorURIUtils.extractCouchConfig(uri);
        Assert.assertEquals("http", config.getProtocol());
        Assert.assertEquals("username", config.getUsername());
        Assert.assertEquals("password", config.getPassword());
        Assert.assertEquals("username.cloudant.com", config.getHost());
        Assert.assertEquals(80, config.getPort());
    }

    @Test
    public void extractCouchConfig_defaultHttpsPort_443() throws Exception{
        URI uri = new URI("https://username:password@username.cloudant.com/db1");
        CouchConfig config = ReplicatorURIUtils.extractCouchConfig(uri);
        Assert.assertEquals("https", config.getProtocol());
        Assert.assertEquals("username", config.getUsername());
        Assert.assertEquals("password", config.getPassword());
        Assert.assertEquals("username.cloudant.com", config.getHost());
        Assert.assertEquals(443, config.getPort());
    }

    @Test(expected = IllegalArgumentException.class)
    public void extractDbName_slashInDbName_exception() throws Exception {
        URI uri = new URI("http://127.0.0.1:5984/db1/db2");
        ReplicatorURIUtils.extractDatabaseName(uri);
    }

    @Test
    public void extractDbName_localHostWoUserPassword_dbName() throws Exception {
        URI uri = new URI("http://127.0.0.1:5984/db1");
        String db = ReplicatorURIUtils.extractDatabaseName(uri);
        Assert.assertEquals("db1", db);
    }

    @Test
    public void extractDbName_localHostWithUserPassword_dbName() throws Exception {
        URI uri = new URI("http://username:password@127.0.0.1:5984/db1");
        String db = ReplicatorURIUtils.extractDatabaseName(uri);
        Assert.assertEquals("db1", db);
    }

    @Test
    public void extractDbName_cloudant_dbName() throws Exception {
        URI uri = new URI("https://username:password@username.cloudant.com/db1");
        String db = ReplicatorURIUtils.extractDatabaseName(uri);
        Assert.assertEquals("db1", db);
    }

}
