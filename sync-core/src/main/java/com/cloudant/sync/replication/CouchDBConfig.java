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
import com.google.common.base.Strings;

import java.net.URI;
import java.net.URISyntaxException;

class CouchDBConfig {

    private final String host;
    private final String username;
    private final String password;
    private final int  port;

    private final String protocol;

    public CouchDBConfig(String protocol, String host, int port) {
        this(protocol, host, port, null, null);
    }

    public URI getURI(String db) throws URISyntaxException {
        if(Strings.isNullOrEmpty(this.getUsername())) {
            return new URI(this.getProtocol(), null, this.getHost(), this.getPort(), "/" + db,
                    null,
                    null);
        } else {
            return new URI(this.getProtocol(), this.getUsername() + ":" + this.getPassword(),
                    this.getHost(), this.getPort(), "/" + db, null, null);
        }
    }


    public CouchDBConfig(String protocol, String host, int port, String apiKey, String apiSecret) {
        this.host = host;
        this.port = port;
        this.protocol = protocol;
        this.username = apiKey;
        this.password = apiSecret;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getHost() {
        return host;
    }

    public String getProtocol() {
        return protocol;
    }

    public CouchConfig getCouchDbProperties() {
        CouchConfig c = new CouchConfig(
                this.getProtocol(),
                this.getHost(),
                this.getPort(),
                this.getUsername(),
                this.getPassword());
        return c;
    }
}
