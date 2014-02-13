/**
 * Copyright (C) 2013 Cloudant
 *
 * Copyright (C) 2011 Ahmed Yehia (ahmed.yehia.m@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudant.mazha;

import com.google.common.base.Strings;

import java.net.URI;
import java.net.URISyntaxException;

public class CouchConfig {

    private static final String user_agent = "Cloudant Sync";

    private static final String couchdb_protocol = "http";
    private static final String couchdb_host= "127.0.0.1";
    private static final int couchdb_port = 5984;
    private static final String couchdb_username= "";
    private static final String couchdb_password= "";

    // required
	private final String protocol;
	private final String host;
	private final int port;
	private final String username;
	private final String password;

	// optional
    private String userAgent = user_agent;

    // Timeout to wait for a response, in milliseconds. Defaults to 0 (no timeout).
	private int socketTimeout = 30000;

    // Timeout to establish a connection, in milliseconds. Defaults to 0 (no timeout).
    private int connectionTimeout = 30000;

    // Max connections.
    private int maxConnections = 5;

    private int bufferSize = 1024 * 8;

    private boolean staleConnectionCheckingEnabled = Boolean.FALSE;

    private boolean handleRedirectEnabled = Boolean.FALSE;

    public boolean isStaleConnectionCheckingEnabled() {
        return staleConnectionCheckingEnabled;
    }

    public void setStaleConnectionCheckingEnabled(boolean staleConnectionCheckingEnabled) {
        this.staleConnectionCheckingEnabled = staleConnectionCheckingEnabled;
    }

    public boolean isHandleRedirectEnabled() {
        return handleRedirectEnabled;
    }

    public void setHandleRedirectEnabled(boolean handleRedirectEnabled) {
        this.handleRedirectEnabled = handleRedirectEnabled;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public static CouchConfig defaultConfig() {
        CouchConfig config = new CouchConfig(
                couchdb_protocol,
                couchdb_host,
                couchdb_port,
                couchdb_username,
                couchdb_password
        );
        return config;
    }

	public CouchConfig(String protocol,
                       String host,
                       int port,
                       String username,
                       String password) {
		this.protocol = protocol;
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
	}

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String ua) {
        this.userAgent = ua;
    }

    public String getProtocol() {
		return protocol;
	}

	public String getHost() {
		return host;
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

	public int getSocketTimeout() {
		return socketTimeout;
	}

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	public int getMaxConnections() {
		return maxConnections;
	}

	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	public void setMaxConnections(int maxConnections) {
		this.maxConnections = maxConnections;
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
}
