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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CouchConfig {

    // The root URI for the database
    // This could be in the form http://host/database-name
    // but is treated as an opaque URI to correctly handle
    // situations where the request is forwarded eg through
    // a reverse proxy
    private URI rootUri;

    // Timeout to wait for a response, in milliseconds. Defaults to 0 (no timeout).
	private int socketTimeout = 30000;

    // Timeout to establish a connection, in milliseconds. Defaults to 0 (no timeout).
    private int connectionTimeout = 30000;

    // Max connections.
    private int maxConnections = 5;

    private int bufferSize = 1024 * 8;

    private boolean staleConnectionCheckingEnabled = Boolean.FALSE;

    private boolean handleRedirectEnabled = Boolean.FALSE;

    // Optional custom headers
    private Map<String, String> customHeaders;

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

    public CouchConfig(URI rootUri) {
        this.rootUri = rootUri;
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

    public Map<String, String> getCustomHeaders() {
        return customHeaders;
    }

    public void setCustomHeaders(Map<String, String> customHeaders) {
        List<String> prohibitedHeaders = Arrays.asList("www-authenticate", "host", "connection",
                "content-type", "accept", "content-length");
        for (Map.Entry<String, String> header : customHeaders.entrySet()) {
            if (prohibitedHeaders.contains(header.getKey().toLowerCase())) {
                throw new IllegalArgumentException("Bad optional HTTP header: " + header);
            }
        }
        this.customHeaders = customHeaders;
    }

    public URI getRootUri() {
        return rootUri;
    }
}
