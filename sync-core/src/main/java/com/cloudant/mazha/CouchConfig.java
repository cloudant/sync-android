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

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CouchConfig {

    // The root URI for the database
    // This could be in the form http://host/database-name
    // but is treated as an opaque URI to correctly handle
    // situations where the request is forwarded eg through
    // a reverse proxy
    private URI rootUri;

    // Optional custom headers
    private Map<String, String> customHeaders;

    public CouchConfig(URI rootUri) {
        this.rootUri = rootUri;
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
