/**
 * Copyright © 2013 Cloudant
 *
 * Copyright © 2011 Ahmed Yehia (ahmed.yehia.m@gmail.com)
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

package com.cloudant.sync.internal.mazha;

import com.cloudant.http.HttpConnectionRequestInterceptor;
import com.cloudant.http.HttpConnectionResponseInterceptor;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class CouchConfig {

    // The root URI for the database
    // This could be in the form http://host/database-name
    // but is treated as an opaque URI to correctly handle
    // situations where the request is forwarded eg through
    // a reverse proxy
    private URI rootUri;

    private List<HttpConnectionRequestInterceptor> requestInterceptors = new ArrayList
            <HttpConnectionRequestInterceptor>();
    private List<HttpConnectionResponseInterceptor> responseInterceptors = new ArrayList
            <HttpConnectionResponseInterceptor>();

    public CouchConfig(URI rootUri) {
        this.rootUri = rootUri;
    }

    public CouchConfig(URI rootUri,
                       List<HttpConnectionRequestInterceptor> requestInterceptors,
                       List<HttpConnectionResponseInterceptor> responseInterceptors) {
        this.rootUri = rootUri;
        this.requestInterceptors = requestInterceptors;
        this.responseInterceptors = responseInterceptors;
    }

    public List<HttpConnectionRequestInterceptor> getRequestInterceptors() {
        return requestInterceptors;
    }

    public void setRequestInterceptors(List<HttpConnectionRequestInterceptor> requestInterceptors) {
        this.requestInterceptors = requestInterceptors;
    }

    public List<HttpConnectionResponseInterceptor> getResponseInterceptors() {
        return responseInterceptors;
    }

    public void setResponseInterceptors(List<HttpConnectionResponseInterceptor>
                                                responseInterceptors) {
        this.responseInterceptors = responseInterceptors;
    }

    public URI getRootUri() {
        return rootUri;
    }
}
