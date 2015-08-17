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
package com.cloudant.http;

/**
 * Created by tomblench on 30/03/15.
 */
public class HttpConnectionInterceptorContext {

    public boolean replayRequest;
    public final HttpConnection connection;

    /**
     * Constructor
     * @param connection HttpConnection
     */
    public HttpConnectionInterceptorContext(HttpConnection connection) {
        this.replayRequest = false;
        this.connection = connection;
    }

    /**
     * Shallow copy constructor
     * @param other Context to copy
     */
    public HttpConnectionInterceptorContext(HttpConnectionInterceptorContext other) {
        this.replayRequest = other.replayRequest;
        this.connection = other.connection;

    }

}
