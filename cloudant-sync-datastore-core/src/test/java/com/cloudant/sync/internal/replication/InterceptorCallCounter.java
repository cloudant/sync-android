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

package com.cloudant.sync.internal.replication;

import com.cloudant.http.HttpConnectionInterceptorContext;
import com.cloudant.http.HttpConnectionRequestInterceptor;
import com.cloudant.http.HttpConnectionResponseInterceptor;

/**
 * This request and response interceptor counts the number of times the interceptRequest and interceptResponse
 * methods get called.
 */
public class InterceptorCallCounter implements HttpConnectionRequestInterceptor, HttpConnectionResponseInterceptor {

    public int interceptorRequestTimesCalled = 0;
    public int interceptorResponseTimesCalled = 0;

    @Override
    public HttpConnectionInterceptorContext interceptRequest(HttpConnectionInterceptorContext context) {
        interceptorRequestTimesCalled++;
        return context;
    }

    @Override
    public HttpConnectionInterceptorContext interceptResponse(HttpConnectionInterceptorContext context) {
        interceptorResponseTimesCalled++;
        return context;
    }
}
