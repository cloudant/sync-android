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

import java.net.HttpURLConnection;

/**

 A Request Interceptor is run before the request is made to the server. It can use headers to add support
 for other authentication methods, for example cookie authentication. See
 {@link CookieInterceptor#interceptRequest(HttpConnectionInterceptorContext)} for an example.


 Interceptors are executed in a pipeline and modify the context in a serial fashion.
 */


public interface HttpConnectionRequestInterceptor {

    /**
     * Intercept the request.
     * This method <strong>must not</strong> do any of the following:
     * <ul>
     *     <li>Return null</li>
     *     <li>Call methods on the underlying {@link java.net.HttpURLConnection} which
     *     initiate a request such as {@link HttpURLConnection#getResponseCode()}</li>
     * </ul>
     * @param context Input context
     * @return Output context
     */
    HttpConnectionInterceptorContext interceptRequest(HttpConnectionInterceptorContext context);

}
