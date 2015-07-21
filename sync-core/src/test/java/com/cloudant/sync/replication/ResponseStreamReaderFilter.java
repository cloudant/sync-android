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

package com.cloudant.sync.replication;

import com.cloudant.http.HttpConnection;
import com.cloudant.http.HttpConnectionFilterContext;
import com.cloudant.http.HttpConnectionResponseFilter;

import java.io.IOException;

/**
 * This response filter reads the data stream from the server in order to
 * force an IO error when Jackson attempts to read the stream to handle the content.
 */
public class ResponseStreamReaderFilter implements HttpConnectionResponseFilter {
    @Override
    public HttpConnectionFilterContext filterResponse(HttpConnectionFilterContext context) {


        HttpConnection connection = context.connection;
        try {
            connection.responseAsString();
        } catch (IOException e){
            e.printStackTrace();
        }


        return context;
    }
}
