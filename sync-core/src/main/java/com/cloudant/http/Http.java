//  Copyright (c) 2015 IBM Cloudant. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.cloudant.http;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Created by tomblench on 23/03/15.
 */

/**
 * Factory methods for obtaining <code>HttpConnection</code>s.
 *
 * @see com.cloudant.http.HttpConnection
 */
public class Http {

    // high level http operations, URL and URI flavoured

    public static HttpConnection GET(URL url)
    {
        return connect("GET", url, null);
    }

    public static HttpConnection GET(URI uri)
    {
        return connect("GET", uri, null);
    }

    public static HttpConnection PUT(URL url,
                                     String contentType)
    {
        return connect("PUT", url, contentType);
    }

    public static HttpConnection PUT(URI uri,
                                     String contentType)
    {
        return connect("PUT", uri, contentType);
    }

    public static HttpConnection POST(URL url,
                                      String contentType)
    {
        return connect("POST", url, contentType);
    }

    public static HttpConnection POST(URI uri,
                                      String contentType)
    {
        return connect("POST", uri, contentType);
    }

    public static HttpConnection DELETE(URL url)
    {
        return connect("DELETE", url, null);
    }

    public static HttpConnection DELETE(URI uri)
    {
        return connect("DELETE", uri, null);
    }

    public static HttpConnection HEAD(URL url)
    {
        return connect("HEAD", url, null);
    }

    public static HttpConnection HEAD(URI uri)
    {
        return connect("HEAD", uri, null);
    }

    // low level http operations

    public static HttpConnection connect(String requestMethod,
                                         URL url,
                                         String contentType) {
        return new HttpConnection(requestMethod, url, contentType);
    }

    public static HttpConnection connect(String requestMethod,
                                         URI uri,
                                         String contentType) {
        try {
            return new HttpConnection(requestMethod, uri.toURL(), contentType);
        } catch (MalformedURLException mue) {
            return null;
        }
    }


}
