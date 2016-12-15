/*
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
import com.cloudant.http.interceptors.CookieInterceptor;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.logging.Logger;

public class CouchConfig {

    // The root URI for the database
    // This could be in the form http://host/database-name
    // but is treated as an opaque URI to correctly handle
    // situations where the request is forwarded eg through
    // a reverse proxy
    private URI rootUri;

    private String username;

    private String password;

    private List<HttpConnectionRequestInterceptor> requestInterceptors = new ArrayList
            <HttpConnectionRequestInterceptor>();
    private List<HttpConnectionResponseInterceptor> responseInterceptors = new ArrayList
            <HttpConnectionResponseInterceptor>();

    public CouchConfig(URI rootUri) {
        this(rootUri,
                Collections.<HttpConnectionRequestInterceptor>emptyList(),
                Collections.<HttpConnectionResponseInterceptor>emptyList(),
                null,
                null);
    }

    public CouchConfig(URI rootUri,
                       List<HttpConnectionRequestInterceptor> requestInterceptors,
                       List<HttpConnectionResponseInterceptor> responseInterceptors,
                       String username,
                       String password) {
        this.rootUri = rootUri;
        this.requestInterceptors = requestInterceptors;
        this.responseInterceptors = responseInterceptors;
        this.username = username;
        this.password = password;

    }


    public List<HttpConnectionRequestInterceptor> getRequestInterceptors(boolean includeCookie) {
        CookieInterceptor cookieInterceptor = buildCookieInterceptor();
        if (includeCookie && cookieInterceptor != null) {
            List<HttpConnectionRequestInterceptor> requestInterceptors = new ArrayList
                    <HttpConnectionRequestInterceptor>();
            requestInterceptors.addAll(this.requestInterceptors);
            requestInterceptors.add(cookieInterceptor);
            return requestInterceptors;
        }
        return requestInterceptors;
    }

    public List<HttpConnectionResponseInterceptor> getResponseInterceptors(boolean includeCookie) {
        CookieInterceptor cookieInterceptor = buildCookieInterceptor();
        if (includeCookie && cookieInterceptor != null) {
            List<HttpConnectionResponseInterceptor> requestInterceptors = new ArrayList
                    <HttpConnectionResponseInterceptor>();
            requestInterceptors.addAll(this.responseInterceptors);
            requestInterceptors.add(cookieInterceptor);
            return requestInterceptors;
        }
        return responseInterceptors;
    }

    private CookieInterceptor buildCookieInterceptor() {
        if (username != null && password != null) {
            String path = rootUri.getRawPath() == null ? "" : rootUri.getRawPath();

            if (path.length() > 0) {
                int index = path.lastIndexOf("/");
                if (index == path.length() - 1) {
                    // we need to go back one
                    path = path.substring(0, index);
                    index = path.lastIndexOf("/");
                }
                path = path.substring(0, index);
            }

            URI baseURI;
            try {
                baseURI = new URI(rootUri.getScheme(), null, rootUri.getHost(), rootUri.getPort()
                        , path, null, null);
                Logger.getLogger(this.getClass().getCanonicalName()).info(String.format(Locale.ENGLISH, "Cookie info: %s", baseURI));
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
            return new CookieInterceptor(username, password, baseURI.toString());
        }
        return null;
    }

    public List<HttpConnectionRequestInterceptor> getRequestInterceptors() {
        return this.getRequestInterceptors(true);
    }

    public void setRequestInterceptors(List<HttpConnectionRequestInterceptor> requestInterceptors) {
        this.requestInterceptors = requestInterceptors;
    }

    public List<HttpConnectionResponseInterceptor> getResponseInterceptors() {
        return this.getResponseInterceptors(true);
    }

    public void setResponseInterceptors(List<HttpConnectionResponseInterceptor>
                                                responseInterceptors) {
        this.responseInterceptors = responseInterceptors;
    }

    public URI getRootUri() {
        return rootUri;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

}
