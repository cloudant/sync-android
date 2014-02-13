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


import com.cloudant.mazha.json.JSONHelper;
import org.apache.commons.io.IOUtils;
import org.apache.http.*;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.*;
import org.apache.http.client.params.HttpClientParams;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.params.ConnManagerParams;
import org.apache.http.conn.params.ConnPerRoute;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;

public class HttpRequests {

    public static final int CONN_PER_ROUT = 4;
    private HttpClient httpClient;
    private JSONHelper jsonHelper;

    private HttpHost host;
    private BasicHttpContext context;
    private boolean debugging;

    public HttpRequests(CouchConfig config) {
        this.httpClient = createHttpClient(config);
        this.jsonHelper = new JSONHelper();
    }

    // HTTP GET Requests
    InputStream get(URI uri) {
        HttpGet get = new HttpGet(uri);
        get.addHeader("Accept", "application/json");
        return get(get);
    }

    InputStream get(HttpGet httpGet) {
        HttpResponse response = executeRequest(httpGet);
        return getStream(response);
    }

    HttpResponse getResponse(URI uri) {
        HttpGet get = new HttpGet(uri);
        get.addHeader("Accept", "application/json");
        return getResponse(get);
    }

    HttpResponse getResponse(HttpGet httpGet) {
        return executeRequest(httpGet);
    }

    // HTTP DELETE Requests
    InputStream delete(URI uri) {
        HttpDelete delete = new HttpDelete(uri);
        return delete(delete);
    }

    InputStream delete(HttpDelete delete) {
        HttpResponse response = deleteResponse(delete);
        return getStream(response);
    }

    HttpResponse deleteResponse(URI uri) {
        HttpDelete delete = new HttpDelete(uri);
        return deleteResponse(delete);
    }

    HttpResponse deleteResponse(HttpDelete delete) {
        return executeRequest(delete);
    }

    // HTTP PUT Requests
    InputStream put(URI uri) {
        HttpPut put = new HttpPut(uri);
        return getStream(this.putResponse(put));
    }

    InputStream put(URI uri, String payload) {
        HttpPut put = new HttpPut(uri);
        put.addHeader("Accept", "application/json");
        setEntity(put, payload);
        return getStream(this.putResponse(put));
    }

    HttpResponse putResponse(HttpPut put) {
        return executeRequest(put);
    }

    // HTTP POST Requests
    InputStream post(URI uri, String payload) {
        HttpResponse response = postResponse(uri, payload);
        return getStream(response);
    }

    HttpResponse postResponse(URI uri, String payload) {
        HttpPost post = new HttpPost(uri);
        setEntity(post, payload);
        return executeRequest(post);
    }

    // HTTP HEAD Requests
    HttpResponse head(URI uri) {
        HttpHead head = new HttpHead(uri);
        return executeRequest(head);
    }


    /**
     * Executes a HTTP request.
     *
     * @param request The HTTP request to execute.
     * @return {@link org.apache.http.HttpResponse}
     */
    protected HttpResponse executeRequest(HttpRequestBase request) {
        try {
            HttpResponse response = httpClient.execute(host, request, context);
            validate(request, response);
            return response;
        } catch (IOException e) {
            request.abort();
            throw new ServerException(e);
        }
    }

    /**
     * @return {@link org.apache.http.impl.client.DefaultHttpClient} instance.
     */
    private HttpClient createHttpClient(CouchConfig config) {
        try {
            HttpParams params = getHttpConnectionParams(config);
            ClientConnectionManager manager = getClientConnectionManager(params);

            DefaultHttpClient httpClient = new DefaultHttpClient(manager, params);
            addHttpBasicAuth(config, httpClient);
            addDebuggingInterceptor(httpClient);

            this.host = new HttpHost(config.getHost(), config.getPort(), config.getProtocol());
            this.context = new BasicHttpContext();

            return httpClient;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private ClientConnectionManager getClientConnectionManager(HttpParams params) {
        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("http",
                PlainSocketFactory.getSocketFactory(), 80));
        schemeRegistry.register(new Scheme("https",
                SSLSocketFactory.getSocketFactory(), 443));
        return new ThreadSafeClientConnManager(params, schemeRegistry);
    }

    private HttpParams getHttpConnectionParams(CouchConfig config) {
        BasicHttpParams params = new BasicHttpParams();

        // Turn off stale checking.  Our connections break all the time anyway,
        // and it's not worth it to pay the penalty of checking every time.
        HttpConnectionParams.setStaleCheckingEnabled(params, config.isStaleConnectionCheckingEnabled());
        HttpConnectionParams.setConnectionTimeout(params, config.getConnectionTimeout());
        HttpConnectionParams.setSoTimeout(params, config.getSocketTimeout());
        HttpConnectionParams.setSocketBufferSize(params, config.getBufferSize());

        // Don't handle redirects -- return them to the caller.  Our code
        // often wants to re-POST after a redirect, which we must do ourselves.
        HttpClientParams.setRedirecting(params, config.isHandleRedirectEnabled());

        // Set the specified user agent and register standard protocols.
        HttpProtocolParams.setUserAgent(params, config.getUserAgent());

        ConnManagerParams.setMaxConnectionsPerRoute(params, new ConnPerRoute() {
            @Override
            public int getMaxForRoute(HttpRoute route) {
                return CONN_PER_ROUT;
            }
        });
        return params;
    }

    private void addHttpBasicAuth(CouchConfig config, DefaultHttpClient httpclient) {
        // basic authentication
        if (config.getUsername() != null && config.getPassword() != null) {
            httpclient.getCredentialsProvider().setCredentials(
                    new AuthScope(config.getHost(),
                            config.getPort()),
                    new UsernamePasswordCredentials(config.getUsername(),
                            config.getPassword()));
            // Auth cache?
//            AuthCache authCache = new BasicAuthCache();
//            BasicScheme basicAuth = new BasicScheme();
//            authCache.put(host, basicAuth);
//            context.setAttribute(ClientContext.AUTH_CACHE, authCache);
        }
    }

    private void addDebuggingInterceptor(DefaultHttpClient httpclient) {

        if (!isDebugging()) {
            return;
        }

        // request interceptor
        httpclient.addRequestInterceptor(new HttpRequestInterceptor() {
            public void process(
                    final HttpRequest request,
                    final HttpContext context) throws IOException {
                System.out.println(">> " + request.getRequestLine());

            }
        });

        // response interceptor
        httpclient.addResponseInterceptor(new HttpResponseInterceptor() {
            public void process(
                    final HttpResponse response,
                    final HttpContext context) throws IOException {
                System.out.println("<< Status: " + response.getStatusLine().getStatusCode());
            }
        });
    }

    /**
     * General validation of response, and only checks 404 error, and it leaves caller to check specific error code.
     */
    private void validate(HttpRequest request, HttpResponse response) throws CouchException {
        int code = response.getStatusLine().getStatusCode();
        if (code == 200 || code == 201 || code == 202) { // success (ok | created | accepted)
            return;
        }

        String msg = String.format("Request: %s << Status: %s (%s) ", request.getRequestLine(), code, response.getStatusLine().getReasonPhrase());
        if (code == HttpStatus.SC_NOT_FOUND) {
            throw new NoResourceException(msg);
        } else {
            CouchException exception = null;
            try {
                exception = getCouchErrorFromResponse(response);
            } catch (Exception e) {
                throw new ServerException(msg);
            }
            throw exception;
        }
    }

    private CouchException getCouchErrorFromResponse(HttpResponse response) throws IOException {
        int code = response.getStatusLine().getStatusCode();
        InputStream is = null;
        try {
            is = response.getEntity().getContent();
            return this.jsonHelper.fromJson(new InputStreamReader(is), CouchException.class);
        } catch (IOException e) {
            return new CouchException("Unknown error", code);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    protected void setEntity(HttpEntityEnclosingRequestBase httpRequest, String json) {
        try {
            StringEntity entity = new StringEntity(json, "UTF-8");
            entity.setContentType("application/json");
            httpRequest.setEntity(entity);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    InputStream getStream(HttpResponse response) {
        try {
            return response.getEntity().getContent();
        } catch (Exception e) {
            throw new ServerException(e);
        }
    }

    /**
     * Shuts down the connection manager used by this client instance.
     */
    public void shutdown() {
        this.httpClient.getConnectionManager().shutdown();
    }

    public boolean isDebugging() {
        return debugging;
    }

    public void setDebugging(boolean debugging) {
        this.debugging = debugging;
    }
}
