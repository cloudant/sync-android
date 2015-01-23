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
import com.cloudant.sync.util.Misc;

import com.google.common.base.Strings;
import com.google.common.io.Resources;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.params.ConnManagerParams;
import org.apache.http.conn.params.ConnPerRoute;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HttpRequests {

    public static final int CONN_PER_ROUT = 4;
    private static Logger logger = Logger.getLogger(HttpRequests.class.getCanonicalName());
    private HttpClient httpClient;
    private JSONHelper jsonHelper;

    private BasicHttpContext context;
    private boolean debugging;

    private Map<String, String> requestHeaders;

    /**
     * Create a HttpRequests object with requests constants, such as username, password and other
     * HttpParameters. The user agent parameter will be set internally so it is constant
     *
     * @param params Parameters for the HttpConnection
     * @param userInfo Username:Password string for Basic Auth
     */
    public HttpRequests(HttpParams params, String userInfo, Map<String, String> customHeaders){
        this.jsonHelper = new JSONHelper();
        this.context = new BasicHttpContext();

        // Always want to send userAgent param as us and set routing information
        // Set the specified user agent and register standard protocols.
        HttpProtocolParams.setUserAgent(params, this.getUserAgent());

        ConnManagerParams.setMaxConnectionsPerRoute(params, new ConnPerRoute() {
            @Override
            public int getMaxForRoute(HttpRoute route) {
                return CONN_PER_ROUT;
            }
        });

        ClientConnectionManager manager = getClientConnectionManager(params);
        DefaultHttpClient httpClient = new DefaultHttpClient(manager,params);
        addDebuggingInterceptor(httpClient);
        this.httpClient = httpClient;

        String authHeaderValue = null;
        if (!Strings.isNullOrEmpty(userInfo)) {
            Base64 base64 = new Base64();
            authHeaderValue = "Basic " + new String(base64.encode(userInfo.getBytes()));
        }

        this.requestHeaders = new HashMap<String, String>();
        // copy custom headers and fold case to lowercase
        if (customHeaders != null) {
            for (Map.Entry<String, String> header : customHeaders.entrySet()) {
                this.requestHeaders.put(header.getKey().toLowerCase(), header.getValue());
            }
        }

        // Insert authHeaderValue if there isn't already an Authorization header
        if (authHeaderValue != null) {
            if (this.requestHeaders.containsKey("authorization")) {
                logger.warning(
                        "custom header \"authorization\" and username/password are both set; will use the value from custom header"
                );
            } else {
                this.requestHeaders.put("authorization", authHeaderValue);
            }
        }
    }

    // HTTP GET Requests
    public InputStream get(URI uri) {
        HttpGet get = new HttpGet(uri);
        get.addHeader("Accept", "application/json");
        return get(get);
    }

    public InputStream getCompressed(URI uri) {
        HttpGet get = new HttpGet(uri);
        get.addHeader("Accept-Encoding", "gzip");
        return get(get);
    }

    public InputStream get(HttpGet httpGet) {
        HttpResponse response = executeRequest(httpGet);
        return getStream(response);
    }

    public HttpResponse getResponse(URI uri) {
        HttpGet get = new HttpGet(uri);
        get.addHeader("Accept", "application/json");
        return getResponse(get);
    }

    public HttpResponse getResponse(HttpGet httpGet) {
        return executeRequest(httpGet);
    }

    // HTTP DELETE Requests
    public InputStream delete(URI uri) {
        HttpDelete delete = new HttpDelete(uri);
        return delete(delete);
    }

    public InputStream delete(HttpDelete delete) {
        HttpResponse response = deleteResponse(delete);
        return getStream(response);
    }

    public HttpResponse deleteResponse(URI uri) {
        HttpDelete delete = new HttpDelete(uri);
        return deleteResponse(delete);
    }

    public HttpResponse deleteResponse(HttpDelete delete) {
        return executeRequest(delete);
    }

    // HTTP PUT Requests
    public InputStream put(URI uri) {
        HttpPut put = new HttpPut(uri);
        return getStream(this.putResponse(put));
    }

    public InputStream put(URI uri, String payload) {
        HttpPut put = new HttpPut(uri);
        put.addHeader("Accept", "application/json");
        setEntity(put, payload);
        return getStream(this.putResponse(put));
    }

    public InputStream put(URI uri, String contentType, byte[] payload) {
        HttpPut put = new HttpPut(uri);
        put.addHeader("Accept", "application/json");
        setEntity(put, contentType, payload);
        return getStream(this.putResponse(put));
    }

    public InputStream putStream(URI uri, String contentType, InputStream is, long contentLength) {
        HttpPut put = new HttpPut(uri);
        put.addHeader("Accept", "application/json");
        setEntity(put, contentType, is, contentLength);
        return getStream(this.putResponse(put));
    }

    public HttpResponse putResponse(HttpPut put) {
        return executeRequest(put);
    }

    // HTTP POST Requests
    public InputStream post(URI uri, String payload) {
        HttpResponse response = postResponse(uri, payload);
        return getStream(response);
    }

    public HttpResponse postResponse(URI uri, String payload) {
        HttpPost post = new HttpPost(uri);
        setEntity(post, payload);
        return executeRequest(post);
    }

    // HTTP HEAD Requests
    public HttpResponse head(URI uri) {
        HttpHead head = new HttpHead(uri);
        return executeRequest(head);
    }


    /**
     * Executes a HTTP request.
     *
     * @param request The HTTP request to execute.
     * @return {@link org.apache.http.HttpResponse}
     */
    private HttpResponse executeRequest(HttpRequestBase request) {
        try {
            if (requestHeaders != null) {
                for (Map.Entry<String, String> header : requestHeaders.entrySet()) {
                    request.setHeader(header.getKey(), header.getValue());
                }
            }
            //log the request
            logger.info(request.toString());
            HttpResponse response = httpClient.execute(request, context);
            validate(request, response);
            logger.info(response.getStatusLine().toString());
            if(logger.isLoggable(Level.FINER)) {
                for (Header h : response.getAllHeaders()) {
                    logger.finer(h.toString());
                }
            }
            return response;
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Aborting request", e);
            request.abort();
            throw new ServerException(e);
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



    private static final String default_user_agent = "CloudantSync";
    private static String userAgent = null;
    public String getUserAgent() {
        if(userAgent == null) {
            String ua = this.getUserAgentFromResource();
            if(Misc.isRunningOnAndroid()) {
                try {
                    Class c = Class.forName("android.os.Build$VERSION");
                    String codename = (String)c.getField("CODENAME").get(null);
                    int sdkInt = c.getField("SDK_INT").getInt(null);
                    userAgent = String.format("%s Android %s %d", ua, codename, sdkInt);
                } catch (Exception e) {
                    userAgent = String.format("%s Android unknown version", ua);
                }
            } else {
                userAgent = String.format("%s (%s; %s; %s)",
                        ua,
                        System.getProperty("os.arch"),
                        System.getProperty("os.name"),
                        System.getProperty("os.version"));
            }
        }
        return userAgent;
    }

    private String getUserAgentFromResource() {
        final URL url = getClass().getClassLoader().getResource("mazha.properties");
        final Properties properties = new Properties();
        try {
            properties.load(Resources.newInputStreamSupplier(url).getInput());
            return properties.getProperty("user.agent", default_user_agent);
        } catch (Exception ioException) {
            return default_user_agent;
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

    protected void setEntity(HttpEntityEnclosingRequestBase httpRequest, String contentType, byte[] data) {
        ByteArrayEntity entity = new ByteArrayEntity(data);
        entity.setContentType(contentType);
        httpRequest.setEntity(entity);
    }

    protected void setEntity(HttpEntityEnclosingRequestBase httpRequest, String contentType, InputStream is, long contentLength) {
        InputStreamEntity entity = new InputStreamEntity(is, contentLength);
        entity.setContentType(contentType);
        httpRequest.setEntity(entity);
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
