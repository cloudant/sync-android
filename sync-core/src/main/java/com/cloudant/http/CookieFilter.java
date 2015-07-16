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

import com.cloudant.mazha.json.JSONHelper;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * Adds cookie authentication support to http requests.
 *
 * It does this by adding the cookie header for CouchDB
 * using request filtering pipeline in {@link HttpConnection}.
 *
 * If a response has a response code of 401, it will fetch a cookie from
 * the server using provided credentials and tell {@link HttpConnection} to reply
 * the request by setting {@link HttpConnectionFilterContext#replayRequest} property to true.
 *
 * If the request to get the cookie for use in future request fails with a 401 status code
 * (or any status that indicates client error) cookie authentication will not be attempted again.
 *
 *
 */
public  class CookieFilter implements HttpConnectionRequestFilter, HttpConnectionResponseFilter {

    private final static Logger logger = Logger.getLogger(CookieFilter.class.getCanonicalName());
    final String sessionRequestBody;
    private String cookie = null;
    private boolean shouldAttemptCookieRequest = true;
    private final String username;

    /**
     * Constructs a cookie filter.
     * @param username The username to use when getting the cookie
     * @param password The password to use when getting the cookie
     */
    public CookieFilter(String username, String password){
        this.sessionRequestBody = String.format("name=%s&password=%s",username,password);
        this.username = username;
    }

    @Override
    public HttpConnectionFilterContext filterRequest(HttpConnectionFilterContext context) {

        HttpURLConnection connection = context.connection.getConnection();

        if(shouldAttemptCookieRequest) {
            if (cookie == null) {
                cookie = getCookie(connection.getURL());
            }
            connection.setRequestProperty("Cookie", cookie);
        }

        return context;
    }

    @Override
    public HttpConnectionFilterContext filterResponse(HttpConnectionFilterContext context) {
        HttpURLConnection connection = context.connection.getConnection();
        try {
            if (context.connection.getConnection().getResponseCode() == 401) {
                //we need to get a new cookie
                cookie = getCookie(connection.getURL());
                //don't resend request, failed to get cookie
                if(cookie != null) {
                    context.replayRequest = true;
                } else {
                    context.replayRequest = false;
                }
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to get response code from request",e);
        }
        return context;

    }

     private String getCookie(URL url){
        try {
            URL sessionURL = new URL(String.format("%s://%s:%d/_session",
                    url.getProtocol(),
                    url.getHost(),
                    url.getPort()));

            HttpConnection conn = Http.POST(sessionURL, "application/x-www-form-urlencoded");
            conn.setRequestBody(sessionRequestBody);
            HttpURLConnection connection = conn.execute().getConnection();
            String cookieHeader = connection.getHeaderField("Set-Cookie");
            int responseCode = connection.getResponseCode();

            if(responseCode / 100 == 2){

                if(sessionHasStarted(connection.getInputStream())) {
                    return cookieHeader.substring(0, cookieHeader.indexOf(";"));
                } else {
                    return null;
                }

            } else if(responseCode == 401){
                shouldAttemptCookieRequest  = false;
                logger.severe("Credentials are incorrect, cookie authentication will not be" +
                        " attempted again by this filter object");
            } else if (responseCode / 100 == 5){
                logger.log(Level.SEVERE,
                        "Failed to get cookie from server, response code %s, cookie auth",
                        responseCode);
            }  else {
                // catch any other response code
                logger.log(Level.SEVERE,
                        "Failed to get cookie from server, response code %s, " +
                                "cookie authentication will not be attempted again",
                        responseCode);
                shouldAttemptCookieRequest = false;
            }

        } catch (MalformedURLException e) {
            logger.log(Level.SEVERE,"Failed to create URL for _session endpoint",e);
        } catch (UnsupportedEncodingException e) {
            logger.log(Level.SEVERE, "Failed to encode cookieRequest body", e);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to read cookie response header", e);
        }
        return null;
    }

    private boolean sessionHasStarted(InputStream responseStream){
        //check the response body
        JSONHelper jsonHelper = new JSONHelper();
        Map<String,Object> jsonResponse = jsonHelper.fromJson(new InputStreamReader(responseStream));

        // only check for ok:true, https://issues.apache.org/jira/browse/COUCHDB-1356
        // means we cannot check that the name returned is the one we sent.
       return jsonResponse.containsKey("ok") && (Boolean) jsonResponse.get("ok");

    }
}
