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

import com.cloudant.android.Base64OutputStreamFactory;

import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;

/**
 * Created by tomblench on 23/03/15.
 */

/**
 * <p>
 * A wrapper for <code>HttpURLConnection</code>s.
 * </p>
 *
 * <p>
 * Provides some convenience methods for making requests and sending/receiving data as streams,
 * strings, or byte arrays.
 * </p>
 *
 * <p>
 * Typical usage:
 * </p>
 *
 * <pre>
 * HttpConnection hc = new HttpConnection("POST", "application/json", new URL("http://somewhere"));
 * hc.setUsername("user");
 * hc.setPassword("pass");
 * hc.requestProperties.put("x-some-header", "some-value");
 * hc.setRequestBody("{\"hello\": \"world\"});
 * String result = hc.execute().responseAsString();
 * // get the underlying HttpURLConnection if you need to do something a bit more advanced:
 * int response = hc.getConnection().getResponseCode();
 * </pre>
 *
 * <p>
 * <b>Important:</b> this class is not thread-safe and <code>HttpConnection</code>s should not be
 * shared across threads.
 * </p>
 *
 * @see java.net.HttpURLConnection
 */
public class HttpConnection  {

    private final String requestMethod;
    public final URL url;
    private final String contentType;

    // created in executeInternal
    private HttpURLConnection connection;

    // set by the various setRequestBody() methods
    private InputStream input;
    private long inputLength;

    public final HashMap<String, String> requestProperties;

    public HttpConnection(String requestMethod,
                          URL url,
                          String contentType) {
        this.requestMethod = requestMethod;
        this.url = url;
        this.contentType = contentType;
        this.requestProperties = new HashMap<String, String>();
    }

    /**
     * Set the String of request body data to be sent to the server.
     * @param input String of request body data to be sent to the server
     */
    public void setRequestBody(final String input) {
        this.input = new ByteArrayInputStream(input.getBytes());
        // input is in bytes, not characters
        this.inputLength = input.getBytes().length;
    }

    /**
     * Set the byte array of request body data to be sent to the server.
     * @param input byte array of request body data to be sent to the server
     */
    public void setRequestBody(final byte[] input) {
        this.input = new ByteArrayInputStream(input);
        this.inputLength = input.length;
    }

    /**
     * Set the InputStream of request body data to be sent to the server.
     * @param input InputStream of request body data to be sent to the server
     */
    public void setRequestBody(InputStream input) {
        this.input = input;
        // -1 signals inputLength unknown
        this.inputLength = -1;
    }

    /**
     * Set the InputStream of request body data, of known length, to be sent to the server.
     * @param input InputStream of request body data to be sent to the server
     * @param inputLength Length of request body data to be sent to the server, in bytes
     */
    public void setRequestBody(InputStream input, long inputLength) {
        this.input = input;
        this.inputLength = inputLength;
    }

    /**
     * <p>
     * Execute request without returning data from server.
     * </p>
     * <p>
     * Call {@code responseAsString}, {@code responseAsBytes}, or {@code responseAsInputStream}
     * after {@code execute} if the response body is required.
     * </p>
     * @throws IOException
     */
    public HttpConnection execute() throws IOException {
        System.setProperty("http.keepAlive", "false");
        connection = (HttpURLConnection) url.openConnection();
        for (String key : requestProperties.keySet()) {
            connection.setRequestProperty(key, requestProperties.get(key));
        }
        // always read the result, so we can retrieve the HTTP response code
        connection.setDoInput(true);
        connection.setRequestMethod(requestMethod);
        if (contentType != null) {
            connection.setRequestProperty("Content-type", contentType);
        }
        if (url.getUserInfo() != null) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStream bos = Base64OutputStreamFactory.get(baos);
            bos.write(url.getUserInfo().getBytes());
            bos.flush();
            bos.close();
            String encodedAuth = baos.toString();
            connection.setRequestProperty("Authorization", String.format("Basic %s", encodedAuth));
        }
        if (input != null) {
            connection.setDoOutput(true);
            if (inputLength != -1) {
                // TODO on 1.7 upwards this method takes a long, otherwise int
                connection.setFixedLengthStreamingMode((int)this.inputLength);
            } else {
                // TODO some situations where we can't do chunking, like multipart/related
                /// https://issues.apache.org/jira/browse/COUCHDB-1403
                connection.setChunkedStreamingMode(1024);
            }

            // See "8.2.3 Use of the 100 (Continue) Status" in http://tools.ietf.org/html/rfc2616
            // Attempting to write to the connection's OutputStream may cause an exception to be
            // thrown. This is useful because it avoids sending large request bodies (such as
            // attachments) if the server is going to reject our request. Reasons for rejecting
            // requests could be 401 Unauthorized (eg cookie needs to be refreshed), etc.
            connection.setRequestProperty("Expect", "100-continue");

            int bufSize = 1024;
            int nRead = 0;
            byte[] buf = new byte[bufSize];
            InputStream is = input;
            OutputStream os = connection.getOutputStream();

            while((nRead = is.read(buf)) >= 0) {
                os.write(buf, 0, nRead);
            }
            os.flush();
            // we do not call os.close() - on some JVMs this incurs a delay of several seconds
            // see http://stackoverflow.com/questions/19860436
        }
        // return ourselves to allow method chaining
        return this;
    }

    /**
     * <p>
     * Return response body data from server as a String.
     * </p>
     * <p>
     * <b>Important:</b> you must call <code>execute()</code> before calling this method.
     * </p>
     * @return String of response body data from server, if any
     * @throws IOException
     */
    public String responseAsString() throws IOException {
        if (connection == null) {
            throw new IOException("Attempted to read response from server before calling execute()");
        }
        InputStream is = connection.getInputStream();
        String string = IOUtils.toString(is);
        is.close();
        return string;
    }

    /**
     * <p>
     * Return response body data from server as a byte array.
     * </p>
     * <p>
     * <b>Important:</b> you must call <code>execute()</code> before calling this method.
     * </p>
     * @return Byte array of response body data from server, if any
     * @throws IOException
     */
    public byte[] responseAsBytes() throws IOException {
        if (connection == null) {
            throw new IOException("Attempted to read response from server before calling execute()");
        }
        InputStream is = connection.getInputStream();
        byte[] bytes = IOUtils.toByteArray(is);
        is.close();
        return bytes;
    }

    /**
     * <p>
     * Return response body data from server as an InputStream.
     * </p>
     * <p>
     * <b>Important:</b> you must call <code>execute()</code> before calling this method.
     * </p>
     * @return InputStream of response body data from server, if any
     * @throws IOException
     */
    public InputStream responseAsInputStream() throws IOException {
        if (connection == null) {
            throw new IOException("Attempted to read response from server before calling execute()");
        }
        InputStream is = connection.getInputStream();
        return is;
    }

    /**
     * Get the underlying HttpURLConnection object, allowing clients to set/get properties not
     * exposed here.
     * @return HttpURLConnection
     */
    public HttpURLConnection getConnection() {
        return connection;
    }

}
