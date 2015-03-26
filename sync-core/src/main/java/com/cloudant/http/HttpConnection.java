package com.cloudant.http;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by tomblench on 23/03/15.
 */

/**
 * A wrapper for HttpUrlConnections
 *
 * Provides some convenience methods and a rudimentary request/response filtering mechanism
 */
public class HttpConnection  {

    private final String requestMethod;
    private final String contentType;
    public final URL url;

    // created in executeInternal
    private HttpURLConnection connection;

    // optional & settable
    private String username;
    private String password;

    // set by the various setInput() methods
    private InputStream input;
    private long inputLength;

    public final HashMap<String, String> requestProperties;

    public HttpConnection(String requestMethod,
                                   String contentType,
                                   URL url) {
        this.requestMethod = requestMethod;
        this.contentType = contentType;
        this.url = url;
        this.requestProperties = new HashMap<String, String>();
    }

    public void setInput(final String input) {
        this.input = new ByteArrayInputStream(input.getBytes());
        // input is in bytes, not characters
        this.inputLength = input.getBytes().length;
    }

    public void setInput(final byte[] input) {
        this.input = new ByteArrayInputStream(input);
        this.inputLength = input.length;
    }

    public void setInput(InputStream input) {
        this.input = input;
    }

    public void setInput(InputStream input, long inputLength) {
        this.input = input;
        this.inputLength = inputLength;
    }

    private static final int maxRetries = 10;

    private void executeInternal() throws IOException {
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
        if (username != null) {
            String encodedAuth = Base64.encodeBase64String(
                    String.format("%s:%s", username, password != null ? password : "")
                            .getBytes());
            connection.setRequestProperty("Authorization", String.format("Basic %s", encodedAuth));
        }
        if (input != null) {
            connection.setDoOutput(true);
            if (inputLength != -1) {
                // TODO should inputlength be a long or int
                connection.setFixedLengthStreamingMode((int)this.inputLength);
            } else {
                // TODO some situations where we can't do chunking, like multipart/related
                /// https://issues.apache.org/jira/browse/COUCHDB-1403
                connection.setChunkedStreamingMode(1024);
            }
            connection.setRequestProperty("Expect", "100-continue");

            // this will throw an exception if it doesn't get a 100-continue
            int bufSize = 1024;
            int nRead = 0;
            byte[] buf = new byte[bufSize];
            InputStream is = input;
            OutputStream os = connection.getOutputStream();

            while((nRead = is.read(buf)) >= 0) {
                os.write(buf, 0, nRead);
            }
        }
    }

    public void execute() throws IOException {
        executeInternal();
    }

    public String executeToString() throws IOException {
        executeInternal();
        InputStream is = connection.getInputStream();
        String string = IOUtils.toString(is);
        is.close();
        return string;
    }

    public byte[] executeToBytes() throws IOException {
        executeInternal();
        InputStream is = connection.getInputStream();
        byte[] bytes = IOUtils.toByteArray(is);
        is.close();
        return bytes;
    }

    public InputStream executeToInputStream() throws IOException {
        executeInternal();
        InputStream is = connection.getInputStream();
        return is;
    }

    public HttpURLConnection getConnection() {
        return connection;
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
