package com.cloudant.http;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Created by tomblench on 23/03/15.
 */
public class Http {

    // high level http operations, URL and URI flavoured

    public static HttpConnection GET(URL url)
    {
        return connect("GET", null, url);
    }

    public static HttpConnection GET(URI uri)
    {
        return connect("GET", null, uri);
    }

    public static HttpConnection PUT(String contentType,
                                     URL url)
    {
        return connect("PUT", contentType, url);
    }

    public static HttpConnection PUT(String contentType,
                                     URI uri)
    {
        return connect("PUT", contentType, uri);
    }

    public static HttpConnection POST(String contentType,
                                     URL url)
    {
        return connect("POST", contentType, url);
    }

    public static HttpConnection POST(String contentType,
                                      URI uri)
    {
        return connect("POST", contentType, uri);
    }

    public static HttpConnection DELETE(URL url)
    {
        return connect("DELETE", null, url);
    }

    public static HttpConnection DELETE(URI uri)
    {
        return connect("DELETE", null, uri);
    }

    public static HttpConnection HEAD(URL url)
    {
        return connect("HEAD", null, url);
    }

    public static HttpConnection HEAD(URI uri)
    {
        return connect("HEAD", null, uri);
    }

    // low level http operations

    public static HttpConnection connect(String requestMethod,
                                         String contentType,
                                         URL url) {
        return new HttpConnection(requestMethod, contentType, url);
    }

    public static HttpConnection connect(String requestMethod,
                                         String contentType,
                                         URI uri) {
        try {
            return new HttpConnection(requestMethod, contentType, uri.toURL());
        } catch (MalformedURLException mue) {
            return null;
        }
    }


}
