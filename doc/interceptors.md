qHTTP Interceptors
=====

With the release of sync-android version 0.13.0 a new HTTP Interceptor API was introduced.
HTTP Interceptors allow the developer to modify the HTTP requests and responses during
the replication process.

Interceptors can be used to implement your own authentication schemes, for example OAuth, or
provide custom headers so you can perform your own analysis on usage. They can also be used
to monitor or log the requests made by the library.

To monitor or make changes to HTTP requests, implement one or both of the following in
your class:

- To modify the outgoing request, `HTTPConnectionRequestInterceptor`.
- To examine the incoming response, `HTTPConnectionResponseInterceptor`.

For an example of how to implement a interceptor, see the `CookieInterceptor` class.

In order to add an HTTP Interceptor to a replication, you call the `addRequestInterceptors`
or `addResponseInterceptors` on the `ReplicatorBuilder` class.

One of the built-in interceptors is `BasicAuthInterceptor` which can be used to
authenticate with servers where cookie authentication is not supported.

To add an add instance of `BasicAuthInterceptor` to a pull replication, do the following:

```java
DocumentStore ds = DocumentStore.getInstance(new File("my_datastore"));
BasicAuthInterceptor interceptor = new BasicAuthInterceptor("username:password");

Replicator replicator = ReplicatorBuilder.pull()
              .from(new URI("https://username.cloudant.com"))
              .to(ds)
              .addRequestInterceptors(interceptor)
              .addResponseInterceptors(interceptor)
              .build();

replicator.start();
```

## Adding Custom Request Headers

Request Interceptors can be used to add custom HTTP headers by
accessing the underlying `HttpURLConnection`, as in this example:

```java
@Override
public HttpConnectionInterceptorContext interceptRequest
(HttpConnectionInterceptorContext context) {
    HttpURLConnection connection = context.connection.getConnection();
    connection.setRequestProperty("x-my-header", "value");
    return context;
}
```

## Things to Avoid

The `com.cloudant.http.HttpConnectionInterceptorContext` object provides access to the underlying
`com.cloudant.http.HttpConnection` and `java.net.HttpURLConnection` classes. This allows you to change
settings and interact with a connection in ways would could potentially cause
errors in the replicator.

For example, reading a `java.net.HttpURLConnection` object's input stream will consume
the response data, meaning that the replicator will not receive the data from
Cloudant or CouchDB.

Currently the API has only been tested and verified for the following:

* Request Interceptors can modify the request headers and body.
* Response Interceptors can only set the interceptor context replay flag.

Changing anything else is unsupported. In the future, the number of supported APIs
is likely to be expanded.
