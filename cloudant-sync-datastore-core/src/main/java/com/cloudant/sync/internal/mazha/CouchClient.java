/*
 * Copyright © 2013 Cloudant
 *
 * Copyright © 2011 Ahmed Yehia (ahmed.yehia.m@gmail.com)
 *
 * Copyright © 2016 IBM Corp. All rights reserved.
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


import com.cloudant.http.Http;
import com.cloudant.http.HttpConnection;
import com.cloudant.http.HttpConnectionRequestInterceptor;
import com.cloudant.http.HttpConnectionResponseInterceptor;
import com.cloudant.http.internal.interceptors.SSLCustomizerInterceptor;
import com.cloudant.http.internal.interceptors.UserAgentInterceptor;
import com.cloudant.sync.internal.common.RetriableTask;
import com.cloudant.sync.internal.documentstore.DocumentRevsList;
import com.cloudant.sync.internal.documentstore.MultipartAttachmentWriter;
import com.cloudant.sync.internal.util.JSONUtils;
import com.cloudant.sync.internal.util.Misc;
import com.cloudant.sync.replication.PullFilter;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

public class CouchClient {

    public static final List<HttpConnectionRequestInterceptor> DEFAULT_REQUEST_INTERCEPTORS;

     // Set up the defaults
     static {
         HttpConnectionRequestInterceptor ua_interceptor =
                 new UserAgentInterceptor(CouchClient.class.getClassLoader(),
                         "META-INF/com.cloudant.sync.client.properties");
         HttpConnectionRequestInterceptor tlsInterceptor = checkAndGetTlsInterceptor();
         DEFAULT_REQUEST_INTERCEPTORS = (tlsInterceptor != null) ?
                 Arrays.asList(ua_interceptor, tlsInterceptor) :
                 Collections.singletonList(ua_interceptor);
     }

    private CouchURIHelper uriHelper;
    private List<HttpConnectionRequestInterceptor> requestInterceptors;
    private List<HttpConnectionResponseInterceptor> responseInterceptors;
    private final static Logger logger = Logger.getLogger(RetriableTask.class.getCanonicalName());

    public CouchClient(URI rootUri,
                       List<HttpConnectionRequestInterceptor> requestInterceptors,
                       List<HttpConnectionResponseInterceptor> responseInterceptors) {
        this.uriHelper = new CouchURIHelper(rootUri);
        this.requestInterceptors = new ArrayList<HttpConnectionRequestInterceptor>();
        this.responseInterceptors = new ArrayList<HttpConnectionResponseInterceptor>();

        this.requestInterceptors.addAll(DEFAULT_REQUEST_INTERCEPTORS);

        if (requestInterceptors != null) {
            this.requestInterceptors.addAll(requestInterceptors);
        }

        if (responseInterceptors != null) {
            this.responseInterceptors.addAll(responseInterceptors);
        }
    }

    private static SSLCustomizerInterceptor checkAndGetTlsInterceptor() {
        // Some assistance for TLSv1.2 support. Two things we check before we try to force TLSv1.2
        // so that we don't interfere with any other custom configuration that may have been
        // provided:
        //   i) are we running on Android, and an old version of Android (api level < 20 does not
        //      have TLSv1.2 enabled by default)
        //   ii) does the default SSLContext have TLSv1.2 enabled or available
        if (Misc.isRunningOnAndroid()) {
            // This block catches all exceptions from TLSv1.2 checks, if we get exceptions we bail
            // with a RuntimeException
            try {
                // Get the API level reflectively so we don't need to import classes only available
                // in Android
                int androidApiLevel = Class.forName("android.os.Build$VERSION").getField
                        ("SDK_INT").getInt(null);
                // If we are on an old version of Android and TLSv1.2 has not been enabled already
                // on the default context then we add a special interceptor
                if (androidApiLevel < 20) {
                    // Check i has passed we are on an old version of Android

                    // Get the default SSLContext
                    SSLContext defSslCtx = SSLContext.getDefault();

                    // Check the default protocols for TLSv1.2
                    if (!Arrays.asList(defSslCtx.getDefaultSSLParameters().getProtocols())
                            .contains(TlsOnlySslSocketFactory.TLSv12) &&
                            Arrays.asList(defSslCtx.getSupportedSSLParameters().getProtocols())
                                    .contains(TlsOnlySslSocketFactory.TLSv12)) {
                        // Check ii has passed TLSv1.2 is not already enabled on the default context
                        // but is supported on the default context so we can use it

                        // In an ideal world we would also check the default SSLSocketFactory that
                        // is set on the HttpsUrlConnection to see if that has been customized from
                        // the default, but there isn't really a way to check this, equals is not
                        // overridden on the SSLSocketFactory and there is no route back to the
                        // SSLContext that generated the SSLSocketFactory.
                        SSLContext sc = SSLContext.getInstance(TlsOnlySslSocketFactory.TLSv12);
                        sc.init(null, null, null);
                        // We add this interceptor, but it could be overridden by a later user
                        // provided interceptor, so this shouldn't change anyone's existing
                        // customizations.
                        SSLSocketFactory s = sc.getSocketFactory();
                        return new SSLCustomizerInterceptor(new TlsOnlySslSocketFactory(s));
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            } catch (NoSuchFieldException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            } catch (KeyManagementException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    private static final class TlsOnlySslSocketFactory extends SSLSocketFactory {

        private static final String TLSv12 = "TLSv1.2";
        private static final String[] TLSv12Only = new String[]{TLSv12};
        private final SSLSocketFactory delegate;

        private TlsOnlySslSocketFactory(SSLSocketFactory delegate) {
            this.delegate = delegate;
        }

        @Override
        public String[] getDefaultCipherSuites() {
            return delegate.getDefaultCipherSuites();
        }

        @Override
        public String[] getSupportedCipherSuites() {
            return delegate.getSupportedCipherSuites();
        }

        @Override
        public Socket createSocket(Socket socket, String s, int i, boolean b) throws IOException {
            return tlsOnlySocket(delegate.createSocket(socket, s, i, b));
        }

        @Override
        public Socket createSocket(String s, int i) throws IOException {
            return tlsOnlySocket(delegate.createSocket(s, i));
        }

        @Override
        public Socket createSocket(String s, int i, InetAddress inetAddress, int i1) throws
                IOException {
            return tlsOnlySocket(delegate.createSocket(s, i, inetAddress, i1));
        }

        @Override
        public Socket createSocket(InetAddress inetAddress, int i) throws IOException {
            return tlsOnlySocket(delegate.createSocket(inetAddress, i));
        }

        @Override
        public Socket createSocket(InetAddress inetAddress, int i, InetAddress inetAddress1, int
                i1) throws IOException {
            return tlsOnlySocket(delegate.createSocket(inetAddress, i, inetAddress1, i1));
        }

        private Socket tlsOnlySocket(Socket s) {
            if (s instanceof SSLSocket) {
                ((SSLSocket) s).setEnabledProtocols(TLSv12Only);
            }
            return s;
        }
    }

    public URI getRootUri() {
        return this.uriHelper.getRootUri();
    }

    // result of executing an HTTP call:
    // - stream non-null and exception null: the call was successful, result in stream
    // - stream null and exception non-null: the call was unsuccessful, details in exception
    // - fatal: set to true when exception non-null, indicates call should not be retried
    private static class ExecuteResult {
        private ExecuteResult(InputStream stream,
                              InputStream errorStream,
                              int responseCode,
                              String responseMessage,
                              Throwable cause) {
            boolean needsCouchException = false;
            switch (responseCode / 100) {
                case 1:
                case 2:
                    // 1xx and 2xx are OK
                    // we would not normally expect to see 1xx in a response
                    // explicitly set these to show we are OK
                    this.fatal = false;
                    this.exception = null;
                    this.stream = stream;
                    break;
                case 3:
                    // 3xx redirection
                    throw new CouchException("Unexpected redirection (3xx) code encountered",
                            responseCode);
                case 4:
                    // 4xx errors normally mean we are not authenticated so we shouldn't retry
                    this.fatal = true;
                    if (responseCode == 404) {
                        this.exception = new NoResourceException(responseMessage, cause);
                    } else if (responseCode == 409) {
                        this.exception = new DocumentConflictException(responseMessage);
                    } else {
                        needsCouchException = true;
                    }
                    break;
                case 5:
                    // 5xx errors are transient server errors so we should retry
                    this.fatal = false;
                    needsCouchException = true;
                    break;
                default:
                    // couldn't get response code
                    // something bad happened but we should retry (timeouts, socket closed etc)
                    this.fatal = false;
                    needsCouchException = true;
            }
            if (needsCouchException) {
                try {
                    Map<String, String> json = JSONUtils.fromJson(new InputStreamReader
                            (errorStream, Charset.forName("UTF-8")), Map.class);
                    CouchException ce = new CouchException(responseMessage, cause, responseCode);
                    ce.setError(json.get("error"));
                    ce.setReason(json.get("reason"));
                    this.exception = ce;
                } catch (Exception e) {
                    CouchException ce = new CouchException("Error deserializing server response",
                            cause,
                            responseCode);
                    this.exception = ce;
                }
            }
        }

        InputStream stream;
        CouchException exception;
        boolean fatal;
    }

    // - if 2xx then return stream
    // - map 404 to NoResourceException
    // - if there's a couch error returned as json, un-marshall and throw
    // - anything else, just throw the IOException back, use the cause part of the exception?

    // it needs to catch eg FileNotFoundException and rethrow to emulate the previous exception
    // handling behaviour
    private ExecuteResult execute(HttpConnection connection) {

        InputStream inputStream = null; // input stream - response from server on success
        InputStream errorStream = null; // error stream - response from server for a 500 etc
        String responseMessage = null;
        int responseCode = -1;
        Throwable cause = null;

        // first try to execute our request and get the input stream with the server's response
        // we want to catch IOException because HttpUrlConnection throws these for non-success
        // responses (eg 404 throws a FileNotFoundException) but we need to map to our own
        // specific exceptions
        try {
            inputStream = connection.execute().responseAsInputStream();
        } catch (IOException ioe) {
            cause = ioe;
        }

        // response code and message will generally be present together or not all
        try {
            responseCode = connection.getConnection().getResponseCode();
            responseMessage = connection.getConnection().getResponseMessage();
        } catch (IOException ioe) {
            responseMessage = "Error retrieving server response message";
        }

        // error stream will be present or null if not applicable
        errorStream = connection.getConnection().getErrorStream();

        try {
            ExecuteResult executeResult = new ExecuteResult(inputStream,
                    errorStream,
                    responseCode,
                    responseMessage,
                    cause
            );
            return executeResult;
        } finally {
            // don't close inputStream as the callee still needs it
            IOUtils.closeQuietly(errorStream);
        }
    }

    // execute HTTP task with retries:
    // return an InputStream if successful or throw an exception
    private <T> T executeWithRetry(final Callable<ExecuteResult> task,
                                   InputStreamProcessor<T> processor) throws
            CouchException {
        int attempts = 10;
        CouchException lastException = null;
        while (attempts-- > 0) {
            ExecuteResult result = null;
            try {
                result = task.call();
                if (result.stream != null) {
                    // success - process the inputstream
                    try {
                        return processor.processStream(result.stream);
                    } catch (Exception e) {
                        // Error processing the input stream
                        if (attempts > 0) {
                            logger.log(Level.WARNING, "Received an exception during response " +
                                    "stream processing. A retry will be attempted.", e);
                        } else {
                            logger.log(Level.SEVERE, "Response stream processing failed, no " +
                                    "retries remaining.", e);
                            throw e;
                        }
                    } finally {
                        result.stream.close();
                    }
                }
            } catch (Exception e) {
                throw new CouchException("Unexpected exception", e, -1);
            }
            lastException = result.exception;
            if (result.fatal) {
                // fatal exception - don't attempt any more retries
                throw result.exception;
            }
        }
        throw lastException;
    }

    private <T> T executeToJsonObjectWithRetry(final HttpConnection connection, final
    TypeReference<T> type) throws CouchException {
        return executeWithRetry(connection, new TypeInputStreamProcessor<T>(type));
    }

    private <T> T executeToJsonObjectWithRetry(final HttpConnection connection,
                                               Class<T> c) throws CouchException {
        return executeToJsonObjectWithRetry(connection, new CouchClientTypeReference<T>(c));
    }

    private <T> T executeWithRetry(final HttpConnection connection,
                                   InputStreamProcessor<T> processor) throws
            CouchException {
        // all CouchClient requests want to receive application/json responses
        connection.requestProperties.put("Accept", "application/json");
        connection.responseInterceptors.addAll(responseInterceptors);
        connection.requestInterceptors.addAll(requestInterceptors);
        return this.executeWithRetry(new Callable<ExecuteResult>() {
            @Override
            public ExecuteResult call() throws Exception {
                return execute(connection);
            }
        }, processor);
    }

    public void createDb() {
        HttpConnection connection = Http.PUT(uriHelper.getRootUri(), "application/json");
        executeToJsonObjectWithRetry(connection, DBOperationResponse.class);
    }

    public void deleteDb() {
        HttpConnection connection = Http.DELETE(this.uriHelper.getRootUri());
        executeToJsonObjectWithRetry(connection, DBOperationResponse.class);
    }

    public CouchDbInfo getDbInfo() {
        HttpConnection connection = Http.GET(uriHelper.getRootUri());
        return executeToJsonObjectWithRetry(connection, CouchDbInfo.class);
    }

    private Map<String, Object> getDefaultChangeFeedOptions() {
        Map<String, Object> options = new HashMap<String, Object>();
        options.put("style", "all_docs");
        options.put("feed", "normal");
        return options;
    }

    private Map<String, Object> getParametrizedChangeFeedOptions(Object since, Integer limit) {
        Map<String, Object> options = getDefaultChangeFeedOptions();
        if (since != null) {
            options.put("since", since);
        }
        if (limit != null) {
            options.put("limit", limit);
        }
        // seq_interval: improve performance and reduce load on the remote database
        if (limit != null) {
            options.put("seq_interval", limit);
        } else {
            options.put("seq_interval", 1000);
        }
        return options;
    }

    public ChangesResult changes(Object since, Integer limit) {
        Map<String, Object> options = getParametrizedChangeFeedOptions(since, limit);
        return this.changesRequestWithGet(options);
    }

    public ChangesResult changes(PullFilter filter, Object since, Integer limit) {
        Map<String, Object> options = getParametrizedChangeFeedOptions(since, limit);
        if (filter != null) {
            String filterName = filter.getName();
            Map filterParameters = filter.getParameters();
            if (filterName != null) {
                options.put("filter", filterName);
                if (filterParameters != null) {
                    options.putAll(filterParameters);
                }
            }
        }
        return this.changesRequestWithGet(options);
    }

    public ChangesResult changes(String selector, Object since, Integer limit) {
        Misc.checkNotNullOrEmpty(selector, null);

        Map<String, Object> options = getParametrizedChangeFeedOptions(since, limit);
        options.put("filter", "_selector");

        return changesRequestWithPost(selector, options);
    }

    public ChangesResult changes(List<String> docIds, Object since, Integer limit) {
        Misc.checkState((docIds != null && !docIds.isEmpty()), null);

        Map<String, Object> options = getParametrizedChangeFeedOptions(since, limit);
        options.put("filter", "_doc_ids");

        Map<String, Object> docIdsMap = new HashMap<String, Object>();
        docIdsMap.put("doc_ids", docIds);
        String docsIdsDoc = JSONUtils.serializeAsString(docIdsMap);

        return changesRequestWithPost(docsIdsDoc, options);
    }

    private ChangesResult changesRequestWithGet(final Map<String, Object> options) {
        URI changesFeedUri = uriHelper.changesUri(options);
        HttpConnection connection = Http.GET(changesFeedUri);
        return executeToJsonObjectWithRetry(connection, ChangesResult.class);
    }

    private ChangesResult changesRequestWithPost( String body, final Map<String, Object> options) {
        URI changesFeedUri = uriHelper.changesUri(options);
        HttpConnection connection = Http.POST(changesFeedUri, "application/json");
        connection.setRequestBody(body);
        return executeToJsonObjectWithRetry(connection, ChangesResult.class);
    }

    // TODO does this still work the same way we expect it to?
    public boolean contains(String id) {
        Misc.checkNotNullOrEmpty(id, "id");
        try {
            getDocumentRev(id);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public Response create(Object document) {
        String json = JSONUtils.toJson(document);
        InputStream is = null;
        try {
            HttpConnection connection = Http.POST(this.uriHelper.getRootUri(), "application/json");
            connection.setRequestBody(json);
            Response res = executeToJsonObjectWithRetry(connection, Response.class);
            if (!res.getOk()) {
                throw new ServerException(res.toString());
            } else {
                return res;
            }
        } catch (CouchException e) {
            if (e.getStatusCode() == 409) {
                throw new DocumentConflictException(e.toString());
            } else {
                throw e;
            }
        }
    }

    public <T> T processAttachmentStream(String id, String rev, String attachmentName, final
    boolean acceptGzip, InputStreamProcessor<T> processor) {
        Misc.checkNotNullOrEmpty(id, "id");
        Misc.checkNotNullOrEmpty(rev, "rev");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        final URI doc = this.uriHelper.attachmentUri(id, queries, attachmentName);
        HttpConnection connection = Http.GET(doc);
        if (acceptGzip) {
            connection.requestProperties.put("Accept-Encoding", "gzip");
        }
        return executeWithRetry(connection, processor);
    }

    public void putAttachmentStream(String id, String rev, String attachmentName, String
            contentType, byte[] attachmentData) {
        Misc.checkNotNullOrEmpty(id, "id");
        Misc.checkNotNullOrEmpty(rev, "rev");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        URI doc = this.uriHelper.attachmentUri(id, queries, attachmentName);
        HttpConnection connection = Http.PUT(doc, contentType);
        connection.setRequestBody(attachmentData);
        //TODO check result?
        executeToJsonObjectWithRetry(connection, Response.class);
    }

    /**
     * Convenience method to get document with all the conflicts revisions. It does that by adding
     * "conflicts=true" option to the GET request. An example response JSON is the following:
     * {
     * "_id" : "c3fe5bfdee767fa3d51717bb8b51d55b",
     * "_rev" : "3-176c3c8b3f284d4fa6e8c075d58b7b86",
     * "hello" : "world",
     * "name" : "Tom",
     * "_conflicts" : [
     *   "2-d9789a01da0c41aeb3d86ff039f461f6",
     *   "2-65ddd7d56da84f25af544e84a3267ccf" ]
     * }
     */
    public Map<String, Object> getDocConflictRevs(String id) {
        Map<String, Object> options = new HashMap<String, Object>();
        options.put("conflicts", true);
        return this.getDocument(id, options, JSONUtils.STRING_MAP_TYPE_DEF);
    }

    /**
     * Convenience method to get document with revision history for a given list of open
     * revisions. It does that by
     * adding "open_revs=["rev1", "rev2"]" option to the GET request.
     *
     * It must return a list because that is how CouchDB return its results.
     */
    public List<OpenRevision> getDocWithOpenRevisions(String id, Collection<String> revisions,
                                                      Collection<String> attsSince,
                                                      boolean pullAttachmentsInline) {
        Misc.checkNotNullOrEmpty(id, "id");
        Misc.checkArgument(revisions.size() > 0, "Need at least one open revision");

        Map<String, Object> options = new HashMap<String, Object>();
        options.put("revs", true);
        // by adding latest we should never receive a "missing" response from the server. A descendant
        // of the revision requested will be returned even if it has been deleted.
        options.put("latest", true);
        // only pull attachments inline if we're configured to
        if (pullAttachmentsInline) {
            options.put("attachments", true);
            if (attsSince != null) {
                options.put("atts_since", JSONUtils.toJson(attsSince));
            }
        } else {
            options.put("attachments", false);
            options.put("att_encoding_info", true);
        }
        options.put("open_revs", JSONUtils.toJson(revisions));
        return this.getDocument(id, options, JSONUtils.OPEN_REVS_LIST_TYPE_DEF);
    }

    /**
     * <p>
     * Return an iterator representing the result of calling the _bulk_docs endpoint.
     * </p>
     * <p>
     * Each time the iterator is advanced, a DocumentRevsList is returned, which represents the
     * leaf nodes and their ancestries for a given document ID.
     * </p>
     *
     * @param request               A request for 1 or more (ID,rev) pairs.
     * @param pullAttachmentsInline If true, retrieve attachments as inline base64
     * @return An iterator representing the result of calling the _bulk_docs endpoint.
     */
    public Iterable<DocumentRevsList> bulkReadDocsWithOpenRevisions(List<BulkGetRequest> request,
                                                                    boolean pullAttachmentsInline) {
        Map<String, Object> options = new HashMap<String, Object>();
        options.put("revs", true);
        options.put("latest", true);

        if (pullAttachmentsInline) {
            options.put("attachments", true);
        } else {
            options.put("attachments", false);
            options.put("att_encoding_info", true);
        }
        URI bulkGet = this.uriHelper.documentUri("_bulk_get", options);
        HttpConnection connection = Http.POST(bulkGet, "application/json");
        Map<String, List<BulkGetRequest>> jsonRequest = new HashMap<String, List<BulkGetRequest>>();
        jsonRequest.put("docs", request);
        // build request
        connection.setRequestBody(JSONUtils.toJson(jsonRequest));
        // deserialise response
        BulkGetResponse response = executeToJsonObjectWithRetry(connection, BulkGetResponse.class);

        Map<String, ArrayList<DocumentRevs>> revsMap = new HashMap<String,
                ArrayList<DocumentRevs>>();

        // merge results back in, so there is one list of DocumentRevs per ID
        for (BulkGetResponse.Result result : response.results) {
            for (BulkGetResponse.Doc doc : result.docs) {
                if (doc.ok != null) {
                    String id = doc.ok.getId();
                    if (!revsMap.containsKey(id)) {
                        revsMap.put(id, new ArrayList<DocumentRevs>());
                    }
                    revsMap.get(id).add(doc.ok);
                }
            }
        }

        List<DocumentRevsList> allRevs = new ArrayList<DocumentRevsList>();

        // flatten out revsMap hash so that there is one entry in our return array for each ID
        for (ArrayList<DocumentRevs> value : revsMap.values()) {
            allRevs.add(new DocumentRevsList(value));
        }
        return allRevs;
    }

    public Map<String, Object> getDocument(String id) {
        return this.getDocument(id, new HashMap<String, Object>(), JSONUtils.STRING_MAP_TYPE_DEF);
    }

    public <T> T getDocument(String id, final Class<T> type) {
        return this.getDocument(id, new HashMap<String, Object>(),
                new CouchClientTypeReference<T>(type));
    }

    public <T> T getDocument(final String id, final Map<String, Object> options,
                             final TypeReference<T> type) {
        Misc.checkNotNullOrEmpty(id, "id");
        Misc.checkNotNull(type, "Type");

        URI doc = uriHelper.documentUri(id, options);
        HttpConnection connection = Http.GET(doc);
        T returndoc = executeToJsonObjectWithRetry(connection, type);
        logger.fine("get returning " + returndoc);
        return returndoc;
    }

    public Map<String, Object> getDocument(String id, String rev) {
        return getDocument(id, rev, JSONUtils.STRING_MAP_TYPE_DEF);
    }

    public <T> T getDocument(String id, String rev, TypeReference<T> type) {
        Misc.checkNotNullOrEmpty(id, "id");
        Misc.checkNotNullOrEmpty(rev, "rev");
        Misc.checkNotNull(type, "Type");

        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        final URI doc = this.uriHelper.documentUri(id, queries);
        HttpConnection connection = Http.GET(doc);
        T returndoc = executeToJsonObjectWithRetry(connection, type);
        logger.fine("get returning " + returndoc);
        return returndoc;

    }

    public <T> T getDocument(String id, String rev, final Class<T> type) {
        return getDocument(id, rev, new CouchClientTypeReference<T>(type));
    }

    /**
     * Get document along with its revision history, and the result is converted to a
     * <code>DocumentRevs</code> object.
     *
     * @see <code>DocumentRevs</code>
     */
    public DocumentRevs getDocRevisions(String id, String rev) {
        return getDocRevisions(id, rev,
                new CouchClientTypeReference<DocumentRevs>(DocumentRevs.class));
    }

    public <T> T getDocRevisions(String id, String rev, TypeReference<T> type) {
        Misc.checkNotNullOrEmpty(id, "id");
        Misc.checkNotNull(rev, "Revision ID");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("revs", "true");
        queries.put("rev", rev);
        URI findRevs = this.uriHelper.documentUri(id, queries);
        HttpConnection connection = Http.GET(findRevs);
        return executeToJsonObjectWithRetry(connection, type);
    }

    // Document should be complete document include "_id" matches ID
    public Response update(String id, Object document) {
        Misc.checkNotNullOrEmpty(id, "id");
        Misc.checkNotNull(document, "Document");

        // Get the latest rev, will throw if doc doesn't exist
        getDocumentRev(id);

        return putUpdate(id, document);
    }

    public Response putUpdate(String id, Object document) {
        Misc.checkNotNullOrEmpty(id, "id");
        Misc.checkNotNull(document, "Document");

        URI doc = this.uriHelper.documentUri(id);
        String json = JSONUtils.toJson(document);
        HttpConnection connection = Http.PUT(doc, "application/json");
        connection.setRequestBody(json);
        Response r = executeToJsonObjectWithRetry(connection, Response.class);
        return r;
    }

    public String getDocumentRev(String id) {
        URI doc = this.uriHelper.documentUri(id);

        HttpConnection head = Http.HEAD(doc);
        executeWithRetry(head, new NoOpInputStreamProcessor());

        String rev = head.getConnection().getHeaderField("ETag");
        // Remove enclosing "" before returning
        return rev.substring(1, rev.length() - 1);
    }

    public Response delete(String id, String rev) {
        Misc.checkNotNullOrEmpty(id, "id");
        Misc.checkNotNullOrEmpty(rev, "rev");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        URI doc = this.uriHelper.documentUri(id, queries);
        HttpConnection connection = Http.DELETE(doc);
        return executeToJsonObjectWithRetry(connection, Response.class);
    }

    public List<Response> bulkCreateDocs(Object... objects) {
        return bulkCreateDocs(Arrays.asList(objects));
    }

    public List<Response> bulkCreateDocs(List<?> objects) {
        Misc.checkNotNull(objects, "Object list");
        String newEditsVal = "\"new_edits\": false, ";
        String payload = String.format("{%s%s%s}", newEditsVal, "\"docs\": ",
                JSONUtils.toJson(objects));
        return bulkCreateDocs(payload);
    }

    private List<Response> bulkCreateDocs(String payload) {
        URI uri = this.uriHelper.bulkDocsUri();
        HttpConnection connection = Http.POST(uri, "application/json");
        connection.setRequestBody(payload);
        return executeToJsonObjectWithRetry(connection, new
                CouchClientTypeReference<List<Response>>());
    }

    /**
     * Bulk insert a list of document that are serialized to JSON data already. For performance reasons,
     * the JSON doc is not validated.
     *
     * @param serializedDocs array of JSON documents
     * @return list of Response
     */
    public List<Response> bulkCreateSerializedDocs(String... serializedDocs) {
        return bulkCreateSerializedDocs(Arrays.asList(serializedDocs));
    }

    /**
     * Bulk insert a list of document that are serialized to JSON data already. For performance
     * reasons,
     * the JSON doc is not validated.
     *
     * @param serializedDocs list of JSON documents
     * @return list of Response
     */
    public List<Response> bulkCreateSerializedDocs(List<String> serializedDocs) {
        Misc.checkNotNull(serializedDocs, "Serialized doc list");
        String payload = generateBulkSerializedDocsPayload(serializedDocs);
        return bulkCreateDocs(payload);
    }

    private String generateBulkSerializedDocsPayload(List<String> serializedDocs) {
        String newEditsVal = "\"new_edits\": false, ";
        StringBuilder sb = new StringBuilder("[");
        for (String doc : serializedDocs) {
            if (sb.length() > 1) {
                sb.append(", ");
            }
            sb.append(doc);
        }
        sb.append("]");
        return String.format("{%s%s%s}", newEditsVal, "\"docs\": ", sb.toString());
    }

    /**
     * Returns the subset of given the documentId/revisions that are not stored in the database.
     *
     * The input revisions is a map, whose key is document ID, and value is a list of revisions.
     * An example input could be (in JSON format):
     *
     * { "03ee06461a12f3c288bb865b22000170":
     *     [
     *       "1-b2e54331db828310f3c772d6e042ac9c",
     *       "2-3a24009a9525bde9e4bfa8a99046b00d"
     *     ],
     *   "82e04f650661c9bdb88c57e044000a4b":
     *     [
     *       "3-bb39f8c740c6ffb8614c7031b46ac162"
     *     ]
     * }
     *
     * The output is in same format.
     *
     * If the ID has no missing revision, it should not appear in the Map's key set. If all IDs
     * do not have missing revisions, the returned Map should be empty map, but never null.
     *
     * @see
     * <a target="_blank" href="http://wiki.apache.org/couchdb/HttpPostRevsDiff">HttpPostRevsDiff
     * documentation</a>
     */
    public Map<String, MissingRevisions> revsDiff(Map<String, Set<String>> revisions) {
        Misc.checkNotNull(revisions, "Input revisions");
        URI uri = this.uriHelper.revsDiffUri();
        String payload = JSONUtils.toJson(revisions);

        HttpConnection connection = Http.POST(uri, "application/json");
        connection.setRequestBody(payload);
        return executeToJsonObjectWithRetry(connection, JSONUtils.STRING_MISSING_REVS_MAP_TYPE_DEF);
    }

    public Response putMultipart(final MultipartAttachmentWriter mpw) {
        Map<String, Object> options = new HashMap<String, Object>();
        options.put("new_edits", "false");
        URI uri = this.uriHelper.documentUri(mpw.getId(), options);
        String contentType = "multipart/related;boundary=" + mpw.getBoundary();
        HttpConnection connection = Http.PUT(uri, contentType);
        connection.setRequestBody(mpw.makeInputStreamGenerator(), mpw.getContentLength());
        return executeToJsonObjectWithRetry(connection, Response.class);
    }

    public boolean isBulkSupported() {
        URI bulkGet = this.uriHelper.documentUri("_bulk_get");
        HttpConnection get = Http.GET(bulkGet);
        Throwable cause = null;
        try {
            executeWithRetry(get, new NoOpInputStreamProcessor());
        } catch (CouchException ce) {
            switch (ce.getStatusCode()) {
                case 404:
                    // not found: _bulk_get not supported
                    return false;
                case 405:
                    // method not allowed: this endpoint exists, we called with the wrong method
                    return true;
                default:
                    // will re-throw with this as cause since we didn't understand the result
                    cause = ce;
            }
        }
        // if we got here, we either ran out of retries or couldn't figure out the response code
        // so all we can do is throw an exception
        throw new RuntimeException("Could not determine if the _bulk_get endpoint is supported",
                cause);
    }

    public static class MissingRevisions {
        public Set<String> possible_ancestors;
        public Set<String> missing;
    }

    public interface InputStreamProcessor<T> {

        T processStream(InputStream stream) throws Exception;
    }

    private static class CouchClientTypeReference<T> extends TypeReference<T> {

        private Class<T> type;

        CouchClientTypeReference() {
            this(null);
        }

        CouchClientTypeReference(Class<T> type) {
            this.type = type;
        }

        @Override
        public Type getType() {
            if (this.type == null) {
                return super.getType();
            } else {
                return this.type;
            }

        }
    }

    private static final class TypeInputStreamProcessor<T> implements InputStreamProcessor<T> {

        private final TypeReference<T> typeReference;

        TypeInputStreamProcessor(TypeReference<T> typeReference) {
            this.typeReference = typeReference;
        }

        @Override
        public T processStream(InputStream stream) {
            return JSONUtils.fromJson(new InputStreamReader(stream, Charset.forName
                    ("UTF-8")), typeReference);
        }
    }

    /**
     * For use with e.g. HEAD requests where there is no InputStream
     */
    public static final class NoOpInputStreamProcessor implements InputStreamProcessor<Void> {

        @Override
        public Void processStream(InputStream stream) {
            return null;
        }
    }
}
