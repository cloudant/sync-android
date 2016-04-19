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


import com.cloudant.common.RetriableTask;
import com.cloudant.http.Http;
import com.cloudant.http.HttpConnection;
import com.cloudant.http.HttpConnectionRequestInterceptor;
import com.cloudant.http.HttpConnectionResponseInterceptor;
import com.cloudant.mazha.json.JSONHelper;
import com.cloudant.sync.datastore.DocumentRevsList;
import com.cloudant.sync.datastore.MultipartAttachmentWriter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

/**
 * @api_private
 */
public class CouchClient  {

    protected final JSONHelper jsonHelper;
    private CouchURIHelper uriHelper;
    private List<HttpConnectionRequestInterceptor> requestInterceptors;
    private List<HttpConnectionResponseInterceptor> responseInterceptors;
    private final static Logger logger = Logger.getLogger(RetriableTask.class.getCanonicalName());

    public CouchClient(URI rootUri,
                       List<HttpConnectionRequestInterceptor> requestInterceptors,
                       List<HttpConnectionResponseInterceptor> responseInterceptors) {
        this.jsonHelper = new JSONHelper();
        this.uriHelper = new CouchURIHelper(rootUri);
        this.requestInterceptors = new ArrayList<HttpConnectionRequestInterceptor>();
        this.responseInterceptors = new ArrayList<HttpConnectionResponseInterceptor>();

        if(requestInterceptors != null) {
            this.requestInterceptors.addAll(requestInterceptors);
        }

        if(responseInterceptors != null) {
            this.responseInterceptors.addAll(responseInterceptors);
        }
    }

    public URI getRootUri() {
        return this.uriHelper.getRootUri();
    }

    // result of executing an HTTP call:
    // - stream non-null and exception null: the call was successful, result in stream
    // - stream null and exception non-null: the call was unsuccessful, details in exception
    // - fatal: set to true when exception non-null, indicates call should not be retried
    private static class ExecuteResult
    {
        private ExecuteResult(InputStream stream,
                              InputStream errorStream,
                              int responseCode,
                              String responseMessage,
                              Throwable cause,
                              JSONHelper jsonHelper)
        {
            this.responseCode = responseCode;
            boolean needsCouchException = false;
            switch(responseCode / 100) {
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
                    throw new CouchException("Unexpected redirection (3xx) code encountered", responseCode);
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
                    Map<String, String> json = jsonHelper.fromJson(new InputStreamReader
                            (errorStream, Charset.forName("UTF-8")), Map.class);
                    CouchException ce = new CouchException(responseMessage, cause, responseCode);
                    ce.setError(json.get("error"));
                    ce.setReason(json.get("reason"));
                    this.exception = ce;
                } catch (Exception e) {
                    CouchException ce = new CouchException("Error deserializing server response", cause,
                            responseCode);
                    this.exception = ce;
                }
            }
        }

        InputStream stream;
        CouchException exception;
        int responseCode;
        boolean fatal;
    }

    // - if 2xx then return stream
    // - map 404 to NoResourceException
    // - if there's a couch error returned as json, un-marshall and throw
    // - anything else, just throw the IOException back, use the cause part of the exception?

    // it needs to catch eg FileNotFoundException and rethrow to emulate the previous exception handling behaviour
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
                    cause,
                    jsonHelper);
            return executeResult;
        } finally {
            // don't close inputStream as the callee still needs it
            IOUtils.closeQuietly(errorStream);
        }
    }

    // execute HTTP task with retries:
    // return an InputStream if successful or throw an exception
    private InputStream executeToInputStreamWithRetry(final Callable<ExecuteResult> task) throws CouchException {
        int attempts = 10;
        CouchException lastException= null;
        while (attempts-- > 0) {
            ExecuteResult result = null;
            try {
                result = task.call();
            } catch (Exception e) {
                throw new CouchException("Unexpected exception", e, -1);
            }
            lastException = result.exception;
            if (result.stream != null) {
                // success - return the inputstream
                return result.stream;
            } else if (result.fatal) {
                // fatal exception - don't attempt any more retries
                throw result.exception;
            }
        }
        throw lastException;
    }

    private <T> T executeToJsonObjectWithRetry(final HttpConnection connection,
                                               Class<T> c) throws CouchException {
        InputStream is = this.executeToInputStreamWithRetry(connection);
        InputStreamReader isr = new InputStreamReader(is, Charset.forName("UTF-8"));
        try {
            T json = new JSONHelper().fromJson(isr, c);
            return json;
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    private InputStream executeToInputStreamWithRetry(final HttpConnection connection) throws CouchException {
        // all CouchClient requests want to receive application/json responses
        connection.requestProperties.put("Accept", "application/json");
        connection.responseInterceptors.addAll(responseInterceptors);
        connection.requestInterceptors.addAll(requestInterceptors);
        InputStream is = this.executeToInputStreamWithRetry(new Callable<ExecuteResult>() {
            @Override
            public ExecuteResult call() throws Exception {
                return execute(connection);
            }
        });
        return is;
    }

    public void createDb() {
        HttpConnection connection = Http.PUT(uriHelper.getRootUri(), "application/json");
        DBOperationResponse res = executeToJsonObjectWithRetry(connection, DBOperationResponse
                .class);
    }

    public void deleteDb() {
        HttpConnection connection = Http.DELETE(this.uriHelper.getRootUri());
        DBOperationResponse res = executeToJsonObjectWithRetry(connection, DBOperationResponse
                .class);
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

    public ChangesResult changes(Object since) {
        return this.changes(since, null);
    }

    public ChangesResult changes(Object since, Integer limit) {
        return this.changes(null, null, since, limit);
    }

    public ChangesResult changes(String filterName, Map<String, String> filterParameters, Object since, Integer limit) {
        Map<String, Object> options = getDefaultChangeFeedOptions();
        if(filterName != null) {
            options.put("filter", filterName);
            if(filterParameters != null) {
                options.putAll(filterParameters);
            }
        }
        if(since != null) {
            options.put("since", since);
        }
        if (limit != null) {
            options.put("limit", limit);
        }
        return this.changes(options);
    }

    public ChangesResult changes(final Map<String, Object> options) {
        URI changesFeedUri = uriHelper.changesUri(options);
        HttpConnection connection = Http.GET(changesFeedUri);
        return executeToJsonObjectWithRetry(connection, ChangesResult.class);
    }

    // TODO does this still work the same way we expect it to?
    public boolean contains(String id) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        URI doc = this.uriHelper.documentUri(id);
        try {
            HttpConnection connection = Http.HEAD(doc);
            this.executeToInputStreamWithRetry(connection);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public Response create(Object document) {
        String json = jsonHelper.toJson(document);
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

    public InputStream getDocumentStream(String id, String rev) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rev), "rev must not be empty");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        final URI doc = this.uriHelper.documentUri(id, queries);
        HttpConnection connection = Http.GET(doc);
        return executeToInputStreamWithRetry(connection);
    }

    public InputStream getAttachmentStream(String id, String rev, String attachmentName, final boolean acceptGzip) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rev), "rev must not be empty");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        final URI doc = this.uriHelper.attachmentUri(id, queries, attachmentName);
        HttpConnection connection = Http.GET(doc);
        if (acceptGzip) {
            connection.requestProperties.put("Accept-Encoding", "gzip");
        }
        return executeToInputStreamWithRetry(connection);
    }

    public void putAttachmentStream(String id, String rev, String attachmentName, String contentType, byte[] attachmentData) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rev), "rev must not be empty");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        URI doc = this.uriHelper.attachmentUri(id, queries, attachmentName);
        HttpConnection connection = Http.PUT(doc, contentType);
        connection.setRequestBody(attachmentData);
        this.executeToInputStreamWithRetry(connection);
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
    public <T> T getDocConflictRevs(String id)  {
        Map<String, Object> options = new HashMap<String, Object>();
        options.put("conflicts", true);
        return this.getDocument(id, options, new TypeReference<T>() {
        });
    }

    /**
     * Convenience method to get document with revision history for a given list of open revisions. It does that by
     * adding "open_revs=["rev1", "rev2"]" option to the GET request.
     *
     * It must return a list because that is how CouchDB return its results.
     *
     */
    public List<OpenRevision> getDocWithOpenRevisions(String id, Collection<String> revisions,
                                                      Collection<String> attsSince,
                                                      boolean pullAttachmentsInline) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkArgument(revisions.size() > 0, "Need at lease one open revision");

        Map<String, Object> options = new HashMap<String, Object>();
        options.put("revs", true);
        // only pull attachments inline if we're configured to
        if (pullAttachmentsInline) {
            options.put("attachments", true);
            if (attsSince != null) {
                options.put("atts_since", jsonHelper.toJson(attsSince));
            }
        } else {
            options.put("attachments", false);
            options.put("att_encoding_info", true);
        }
        options.put("open_revs", jsonHelper.toJson(revisions));
        return this.getDocument(id, options, new TypeReference<List<OpenRevision>>() {
        });
    }

    /**
     * <p>
     * Return an iterator representing the result of calling the _bulk_docs endpoint.
     * </p>
     * <p>
     * Each time the iterator is advanced, a DocumentRevsList is returned, which represents the
     * leaf nodes and their ancestries for a given document id.
     * </p>
     * @param request A request for 1 or more (id,rev) pairs.
     * @param pullAttachmentsInline If true, retrieve attachments as inline base64
     * @return An iterator representing the result of calling the _bulk_docs endpoint.
     */
    public Iterable<DocumentRevsList> bulkReadDocsWithOpenRevisions(List<BulkGetRequest> request,
                                                                    boolean pullAttachmentsInline) {
        Map<String, Object> options = new HashMap<String, Object>();
        options.put("revs", true);

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
        connection.setRequestBody(jsonHelper.toJson(jsonRequest));
        // deserialise response
        BulkGetResponse response = executeToJsonObjectWithRetry(connection, BulkGetResponse.class);

        Map<String,ArrayList<DocumentRevs>> revsMap = new HashMap<String,ArrayList<DocumentRevs>>();

        // merge results back in, so there is one list of DocumentRevs per id
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

        // flatten out revsMap hash so that there is one entry in our return array for each id
        for (ArrayList<DocumentRevs> value : revsMap.values()) {
            allRevs.add(new DocumentRevsList(value));
        }
        return allRevs;
    }

    public Map<String, Object> getDocument(String id) {
        return this.getDocument(id, new HashMap<String, Object>(), JSONHelper.STRING_MAP_TYPE_DEF);
    }

    public <T> T getDocument(String id, final Class<T> type)  {
        return this.getDocument(id, new HashMap<String, Object>(), new TypeReference<T>() {
            @Override
            public Type getType() {
                return type;
            }
        });
    }

    public <T> T getDocument(final String id, final Map<String, Object> options, final
    TypeReference<T> type) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkNotNull(type, "type must not be null");

        URI doc = uriHelper.documentUri(id, options);
        InputStream is = null;
        try {
            HttpConnection connection = Http.GET(doc);
            is = executeToInputStreamWithRetry(connection);
            T returndoc = jsonHelper.fromJson(new InputStreamReader(is, Charset.forName("UTF-8"))
                    , type);
            logger.fine("getDocument returning " + returndoc);
            return returndoc;
        } finally {
            closeQuietly(is);
        }
    }

    public Map<String, Object> getDocument(String id, String rev) {
        return getDocument(id, rev, JSONHelper.STRING_MAP_TYPE_DEF);
    }

    public <T> T getDocument(String id, String rev, TypeReference<T> type) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "Id must not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rev), "Id must not be empty");
        Preconditions.checkNotNull(type, "Type must not be null");

        InputStream is = null;
        try {
            is = this.getDocumentStream(id, rev);
            T returndoc = jsonHelper.fromJson(new InputStreamReader(is, Charset.forName("UTF-8"))
                    , type);
            logger.fine("getDocument returning " + returndoc);
            return returndoc;
        } finally {
            closeQuietly(is);
        }
    }

    public <T> T getDocument(String id, String rev, final Class<T> type) {
        return getDocument(id, rev, new TypeReference<T>() {
            @Override
            public Type getType() {
                return type;
            }
        });
    }

    /**
     * Get document along with its revision history, and the result is converted to a <code>DocumentRevs</code> object.
     *
     * @see <code>DocumentRevs</code>
     *
     */
    public DocumentRevs getDocRevisions(String id, String rev) {
        return getDocRevisions(id, rev, new TypeReference<DocumentRevs>() {
        });
    }

    public <T> T getDocRevisions(String id, String rev, TypeReference<T> type) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkNotNull(rev, "document must not be null");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("revs", "true");
        queries.put("rev", rev);
        URI findRevs = this.uriHelper.documentUri(id, queries);

        InputStream is = null;
        try {
            HttpConnection connection = Http.GET(findRevs);
            is = this.executeToInputStreamWithRetry(connection);
            return jsonHelper.fromJson(new InputStreamReader(is, Charset.forName("UTF-8")), type);
        } finally {
            closeQuietly(is);
        }
    }

    // Document should be complete document include "_id" matches id
    public Response update(String id, Object document) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkNotNull(document, "document must not be null");
        if (!this.contains(id)) {
            throw new NoResourceException("No document for given id: " + id);
        }

        String json = jsonHelper.toJson(document);
        URI doc = this.uriHelper.documentUri(id);
        HttpConnection connection = Http.PUT(doc, "application/json");
        connection.setRequestBody(json);
        Response r = executeToJsonObjectWithRetry(connection, Response.class);
        return r;
    }

    public Response delete(String id, String rev) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rev), "rev must not be empty");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        URI doc = this.uriHelper.documentUri(id, queries);
        HttpConnection connection = Http.DELETE(doc);
        return executeToJsonObjectWithRetry(connection, Response.class);
    }

    private void closeQuietly(InputStream is) {
        try {
            if (is != null) {
                is.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private InputStream bulkCreateDocsInputStream(List<?> objects) {
        Preconditions.checkNotNull(objects, "Object list must not be null.");
        String newEditsVal = "\"new_edits\": false, ";
        URI uri = this.uriHelper.bulkDocsUri();
        String payload = String.format("{%s%s%s}", newEditsVal, "\"docs\": ",
                jsonHelper.toJson(objects));
        HttpConnection connection = Http.POST(uri, "application/json");
        connection.setRequestBody(payload);
        return this.executeToInputStreamWithRetry(connection);
    }

    public List<Response> bulkCreateDocs(Object... objects) {
        return bulkCreateDocs(Arrays.asList(objects));
    }

    public List<Response> bulkCreateDocs(List<?> objects) {
        InputStream is = null;
        try {
            is = bulkCreateDocsInputStream(objects);
            return jsonHelper.fromJsonToList(new InputStreamReader(is, Charset.forName("UTF-8")),
                    new TypeReference<List<Response>>() {
                    });
        } finally {
            closeQuietly(is);
        }
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
        Preconditions.checkNotNull(serializedDocs, "Serialized doc list must not be null.");
        String payload = generateBulkSerializedDocsPayload(serializedDocs);
        URI uri = this.uriHelper.bulkDocsUri();
        InputStream is = null;
        HttpConnection connection = Http.POST(uri, "application/json");
        connection.setRequestBody(payload);
        try {
            is = this.executeToInputStreamWithRetry(connection);
            return jsonHelper.fromJsonToList(new InputStreamReader(is, Charset.forName("UTF-8")),
                    new TypeReference<List<Response>>() {
            });
        } finally {
            closeQuietly(is);
        }
    }

    private String generateBulkSerializedDocsPayload(List<String> serializedDocs) {
        String newEditsVal = "\"new_edits\": false, ";
        StringBuilder sb = new StringBuilder("[");
        for(String doc : serializedDocs) {
            if(sb.length() > 1) {
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
     * The input revisions is a map, whose key is document id, and value is a list of revisions.
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
     * If the id has no missing revision, it should not appear in the Map's key set. If all ids
     * do not have missing revisions, the returned Map should be empty map, but never null.
     *
     * @see <a href="http://wiki.apache.org/couchdb/HttpPostRevsDiff">HttpPostRevsDiff documentation</a>
     */
    public Map<String, MissingRevisions> revsDiff(Map<String, Set<String>> revisions) {
        Preconditions.checkNotNull(revisions, "Input revisions must not be null");
        URI uri = this.uriHelper.revsDiffUri();
        String payload = this.jsonHelper.toJson(revisions);
        InputStream is = null;
        try {
            HttpConnection connection = Http.POST(uri, "application/json");
            connection.setRequestBody(payload);
            is = executeToInputStreamWithRetry(connection);
            Map<String, MissingRevisions> diff = jsonHelper.fromJson(new InputStreamReader(is,
                            Charset.forName("UTF-8")),
                    new TypeReference<Map<String, MissingRevisions>>() {
                    });
            return diff;
        } finally {
            closeQuietly(is);
        }
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
        HttpConnection connection = Http.GET(bulkGet);
        connection.responseInterceptors.addAll(responseInterceptors);
        connection.requestInterceptors.addAll(requestInterceptors);
        ExecuteResult result = this.execute(connection);
        switch (result.responseCode) {
            case 404:
                // not found: _bulk_get not supported
                return false;
            case 405:
                // method not allowed: this endpoint exists, we called with the wrong method
                return true;
            default:
                throw(result.exception);
        }
    }

    public static class MissingRevisions {
        public Set<String> possible_ancestors;
        public Set<String> missing;
    }


}
