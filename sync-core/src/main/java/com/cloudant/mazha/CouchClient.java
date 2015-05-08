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


import com.cloudant.http.Http;
import com.cloudant.http.HttpConnection;
import com.cloudant.mazha.json.JSONHelper;
import com.cloudant.sync.datastore.MultipartAttachmentWriter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CouchClient  {

    protected final JSONHelper jsonHelper;
    private CouchURIHelper uriHelper;

    public CouchClient(CouchConfig config) {
        this.jsonHelper = new JSONHelper();
        this.uriHelper = new CouchURIHelper(config.getRootUri());
    }

    public URI getRootUri() {
        return this.uriHelper.getRootUri();
    }

    // - if 2xx then return stream
    // - map 404 to NoResourceException
    // - if there's a couch error returned as json, unmarshall and throw
    // - anything else, just throw the IOException back, use the cause part of the exception?

    // it needs to catch eg FileNotFoundException and rethrow to emulate the previous exception handling behaviour
    private InputStream executeToInputStream(HttpConnection connection) throws CouchException {

        // all couchclient requests want to receive application/json responses
        connection.requestProperties.put("Accept", "application/json");
        InputStream is = null; // input stream - response from server on success
        InputStream es = null; // error stream - response from server for a 500 etc
        String response = null;
        int code = -1;
        Throwable cause = null;

        // first try to execute our request and get the input stream with the server's response
        // we want to catch IOException because HttpUrlConnection throws these for non-success
        // responses (eg 404 throws a FileNotFoundException) but we need to map to our own
        // specific exceptions
        try {
            is = connection.execute().responseAsInputStream();
        } catch (IOException ioe) {
            cause = ioe;
        }

        try {
            code = connection.getConnection().getResponseCode();
            response = connection.getConnection().getResponseMessage();
            // everything ok? return the stream
            if (code / 100 == 2) { // success [200,299]
                return is;
            } else if (code == 404) {
                throw new NoResourceException(response, cause);
            } else {
                es = connection.getConnection().getErrorStream();
                // TODO what if deserialisation fails?
                CouchException ex = this.jsonHelper.fromJson(new InputStreamReader(es), CouchException.class);
                ex.setStatusCode(code);
                throw ex;
            }
        } catch (IOException ioe) {
            throw new CouchException("Error retrieving server response", ioe, code);
        } finally {
            if (es != null) {
                try {
                    es.close();
                } catch (IOException ioe) {
                    ;
                }
            }
        }
    }

    private <T> T executeToJsonObject(HttpConnection connection, Class<T> c) throws CouchException {
        InputStream is = this.executeToInputStream(connection);
        InputStreamReader isr = new InputStreamReader(is);
        T json = new JSONHelper().fromJson(isr, c);
        return json;
    }

    public void createDb() {
        HttpConnection connection = Http.PUT(this.uriHelper.getRootUri(), "application/json");
        DBOperationResponse res = executeToJsonObject(connection, DBOperationResponse.class);
        if (!res.getOk()) {
            throw new ServerException("Response from couch db server: " + res.toString());
        }
    }

    public void deleteDb() {
        HttpConnection connection = Http.DELETE(this.uriHelper.getRootUri());
        DBOperationResponse res = executeToJsonObject(connection, DBOperationResponse.class);
        if (!res.getOk()) {
            throw new ServerException("Response from couch db server: " + res.toString());
        }
    }

    public CouchDbInfo getDbInfo() {
        HttpConnection connection = Http.GET(this.uriHelper.getRootUri());
        return executeToJsonObject(connection, CouchDbInfo.class);
    }

    private Map<String, Object> getDefaultChangeFeeOptions() {
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
        Map<String, Object> options = getDefaultChangeFeeOptions();
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

    public ChangesResult changes(Map<String, Object> options) {
        Preconditions.checkNotNull(options, "options must not be null");
        URI changesFeedUri = this.uriHelper.changesUri(options);
        HttpConnection connection = Http.GET(changesFeedUri);
        return executeToJsonObject(connection, ChangesResult.class);
    }

    // TODO does this still work the same way we expect it to?
    public boolean contains(String id) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        URI doc = this.uriHelper.documentUri(id);
        try {
            HttpConnection connection = Http.HEAD(doc);
            this.executeToInputStream(connection);
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
            Response res = executeToJsonObject(connection, Response.class);
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

    public InputStream getDocumentStream(String id) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        URI doc = this.uriHelper.documentUri(id);
        HttpConnection connection = Http.GET(doc);
        return this.executeToInputStream(connection);
    }

    public InputStream getDocumentStream(String id, String rev) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rev), "rev must not be empty");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        URI doc = this.uriHelper.documentUri(id, queries);
        HttpConnection connection = Http.GET(doc);
        return this.executeToInputStream(connection);
    }

    public InputStream getAttachmentStream(String id, String attachmentName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        URI doc = this.uriHelper.attachmentUri(id, attachmentName);
        HttpConnection connection = Http.GET(doc);
        connection.requestProperties.put("Accept-Encoding", "gzip");
        return this.executeToInputStream(connection);
    }

    public InputStream getAttachmentStreamUncompressed(String id, String attachmentName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        URI doc = this.uriHelper.attachmentUri(id, attachmentName);
        HttpConnection connection = Http.GET(doc);
        return this.executeToInputStream(connection);
    }

    public InputStream getAttachmentStream(String id, String rev, String attachmentName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rev), "rev must not be empty");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        URI doc = this.uriHelper.attachmentUri(id, queries, attachmentName);
        HttpConnection connection = Http.GET(doc);
        connection.requestProperties.put("Accept-Encoding", "gzip");
        return this.executeToInputStream(connection);
    }

    public InputStream getAttachmentStreamUncompressed(String id, String rev, String attachmentName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rev), "rev must not be empty");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        URI doc = this.uriHelper.attachmentUri(id, queries, attachmentName);
        HttpConnection connection = Http.GET(doc);
        return this.executeToInputStream(connection);
    }

    public void putAttachmentStream(String id, String rev, String attachmentName, String attachmentString) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rev), "rev must not be empty");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        URI doc = this.uriHelper.attachmentUri(id, queries, attachmentName);
        HttpConnection connection = Http.PUT(doc, "application/json");
        connection.setRequestBody(attachmentString);
        this.executeToInputStream(connection);
    }

    public void putAttachmentStream(String id, String rev, String attachmentName, String contentType, byte[] attachmentData) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rev), "rev must not be empty");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        URI doc = this.uriHelper.attachmentUri(id, queries, attachmentName);
        HttpConnection connection = Http.PUT(doc, contentType);
        connection.setRequestBody(attachmentData);
        this.executeToInputStream(connection);
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

    public <T> T getDocument(String id, Map<String, Object> options, TypeReference<T> type)  {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkNotNull(type, "type must not be null");

        URI doc = this.uriHelper.documentUri(id, options);
        InputStream is = null;
        try {
            HttpConnection connection = Http.GET(doc);
            is = this.executeToInputStream(connection);
            return jsonHelper.fromJson(new InputStreamReader(is), type);
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
            return jsonHelper.fromJson(new InputStreamReader(is), type);
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
        return getDocRevisions(id, rev, new TypeReference<DocumentRevs>() {});
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
            is = this.executeToInputStream(connection);
            return jsonHelper.fromJson(new InputStreamReader(is), type);
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
        try {
            HttpConnection connection = Http.PUT(doc, "application/json");
            connection.setRequestBody(json);
            return executeToJsonObject(connection, Response.class);
        } catch (CouchException e) {
            if (e.getStatusCode() == 409) {
                throw new DocumentConflictException(e.toString());
            } else {
                throw e;
            }
        }
    }

    public Response delete(String id, String rev) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rev), "rev must not be empty");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        URI doc = this.uriHelper.documentUri(id, queries);
        try {
            HttpConnection connection = Http.DELETE(doc);
            return executeToJsonObject(connection, Response.class);
        } catch (CouchException e) {
            if (e.getStatusCode() == 409) {
                throw new DocumentConflictException(e.toString());
            } else {
                throw e;
            }
        }
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

    InputStream bulkInputStream(List<?> objects) {
        Preconditions.checkNotNull(objects, "Object list must not be null.");
        String newEditsVal = "\"new_edits\": false, ";
        URI uri = this.uriHelper.bulkDocsUri();
        String payload = String.format("{%s%s%s}", newEditsVal, "\"docs\": ",
                jsonHelper.toJson(objects));
        HttpConnection connection = Http.POST(uri, "application/json");
        connection.setRequestBody(payload);
        return this.executeToInputStream(connection);
    }

    public List<Response> bulk(Object... objects) {
        return bulk(Arrays.asList(objects));
    }

    public List<Response> bulk(List<?> objects) {
        InputStream is = null;
        try {
            is = bulkInputStream(objects);
            return jsonHelper.fromJsonToList(new InputStreamReader(is), new TypeReference<List<Response>>() {});
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
    public List<Response> bulkSerializedDocs(String... serializedDocs) {
        return bulkSerializedDocs(Arrays.asList(serializedDocs));
    }

    /**
     * Bulk insert a list of document that are serialized to JSON data already. For performance reasons,
     * the JSON doc is not validated.
     *
     * @param serializedDocs list of JSON documents
     * @return list of Response
     */
    public List<Response> bulkSerializedDocs(List<String> serializedDocs) {
        Preconditions.checkNotNull(serializedDocs, "Serialized doc list must not be null.");
        String payload = createBulkSerializedDocsPayload(serializedDocs);
        URI uri = this.uriHelper.bulkDocsUri();
        InputStream is = null;
        HttpConnection connection = Http.POST(uri, "application/json");
        connection.setRequestBody(payload);
        try {
            is = this.executeToInputStream(connection);
            return jsonHelper.fromJsonToList(new InputStreamReader(is), new TypeReference<List<Response>>() {});
        }
        finally {
            closeQuietly(is);
        }
    }

    private String createBulkSerializedDocsPayload(List<String> serializedDocs) {
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
            try {
                is = connection.execute().responseAsInputStream();
                Map<String, MissingRevisions> diff = jsonHelper.fromJson(new InputStreamReader(is),
                    new TypeReference<Map<String, MissingRevisions>>() { });
                return diff;
            } catch (IOException ioe) {
                // return empty map
                return new HashMap<String, MissingRevisions>();
            }
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
        connection.setRequestBody(mpw, mpw.getContentLength());
        return executeToJsonObject(connection, Response.class);
    }

    public static class MissingRevisions {
        public Set<String> possible_ancestors;
        public Set<String> missing;
    }

}
