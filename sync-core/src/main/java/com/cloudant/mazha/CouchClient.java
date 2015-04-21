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
import com.cloudant.sync.datastore.MultipartAttachmentWriter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.http.client.params.HttpClientParams;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;

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

    public static final String COUCH_ERROR_CONFLICT = "conflict";

    private final HttpRequests httpClient;
    protected final JSONHelper jsonHelper;
    private CouchURIHelper uriHelper;

    public CouchClient(CouchConfig config) {
        this.httpClient = new HttpRequests(this.getHttpConnectionParams(config),
                config.getRootUri().getUserInfo(),
                config.getCustomHeaders());
        this.jsonHelper = new JSONHelper();
        this.uriHelper = new CouchURIHelper(config.getRootUri());
    }

    public URI getRootUri() {
        return this.uriHelper.getRootUri();
    }

    HttpRequests getHttpClient() {
        return httpClient;
    }

    public List<String> getList(URI uri) {
        InputStream is = null;
        try {
            is = httpClient.get(uri);
            return jsonHelper.fromJsonToList(new InputStreamReader(is), JSONHelper.STRING_LIST_TYPE_DEF);
        } finally {
            closeQuietly(is);
        }
    }

    public void createDb() throws CouchException {
        InputStream is = null;
        try {
            is = httpClient.put(this.uriHelper.getRootUri());
            DBOperationResponse res = jsonHelper.fromJson(new InputStreamReader(is), DBOperationResponse.class);
            if (!res.getOk()) {
                throw new ServerException("Response from couch db server: " + res.toString());
            }
        } finally {
            closeQuietly(is);
        }
    }

    public void deleteDb() {
        InputStream is = null;
        try {
            is = httpClient.delete(this.uriHelper.getRootUri());
            DBOperationResponse res = jsonHelper.fromJson(new InputStreamReader(is), DBOperationResponse.class);
            if (!res.getOk()) {
                throw new ServerException("Response from couch db server: " + res.toString());
            }
        } finally {
            closeQuietly(is);
        }
    }

    public CouchDbInfo getDbInfo() {
        InputStream is = null;
        try {
            is = httpClient.get(this.uriHelper.getRootUri());
            return jsonHelper.fromJson(new InputStreamReader(is), CouchDbInfo.class);
        } finally {
            closeQuietly(is);
        }
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
        InputStream is = null;
        try {
            URI changesFeedUri = this.uriHelper.changesUri(options);
            is = httpClient.get(changesFeedUri);
            return jsonHelper.fromJson(new InputStreamReader(is), ChangesResult.class);
        } finally {
            closeQuietly(is);
        }
    }

    public boolean contains(String id) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        URI doc = this.uriHelper.documentUri(id);
        try {
            httpClient.head(doc);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public Response create(Object document) {
        String json = jsonHelper.toJson(document);
        InputStream is = null;
        try {
            is = httpClient.post(this.uriHelper.getRootUri(), json);
            Response res = jsonHelper.fromJson(new InputStreamReader(is), Response.class);
            if (!res.getOk()) {
                throw new ServerException(res.toString());
            } else {
                return res;
            }
        } catch (CouchException e) {
            if (COUCH_ERROR_CONFLICT.equals(e.getError())) {
                throw new DocumentConflictException(e.toString());
            }
            throw e;
        } finally {
            closeQuietly(is);
        }
    }

    public InputStream getDocumentStream(String id) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        URI doc = this.uriHelper.documentUri(id);
        return httpClient.get(doc);
    }

    public InputStream getDocumentStream(String id, String rev) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rev), "rev must not be empty");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        URI doc = this.uriHelper.documentUri(id, queries);
        return httpClient.get(doc);
    }

    public InputStream getAttachmentStream(String id, String attachmentName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        URI doc = this.uriHelper.attachmentUri(id, attachmentName);
        return httpClient.getCompressed(doc);
    }

    public InputStream getAttachmentStreamUncompressed(String id, String attachmentName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        URI doc = this.uriHelper.attachmentUri(id, attachmentName);
        return httpClient.get(doc);
    }

    public InputStream getAttachmentStream(String id, String rev, String attachmentName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rev), "rev must not be empty");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        URI doc = this.uriHelper.attachmentUri(id, queries, attachmentName);
        return httpClient.getCompressed(doc);
    }

    public InputStream getAttachmentStreamUncompressed(String id, String rev, String attachmentName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rev), "rev must not be empty");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        URI doc = this.uriHelper.attachmentUri(id, queries, attachmentName);
        return httpClient.get(doc);
    }

    public void putAttachmentStream(String id, String rev, String attachmentName, String attachmentString) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rev), "rev must not be empty");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        URI doc = this.uriHelper.attachmentUri(id, queries, attachmentName);
        httpClient.put(doc, attachmentString);
    }

    public void putAttachmentStream(String id, String rev, String attachmentName, String contentType, byte[] attachmentData) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rev), "rev must not be empty");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        URI doc = this.uriHelper.attachmentUri(id, queries, attachmentName);
        httpClient.put(doc, contentType, attachmentData);
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
    public <T> T getDocConflictRevs(String id) {
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

    public <T> T getDocument(String id, final Class<T> type) {
        return this.getDocument(id, new HashMap<String, Object>(), new TypeReference<T>() {
            @Override
            public Type getType() {
                return type;
            }
        });
    }

    public <T> T getDocument(String id, Map<String, Object> options, TypeReference<T> type) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkNotNull(type, "type must not be null");

        URI doc = this.uriHelper.documentUri(id, options);
        InputStream is = null;
        try {
            is = this.httpClient.get(doc);
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
            is = httpClient.get(findRevs);
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
        InputStream is = null;
        try {
            is = httpClient.put(doc, json);
            return jsonHelper.fromJson(new InputStreamReader(is), Response.class);
        } catch (CouchException e) {
            if (COUCH_ERROR_CONFLICT.equals(e.getError())) {
                throw new DocumentConflictException(e.toString());
            } else {
                throw e;
            }
        } finally {
            closeQuietly(is);
        }
    }

    public Response delete(String id, String rev) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "id must not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rev), "rev must not be empty");
        Map<String, Object> queries = new HashMap<String, Object>();
        queries.put("rev", rev);
        URI doc = this.uriHelper.documentUri(id, queries);
        InputStream is = null;
        try {
            is = httpClient.delete(doc);
            return jsonHelper.fromJson(new InputStreamReader(is), Response.class);
        } catch (CouchException e) {
            if (COUCH_ERROR_CONFLICT.equals(e.getError())) {
                throw new DocumentConflictException(e.toString());
            } else {
                throw e;
            }
        } finally {
            closeQuietly(is);
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
        return httpClient.post(uri, payload);
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
        try {
            is = httpClient.post(uri, payload);
            return jsonHelper.fromJsonToList(new InputStreamReader(is), new TypeReference<List<Response>>() {});
        } finally {
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
            is = this.httpClient.post(uri, payload);
            Map<String, MissingRevisions> diff = jsonHelper.fromJson(new InputStreamReader(is),
                    new TypeReference<Map<String, MissingRevisions>>() { });
            return diff;
        } finally {
            closeQuietly(is);
        }
    }

    public Response putMultipart(MultipartAttachmentWriter mpw) {
        Map<String, Object> options = new HashMap<String, Object>();
        options.put("new_edits", "false");
        URI uri = this.uriHelper.documentUri(mpw.getId(), options);
        HashMap<String, String> headers = new HashMap<String, String>();
        String contentType = "multipart/related;boundary=" + mpw.getBoundary();
        InputStream is = this.httpClient.putStream(uri, contentType, mpw, mpw.getContentLength());
        return jsonHelper.fromJson(new InputStreamReader(is), Response.class);
    }

    public static class MissingRevisions {
        public Set<String> possible_ancestors;
        public Set<String> missing;
    }

    /**
     * Shutdown the client and release all the allocated resources like connection pool
     */
    public void shutdown() {
        if(httpClient != null) {
            httpClient.shutdown();
        }
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


        return params;
    }
}
