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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Helper class to create url for Couch client.
 */
public class CouchURIHelper {

    private final static Joiner queryJoiner = Joiner.on('&').skipNulls();

    // CouchDB/Cloudant server instance URL. Does not end in /
    private final String base;

    /**
     * Returns a {@code CouchURIHelper} for a given Cloudant or CouchDB instance.
     */
    public CouchURIHelper(String protocol, String hostname, int port) {
        Preconditions.checkNotNull(protocol, "protocol must not be null");
        Preconditions.checkNotNull(hostname, "hostname must not be null");
        Preconditions.checkArgument((port > 0 && port < 65535), "Invalid port");

        base = String.format("%s://%s:%s", protocol, hostname, port);
    }

    /**
     * Returns URI for {@code _all_dbs} endpoint.
     */
    public URI allDbsUri() {
        String allDbs = String.format("%s/%s", this.base, "_all_dbs");
        return uriFor(allDbs);
    }

    /**
     * Returns URI for {@code db}.
     */
    public URI dbUri(String db) {
        Preconditions.checkNotNull(db, "db must not be null");

        String dbUri = String.format("%s/%s", this.base, this.encodePathComponent(db));
        return uriFor(dbUri);
    }

    /**
     * Returns URI for {@code _changes} endpoint for {@code db} using passed
     * {@code query}.
     */
    public URI changesUri(String db, Map<String, Object> query) {
        Preconditions.checkNotNull(db, "db must not be null");

        String base_uri = String.format(
                "%s/%s/%s",
                this.base,
                this.encodePathComponent(db),
                "_changes"
        );
        String uri = appendQueryString(base_uri, query);
        return uriFor(uri);
    }

    /**
     * Returns URI for {@code _bulk_docs} endpoint of {@code db}.
     */
    public URI bulkDocsUri(String db) {
        Preconditions.checkNotNull(db, "db must not be null");

        String uri = String.format(
                "%s/%s/%s",
                this.base,
                this.encodePathComponent(db),
                "_bulk_docs"
        );
        return uriFor(uri);
    }

    /**
     * Returns URI for {@code _revs_diff} endpoint of {@code db}.
     */
    public URI revsDiffUri(String db) {
        Preconditions.checkNotNull(db, "db must not be null");

        String uri = String.format(
                "%s/%s/%s",
                this.base,
                this.encodePathComponent(db),
                "_revs_diff"
        );
        return uriFor(uri);
    }

    /**
     * Returns URI for {@code documentId} in {@code db}.
     */
    public URI documentUri(String db, String documentId) {
        return documentUri(db, documentId, null);
    }

    /**
     * Returns URI for {@code documentId} in {@code db} with {@code query}.
     */
    public URI documentUri(String db, String documentId, Map<String, Object> query)  {
        Preconditions.checkNotNull(db, "db must not be null");
        Preconditions.checkNotNull(documentId, "documentId must not be null");

        String base_uri = String.format(
                "%s/%s/%s",
                this.base,
                this.encodePathComponent(db),
                this.encodePathComponent(documentId)
        );
        String uri = appendQueryString(base_uri, query);
        return uriFor(uri);
    }

    public URI attachmentUri(String db, String documentId, String attachmentId) {
        Preconditions.checkNotNull(db, "db must not be null");
        Preconditions.checkNotNull(documentId, "documentId must not be null");

        String base_uri = String.format(
                "%s/%s/%s/%s",
                this.base,
                this.encodePathComponent(db),
                this.encodePathComponent(documentId),
                this.encodePathComponent(attachmentId)
        );
        return uriFor(base_uri);
    }

    public URI attachmentUri(String db, String documentId, Map<String, Object> query, String attachmentId) {
        Preconditions.checkNotNull(db, "db must not be null");
        Preconditions.checkNotNull(documentId, "documentId must not be null");

        String base_uri = String.format(
                "%s/%s/%s/%s",
                this.base,
                this.encodePathComponent(db),
                this.encodePathComponent(documentId),
                this.encodePathComponent(attachmentId)
        );
        String uri = appendQueryString(base_uri, query);
        return uriFor(uri);
    }


    private String appendQueryString(String url, Map<String, Object> query) {
        if(query != null && query.size() > 0) {
            String queryString = this.queryAsString(query);
            return String.format("%s?%s", url, queryString);
        } else {
            return url;
        }
    }

    /**
     * Joins the entries in a map into a URL-encoded query string.
     * @param query map containing query params
     * @return joined, URL-encoded query params
     */
    private String queryAsString(Map<String, Object> query) {
        ArrayList<String> queryParts = new ArrayList<String>(query.size());
        for(Map.Entry<String, Object> entry : query.entrySet()) {
            String value = this.encodeQueryParameter(entry.getValue().toString());
            String key = this.encodeQueryParameter(entry.getKey().toString());
            String term = String.format("%s=%s", key, value);
            queryParts.add(term);
        }
        return queryJoiner.join(queryParts);
    }

    private URI uriFor(String uri) {
        try {
            return new URI(uri);
        }
        catch (URISyntaxException e) {
            throw new RuntimeException("Error constructing Couch URL", e);
        }
    }

    String encodePathComponent(String in) {
        // The only way to URL-encode in the Java library is to call the URI
        // constructor passing in the parts of the URL to encode. This makes
        // sense as the different parts of the URL require different encoding
        // methods (e.g., reserved chars changes by URI part).
        // It's unclear to me whether we need to use toASCIIString or whether
        // the characters in the "other" set are okay in URIs constructed
        // using new URI(uri).
        // We then have to encode the "/" manually, as we know it needs
        // encoding in this context.
        try {
            URI uri = new URI(
                    null, // scheme
                    null, // authority
                    in, // path
                    null, // query
                    null // fragment
            );
            return uri.toASCIIString().replace("/", "%2F");
        } catch (URISyntaxException e) {
            throw new RuntimeException(
                    "Couldn't encode path component " + in,
                    e);
        }
    }

    String encodeQueryParameter(String in) {
        // As described above, we need to use the Java URI class to encode
        // parameters, then munge into shape. See above for toASCIIString
        // caveat.
        // As this is to escape individual parameters, we need to
        // escape & and =.
        try {
            URI uri = new URI(
                    null, // scheme
                    null, // authority
                    null, // path
                    in, // query
                    null // fragment
            );
            return uri.toASCIIString()
                    .replace("&", "%26").replace("=", "%3D")  // encode qs separators
                    .substring(1);  // remove leading ?
        } catch (URISyntaxException e) {
            throw new RuntimeException(
                    "Couldn't encode query parameter " + in,
                    e);
        }
    }

}
