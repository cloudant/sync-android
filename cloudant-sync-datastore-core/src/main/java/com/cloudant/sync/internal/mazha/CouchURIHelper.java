/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2013 Cloudant
 *
 * Copyright © 2011 Ahmed Yehia (ahmed.yehia.m@gmail.com)
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

import com.cloudant.sync.internal.common.CouchConstants;
import com.cloudant.sync.internal.util.JSONUtils;
import com.cloudant.sync.internal.util.Misc;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Helper class to create url for Couch client.
 */
public class CouchURIHelper {

    // CouchDB/Cloudant server instance URL. Does not end in /
    private final URI rootUri;
    private final String rootUriString;
    /**
     * Returns a {@code CouchURIHelper} for a given Cloudant or CouchDB instance.
     */
    public CouchURIHelper(URI rootUri) {
        this.rootUri = rootUri;
        this.rootUriString = rootUri.toASCIIString();
    }

    /**
     * Returns the Root URI for this instance.
     */
    public URI getRootUri() {
        return this.rootUri;
    }

    /**
     * Returns URI for {@code _changes} endpoint using passed
     * {@code query}.
     */
    public URI changesUri(Map<String, Object> query) {
        String base_uri = String.format(
                "%s/%s",
                this.rootUriString,
                "_changes"
        );

        //lets find the since parameter
        if(query.containsKey("since")){
            Object since = query.get("since");
            if(!(since instanceof String)){
                //json encode the seq number since it isn't a string
                query.put("since", JSONUtils.toJson(since));
            }
        }

        String uri = appendQueryString(base_uri, query);
        return uriFor(uri);
    }

    /**
     * Returns URI for {@code _bulk_docs} endpoint.
     */
    public URI bulkDocsUri() {
        String uri = String.format(
                "%s/%s",
                this.rootUriString,
               "_bulk_docs"
        );
        return uriFor(uri);
    }

    /**
     * Returns URI for {@code _revs_diff} endpoi
     */
    public URI revsDiffUri() {
        String uri = String.format(
                "%s/%s",
                this.rootUriString,
                "_revs_diff"
        );
        return uriFor(uri);
    }

    /**
     * Returns URI for {@code documentId}.
     */
    public URI documentUri(String documentId) {
        return documentUri(documentId, null);
    }

    /**
     * Returns URI for {@code documentId} with {@code query}.
     */
    public URI documentUri(String documentId, Map<String, Object> query)  {
        String base_uri = String.format(
                "%s/%s",
                this.rootUriString,
                this.encodeId(documentId)
        );
        String uri = appendQueryString(base_uri, query);
        return uriFor(uri);
    }

    /**
     * Returns URI for Attachment having {@code attachmentId} for {@code documentId}.
     */
    public URI attachmentUri(String documentId, String attachmentId) {
        String base_uri = String.format(
                "%s/%s/%s",
                this.rootUriString,
                this.encodeId(documentId),
                this.encodeId(attachmentId)
        );
        return uriFor(base_uri);
    }

    /**
     * Returns URI for Attachment having {@code attachmentId} for {@code documentId} with {@code query}.
     */
    public URI attachmentUri(String documentId, Map<String, Object> query, String attachmentId) {
        String base_uri = String.format(
                "%s/%s/%s",
                this.rootUriString,
                this.encodeId(documentId),
                this.encodeId(attachmentId)
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
            String key = this.encodeQueryParameter(entry.getKey());
            String term = String.format("%s=%s", key, value);
            queryParts.add(term);
        }
        return Misc.join("&", queryParts);
    }

    private URI uriFor(String uri) {
        try {
            return new URI(uri);
        }
        catch (URISyntaxException e) {
            throw new RuntimeException("Error constructing Couch URL", e);
        }
    }

    /**
     * Encode a CouchDb Document or Attachment ID in a manner suitable for a GET request
     * @param in The Document or Attachment ID to encode, eg "a/document"
     * @return The encoded Document or Attachment ID, eg "a%2Fdocument"
     */
    String encodeId(String in) {
        try {
            String encodedString = HierarchicalUriComponents.encodeUriComponent(in, "UTF-8",
                    HierarchicalUriComponents.Type.PATH_SEGMENT);
            if (encodedString.startsWith(CouchConstants._design_prefix_encoded) ||
                    encodedString.startsWith(CouchConstants._local_prefix_encoded)) {
                // we replaced a the first slash in the design or local doc URL, which we shouldn't
                // so let's put it back
                return encodedString.replaceFirst("%2F", "/");
            } else {
                return encodedString;
            }
        } catch (UnsupportedEncodingException uee) {
            throw new RuntimeException(
                    "Couldn't encode ID " + in,
                    uee);
        }
    }

    String encodeQueryParameter(String in) {
        // As described above, we need to use the Java URI class to encode
        // parameters, then munge into shape. See above for toASCIIString
        // caveat.
        // As this is to escape individual parameters, we need to
        // escape & and =.
        try {
            String encodedString = HierarchicalUriComponents.encodeUriComponent(in, "UTF-8",
                    HierarchicalUriComponents.Type.QUERY_PARAM);
            return encodedString;
        } catch (UnsupportedEncodingException uee) {
            throw new RuntimeException(
                    "Couldn't encode query parameter " + in,
                    uee);
        }
    }

}
