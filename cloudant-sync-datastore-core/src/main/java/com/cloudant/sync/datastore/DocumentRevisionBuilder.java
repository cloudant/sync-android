/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.datastore;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <p>Build {@code DocumentRevision}s in a chained manner.</p>
 */
public class DocumentRevisionBuilder {


    static {
        // these values are defined http://docs.couchdb.org/en/latest/api/document/common.html

        allowedPrefixes = Arrays.asList(new String[]{
                "_id",
                "_rev",
                "_deleted",
                "_attachments",
                "_conflicts",
                "_deleted_conflicts",
                "_local_seq",
                "_revs_info",
                "_revisions"
        });

    }

    private static List<String> allowedPrefixes;

    private String docId = null;
    private String revId = null;
    private DocumentBody body = null;

    private long sequence = -1;
    private boolean current = false;
    private boolean deleted = false;
    private long docInternalId = -1;
    private long parent = -1;
    private List<? extends Attachment> attachments = null;
    private Datastore datastore = null;

    /**
     * <p>Builds and returns the {@code BasicDocumentRevision} for this builder.</p>
     * @return the {@code BasicDocumentRevision} for this builder
     */
    public BasicDocumentRevision build() {
        BasicDocumentRevision.BasicDocumentRevisionOptions options = new BasicDocumentRevision.BasicDocumentRevisionOptions();
        options.sequence = sequence;
        options.docInternalId = docInternalId;
        options.deleted = deleted;
        options.current = current;
        options.parent = parent;
        options.attachments = attachments;
        return new BasicDocumentRevision(docId, revId, body, options);
    }

    /**
     * <p>Builds and returns the {@code BasicDocumentRevision} for this builder, as a
     * local document.</p>
     * @return the {@code BasicDocumentRevision} for this builder as a local document
     */
    protected BasicDocumentRevision buildBasicDBObjectLocalDocument() {
        assert this.revId.endsWith("-local");
        return new BasicDocumentRevision(docId, revId, body);
    }

    /**
     * <p>Builds and returns the {@code DocumentRevision} for this builder, as a
     * local document.</p>
     * @return the {@code DocumentRevision} for this builder as a local document
     */
    public BasicDocumentRevision buildLocalDocument() {
        assert this.revId.endsWith("-local");
        return new BasicDocumentRevision(docId, revId, body);
    }


    /**
     * <p>Builds and returns the stub {@code DocumentRevision} for this builder.</p>
     *
     * <p>A "stub" object has document and revision IDs, but no content/body.
     * </p>
     *
     * @return the stub {@code DocumentRevision} for this builder
     */
    public BasicDocumentRevision buildStub() {
        assert body == null;
        return new BasicDocumentRevision(docId, revId);
    }

    /**
     * <p>Sets the document ID for this builder.</p>
     * @param docId document ID
     * @return the builder object for chained calls
     */
    public DocumentRevisionBuilder setDocId(String docId) {
        this.docId = docId;
        return this;
    }

    /**
     * <p>Sets the revision ID for this builder.</p>
     * @param revId revision ID
     * @return the builder object for chained calls
     */
    public DocumentRevisionBuilder setRevId(String revId) {
        this.revId = revId;
        return this;
    }

    /**
     * <p>Sets the document body for this builder.</p>
     * @param body document revision body
     * @return the builder object for chained calls
     */
    public DocumentRevisionBuilder setBody(DocumentBody body) {
        this.body = body;
        return this;
    }

    /**
     * <p>Sets whether this builder is creating the winning revision.</p>
     * @param current whether this document revision is the winning revision
     * @return the builder object for chained calls
     */
    public DocumentRevisionBuilder setCurrent(boolean current) {
        this.current = current;
        return this;
    }

    /**
     * <p>Sets whether this builder is creating a deleted revsion.</p>
     * @param deleted whether this revision is deleted
     * @return the builder object for chained calls
     */
    public DocumentRevisionBuilder setDeleted(boolean deleted) {
        this.deleted = deleted;
        return this;
    }

    /**
     * <p>Sets the internal ID for this builder.</p>
     * @param internalId  revision internal ID
     * @return the builder object for chained calls
     */
    public DocumentRevisionBuilder setInternalId(long internalId) {
        this.docInternalId = internalId;
        return this;
    }

    /**
     * <p>Sets the sequence number for this builder.</p>
     * @param sequence  sequence number
     * @return the builder object for chained calls
     */
    public DocumentRevisionBuilder setSequence(long sequence) {
        this.sequence = sequence;
        return this;
    }

    /**
     * <p>Sets the parent sequence number for this builder.</p>
     * @param parent parent sequence number
     * @return the builder object for chained calls
     */
    public DocumentRevisionBuilder setParent(long parent) {
        this.parent = parent;
        return this;
    }

    /**
     * <p>Sets the attachments associated with the document for this builder.</p>
     * @param attachments list of attachments associated with the document
     * @return the builder object for chained calls
     */
    public DocumentRevisionBuilder setAttachments(List<? extends Attachment> attachments) {
        this.attachments = attachments;
        return this;
    }

    /**
     * <p>Sets the datastore for this builder.</p>
     * @param datastore the datastore
     * @return the builder object for chained calls
     */
    public DocumentRevisionBuilder setDatastore(Datastore datastore) {
        this.datastore = datastore;
        return this;
    }

    /**
     * Builds and returns the {@link com.cloudant.sync.datastore.MutableDocumentRevision} for this builder.
     * @return {@link com.cloudant.sync.datastore.MutableDocumentRevision} for this builder.
     */
    public MutableDocumentRevision buildMutable() {
        MutableDocumentRevision revision = new MutableDocumentRevision(this.revId);
        revision.body = this.body;
        revision.docId = this.docId;
        return revision;
    }

    /**
     * Builds and returns the {@link com.cloudant.sync.datastore.ProjectedDocumentRevision} for this builder.
     * @return {@link com.cloudant.sync.datastore.ProjectedDocumentRevision} for this builder.
     */
    public ProjectedDocumentRevision buildProjected() {
        BasicDocumentRevision.BasicDocumentRevisionOptions options = new BasicDocumentRevision.BasicDocumentRevisionOptions();
        options.sequence = sequence;
        options.docInternalId = docInternalId;
        options.deleted = deleted;
        options.current = current;
        options.parent = parent;
        options.attachments = attachments;
        return new ProjectedDocumentRevision(docId, revId, body, options, datastore);
    }

    /**
     * Builds a BasicDocumentRevision from a Map of values from a CouchDB instance
     * @param documentURI The URI of the document
     * @param map The map of key value pairs fom the CouchDB server
     * @return A complete document revision representing the data from the Map
     * @throws IOException If attachments fail to be decoded correctly.
     */
    public static BasicDocumentRevision buildRevisionFromMap(URI documentURI, Map<String, ? extends Object> map) throws IOException {

        for (String key : map.keySet()) {

            if (key.startsWith("_") && !allowedPrefixes.contains(key)) {
                throw new IllegalArgumentException("Custom _ prefix keys are not allowed");
            }
        }

        String docId = (String) map.get("_id");
        String revId = (String) map.get("_rev");
        Boolean deleted = map.get("_deleted") == null ? false : (Boolean) map.get("_deleted");
        Map<String, ?> attachmentDataMap = (Map<String, ?>) map.get("_attachments");
        List<Attachment> attachments = new LinkedList<Attachment>();

        if(attachmentDataMap != null) {

            for (String key : attachmentDataMap.keySet()) {
                documentURI.getQuery();
                String attachmentURIPath = documentURI.getPath()+"/"+key;

                URI attachmentURI= null;
                try {
                    attachmentURI = new URI(documentURI.getScheme(),
                            documentURI.getUserInfo(),
                            documentURI.getHost(),
                            documentURI.getPort(),
                            attachmentURIPath,
                            documentURI.getQuery(),
                            documentURI.getFragment());
                } catch (URISyntaxException e) {
                    throw new IllegalArgumentException(e);
                }

                SavedHttpAttachment attachment =
                        new SavedHttpAttachment(key,
                                (Map<String, Object>) attachmentDataMap.get(key),
                                attachmentURI);
                attachments.add(attachment);

            }
        }


        Map<String, Object> body = new HashMap<String, Object>();
        Set<String> keys = map.keySet();

        keys.removeAll(allowedPrefixes);

        for (String key : keys) {
            body.put(key, map.get(key));
        }

        DocumentBody docBody = DocumentBodyFactory.create(body);

        DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
        builder.setDocId(docId).setRevId(revId).setDeleted(deleted).setBody(docBody).setAttachments(attachments);

        return builder.build();

    }


}
