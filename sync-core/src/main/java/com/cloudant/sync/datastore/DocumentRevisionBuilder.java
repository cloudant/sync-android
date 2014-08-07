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

import java.util.List;

/**
 * <p>Build {@code DocumentRevision}s in a chained manner.</p>
 */
public class DocumentRevisionBuilder {

    private String docId = null;
    private String revId = null;
    private DocumentBody body = null;

    private long sequence = -1;
    private boolean current = false;
    private boolean deleted = false;
    private long docInternalId = -1;
    private long parent = -1;
    private List<? extends Attachment> attachments = null;

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
    public DocumentRevision buildLocalDocument() {
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
    public DocumentRevision buildStub() {
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


}
