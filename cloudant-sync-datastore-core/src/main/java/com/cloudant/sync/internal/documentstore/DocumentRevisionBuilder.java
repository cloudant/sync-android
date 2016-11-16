/*
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.internal.documentstore;

import com.cloudant.sync.documentstore.Attachment;
import com.cloudant.sync.documentstore.Database;
import com.cloudant.sync.documentstore.DocumentBody;
import com.cloudant.sync.documentstore.ProjectedDocumentRevision;

import java.util.Arrays;
import java.util.List;

/**
 * <p>Build {@link InternalDocumentRevision}s in a chained manner.</p>
 *
 * @api_private
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
    private Database database = null;

    /**
     * <p>Builds and returns the {@link InternalDocumentRevision} for this builder.</p>
     * @return the {@link InternalDocumentRevision} for this builder
     */
    public InternalDocumentRevision build() {
        DocumentRevisionOptions options = new DocumentRevisionOptions();
        options.sequence = sequence;
        options.docInternalId = docInternalId;
        options.deleted = deleted;
        options.current = current;
        options.parent = parent;
        options.attachments = attachments;
        return new InternalDocumentRevision(docId, revId, body, options);
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
     * @param database the datastore
     * @return the builder object for chained calls
     */
    public DocumentRevisionBuilder setDatabase(Database database) {
        this.database = database;
        return this;
    }

    /**
     * Builds and returns the {@link ProjectedDocumentRevision} for this builder.
     * @return {@link ProjectedDocumentRevision} for this builder.
     */
    public ProjectedDocumentRevision buildProjected() {
        return new ProjectedDocumentRevision(docId, revId, deleted, attachments, body, database);
    }

    static class DocumentRevisionOptions {
        public long sequence;
        // doc_id in revs table
        public long docInternalId;
        // is revision deleted?
        public boolean deleted;
        // is revision current? ("winning")
        public boolean current;
        // parent sequence number
        public long parent;
        // attachments associated with this revision
        public List<? extends Attachment> attachments;
    }

}
