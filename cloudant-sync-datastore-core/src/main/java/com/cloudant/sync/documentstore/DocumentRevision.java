/*
 * Copyright © 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.documentstore;

import com.cloudant.sync.internal.documentstore.InternalDocumentRevision;
import com.cloudant.sync.internal.query.QueryImpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * <p>A single revision of a document within a datastore.</p>
 *
 * <p>Documents within the datastore are in fact trees of document revisions,
 * with one document marked as the current winner at any point. Branches in
 * the tree are caused when a document is edited in more than one place before
 * being replicated between datastores. The consuming application is responsible
 * for finding active branches (also called conflicts), and marking the leaf
 * nodes of all branches but one deleted (thereby resolving the conflict).</p>
 *
 * <p>A {@code DocumentRevision} contains all the information for a single document
 * revision, including its ID and revision ID, along with the document's
 * content for this revision as a {@link DocumentBody} object, and its {@link Attachment}s. Clients will
 * typically set only the revision content rather than the metadata
 * explicitly.</p>
 *
 * @api_public
 */
public class DocumentRevision {

    /**
     * top level key: _id
     */
    protected final String id;

    /**
     * top level key: _rev
     */
    protected String revision;

    /**
     * top level key: _deleted
     */
    protected boolean deleted;

    /**
     * top level key: _attachments
     */
    protected Map<String, Attachment> attachments = new HashMap<String, Attachment>();

    /**
     * the rest of the document
     */
    protected DocumentBody body;

    public DocumentRevision() {
        // BasicDatastore#create will assign an ID
        this(null);
    }

    public DocumentRevision(String id) {
        this(id, null);
    }

    public DocumentRevision(String id, String revision) {
        this(id,revision, null);
    }

    public DocumentRevision(String id, String revision, DocumentBody body) {
        this.id = id;
        this.revision = revision;
        this.body = body;
    }

    /**
     * @return the unique identifier of the document
     */
    public String getId() {
        return id;
    }

    /**
     * @return the revision ID of this document revision
     */
    public String getRevision() {
        return revision;
    }

    /**
     * @return {@code true} if this revision is marked deleted.
     */
    public boolean isDeleted() {
        return deleted;
    }

    public Map<String, Attachment> getAttachments() {
        return attachments;
    }

    public void setAttachments(Map<String, Attachment> attachments) {
        this.attachments = attachments;
    }

    public DocumentBody getBody() {
        return body;
    }

    public void setBody(DocumentBody body) {
        this.body = body;
    }

    /**
     * Returns true if this revision is a full revision. A full revision is a revision which
     * contains all the data related to the revision. For example
     * revisions returned from query where only select fields have been included
     * is <strong>not</strong> regarded as a full revision.
     *
     * @return {@code true} if this revision is a full revision.
     */
    public boolean isFullRevision(){
        return true;
    }

    /**
     * Returns a "Full" document revision.
     *
     * A full document revision is a revision which contains all the information associated with it.
     * For example when a document is returned from
     * {@link QueryImpl#find(Map, long, long, List, List)} using the
     * {@code fields} option to limit the fields returned, a revision will be missing data so it
     * cannot be regarded as a full revision. If the document is a full revision, this method will
     * only attempt to load the full revision from the datastore if {@link InternalDocumentRevision#isFullRevision()}
     * returns false.
     *
     * @return A "Full" document revision.
     * @throws DocumentNotFoundException if the full document cannot be loaded from the Database
     * @throws DocumentStoreException if there was an error reading from the database
     */
    public DocumentRevision toFullRevision() throws DocumentNotFoundException, DocumentStoreException {
        return this;
    }

}
