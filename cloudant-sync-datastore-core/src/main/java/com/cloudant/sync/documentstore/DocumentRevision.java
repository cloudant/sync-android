/*
 * Original iOS version by  Jens Alfke, ported to Android by Marty Schoch
 * Copyright © 2012 Couchbase, Inc. All rights reserved.
 *
 * Modifications for this distribution by Cloudant, Inc., Copyright © 2013 Cloudant, Inc.
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

import com.cloudant.sync.internal.common.ChangeNotifyingMap;
import com.cloudant.sync.internal.common.CouchUtils;
import com.cloudant.sync.internal.common.SimpleChangeNotifyingMap;
import com.cloudant.sync.internal.datastore.DocumentRevisionBuilder;
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
 * content for this revision as a {@link DocumentBody} object. Clients will
 * typically set only the revision content rather than the metadata
 * explicitly.</p>
 *
 * @api_public
 */
public class DocumentRevision implements Comparable<DocumentRevision> {

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
    private  boolean deleted;

    protected boolean fullRevision = true;

    ChangeNotifyingMap<String, Attachment> attachments = SimpleChangeNotifyingMap.wrap(new
            HashMap<String, Attachment>());

    private DocumentBody body;

    boolean bodyModified = false;
    private long sequence = - 1;
    private long internalNumericId;
    private boolean current;
    private long parent = -1L;

    public DocumentRevision() {
        // BasicDatastore#createDocumentFromRevision will assign an id
        this(null);
    }

    public DocumentRevision(String id) {
       this(id, null);
    }

    public DocumentRevision(String id, String revision) {
        this(id,revision, null);
    }

    public DocumentRevision(String docId, String revId, DocumentBody body) {
        this(docId, revId, body, null);

    }

    public DocumentRevision(String docId, String revId, DocumentBody body, DocumentRevisionBuilder.DocumentRevisionOptions options) {
        this.id = docId;
        this.revision = revId;
        this.body = body;

        if(options != null) {
            this.deleted = options.deleted;
            this.sequence = options.sequence;
            this.current = options.current;
            this.internalNumericId = options.docInternalId;
            this.parent = options.parent;
            this.setAttachmentsInternal(options.attachments);
        } else {
            this.current = true;
            this.deleted = false;
        }
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
     * the rest of the document
     */
    public DocumentBody getBody() {
        return body;
    }

    public void setBody(DocumentBody body) {
        this.body = body;
        this.bodyModified = true;
    }

    /**
     * top level key: _attachments
     */
    public Map<String, Attachment> getAttachments() {
        return attachments;
    }

    public void setAttachments(Map<String, Attachment> attachments) {
        if (attachments != null) {
            this.attachments = SimpleChangeNotifyingMap.wrap(attachments);
        } else {
            // user cleared the dict, we don't want our notifying map to try to forward to null
            this.attachments = null;
        }
        // user reset the whole attachments dict, this is a change
        this.bodyModified = true;
    }

    /*
     * Helper used by sub-classes to convert between list and map representation of attachments
     */
    protected void setAttachmentsInternal(List<? extends Attachment> attachments)
    {
        if (attachments != null) {
            // this awkward looking way of doing things is to avoid marking the map as being modified
            HashMap<String, Attachment> m = new HashMap<String, Attachment>();
            for (Attachment att : attachments) {
                m.put(att.name, att);
            }
            this.attachments = SimpleChangeNotifyingMap.wrap(m);
        }
    }

    /**
     * @return {@code true} if this revision is marked deleted.
     */
    public boolean isDeleted() {
        return deleted;
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
        return fullRevision;
    }

    /**
     * Returns a "Full" document revision.
     *
     * A full document revision is a revision which contains all the information associated with it.
     * For example when a document is returned from
     * {@link QueryImpl#find(Map, long, long, List, List)} using the
     * {@code fields} option to limit the fields returned, a revision will be missing data so it
     * cannot be regarded as a full revision. If the document is a full revision, this method will
     * only attempt to load the full revision from the datastore if {@link DocumentRevision#isFullRevision()}
     * returns false.
     *
     * @return A "Full" document revision.
     * @throws DocumentNotFoundException Thrown if the full document cannot be loaded from the
     * datastore.
     */
    public DocumentRevision toFullRevision() throws DocumentNotFoundException {
        return this;
    }

    public void setRevision(String revision) {
        this.revision = revision;
    }

    /**
     * @return {@code true} if this revision is the current winner for the
     * document.
     */
    public boolean isCurrent(){
        return current;
    }

    /**
     * <p>Returns the sequence number of this revision's parent revision.</p>
     *
     * <p>If this number is less than or equal to zero, it means this
     * revision is the root of a document tree. A document may have more than
     * one tree, under certain circumstances, such as two documents with the
     * same ID being created in different datastores that are later replicated
     * across.</p>
     *
     * @return the sequence number of this revision's parent revision.
     */
    public long getParent() {
        return this.parent;
    }

    /**
     * <p>Returns the internal numeric ID of this document revision.</p>
     *
     * <p>This can be useful for efficient storage by plugins extending the
     * datastore.</p>
     *
     * @return  the internal numeric ID of this document revision.
     */
    public long getInternalNumericId(){
        return internalNumericId;
    }

    /**
     * <p>Returns the JSON body of the document revision as a {@code Map}
     * object.</p>
     *
     * <p>This Map includes reserved fields such as {@code _id} and
     * {@code _rev}. Changing the byte array may affect the {@code DocumentRevision},
     * as only a shallow copy is returned.</p>
     *
     * @return the JSON body of the document revision as a {@code Map}
     * object.
     */
    public Map<String,Object> asMap() {
           return addMetaProperties(getBody().asMap());
    }

    Map<String, Object> addMetaProperties(Map<String, Object> map) {
        map.put("_id", id);
        if(revision != null) {
            map.put("_rev", revision);
        }
        if (this.isDeleted()) {
            map.put("_deleted", true);
        }
        return map;
    }

    /**
     * <p>Returns the JSON body of the document revision as a {@code byte}
     * array.</p>
     *
     * <p>This byte array includes reserved fields such as {@code _id} and
     * {@code _rev}. Changing the byte array does not affect the document
     * revisions contents.</p>
     *
     * @return the JSON body of the document revision as a {@code byte}
     * array.
     */
    public byte[] asBytes() {
        byte[] result = null;
        if(getBody() != null) {
            result = getBody().asBytes();
        }
        return result;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    /**
     * <p>Returns the sequence number of this revision.</p>
     *
     * <p>The sequence number is unique across the database, it is updated
     * for every modification to the datastore.</p>
     *
     * @return the sequence number of this revision.
     */
    public long getSequence() {
        return sequence;
    }

    public void initialiseSequence(long sequence) {
        if (this.sequence == -1) {
            this.sequence = sequence;
        }
    }

    /**
     * @return Whether the body has been modified since this DocumentRevision was constructed or
     * retrieved from the Datastore. For internal use only.
     *
     * @api_private
     */
    public boolean isBodyModified() {
        return bodyModified;
    }

    @Override
    public String toString() {
        return "{ id: " + this.id + ", rev: " + this.revision + ", seq: " + sequence + ", parent: " + parent + ", current: " + current + ", deleted " + deleted +" }";
    }

    public int getGeneration() {
        return CouchUtils.generationFromRevId(revision);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DocumentRevision that = (DocumentRevision) o;

        if (!id.equals(that.id)) {
            return false;
        }
        return revision != null ? revision.equals(that.revision) : that.revision == null;

    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + (revision != null ? revision.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(DocumentRevision o) {
        return Long.valueOf(getSequence()).compareTo(o.getSequence());
    }


}
