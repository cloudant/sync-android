/**
 * Original iOS version by  Jens Alfke, ported to Android by Marty Schoch
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 *
 * Modifications for this distribution by Cloudant, Inc., Copyright (c) 2013 Cloudant, Inc.
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

import com.cloudant.sync.util.CouchUtils;
import com.cloudant.sync.util.JSONUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  Revision of a document.
 *
 */
public class BasicDocumentRevision implements DocumentRevision, Comparable<BasicDocumentRevision> {

    private final DocumentBody body;

    private long sequence = - 1;
    private long internalNumericId;

    private boolean deleted;
    private boolean current;
    private long parent = -1L;

    private int generation = -1;

    private Map<String, Attachment> attachments;

    static class BasicDocumentRevisionOptions {
        long sequence;
        // doc_id in revs table
        long docInternalId;
        // is revision deleted?
        boolean deleted;
        // is revision current? ("winning")
        boolean current;
        // parent sequence number
        long parent;
        // attachments associated with this revision
        List<? extends Attachment> attachments;
    }

    BasicDocumentRevision(String docId, String revId, DocumentBody body, BasicDocumentRevisionOptions options) {
        CouchUtils.validateRevisionId(revId);
        CouchUtils.validateDocumentId(docId);
        assert body != null;

        this.id = docId;
        this.revision = revId;
        this.deleted = options.deleted;
        this.body = body;
        this.sequence = options.sequence;
        this.current = options.current;
        this.internalNumericId = options.docInternalId;
        this.parent = options.parent;
        this.attachments = new HashMap<String, Attachment>();
        if (options.attachments != null) {
            for (Attachment att : options.attachments) {
                this.attachments.put(att.name, att);
            }
        }

    }

    BasicDocumentRevision(String docId, String revId, DocumentBody body) {
        CouchUtils.validateDocumentId(docId);
        CouchUtils.validateRevisionId(revId);
        assert body != null;

        this.id = docId;
        this.revision = revId;
        this.body = body;
        this.current = true;
        this.deleted = false;
    }

    BasicDocumentRevision(String docId, String revId) {
        CouchUtils.validateDocumentId(docId);
        CouchUtils.validateRevisionId(revId);

        this.id = docId;
        this.revision = revId;
        this.body = BasicDocumentBody.bodyWith(JSONUtils.EMPTY_JSON);
    }

    /**
     * The unique identifier of the document (mandatory and immutable)
     */
    private String id;
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }

    /**
     * The current MVCC-token/revision of this document (mandatory and immutable)
     */
    private String revision;
    public String getRevision() {
        return this.revision;
    }
    public void setRevision(String revision) {
        this.revision = revision;
    }

    @Override
    public DocumentBody getBody() {
        return body;
    }

    public boolean isLocal(){
        return revision.endsWith("-local");
    }

    /**
     * <p>Returns {@code true} if this revision is the current winner for the
     * document.</p>
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
    long getInternalNumericId(){
        return internalNumericId;
    }

    Map<String, Object> map = null;

    /**
     * <p>Returns the JSON body of the document revision as a {@code Map}
     * object.</p>
     *
     * <p>This Map includes reserved fields such as {@code _id} and
     * {@code _rev}. Changing the byte array may affect the {@code DocumentRevision},
     * as only a shallow copy is returned.</p>
     */
    public Map<String,Object> asMap() {
        if(map == null) {
            map = addMetaProperties(body.asMap());
        }
        return map;
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
     */
    public byte[] asBytes() {
        byte[] result = null;
        if(body != null) {
            result = body.asBytes();
        }
        return result;
    }

    /**
     * <p>Returns {@code true} if this revision is marked deleted.</p>
     */
    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    /**
     * <p>Returns the sequence number of this revision.</p>
     *
     * <p>The sequence number is unique across the database, it is updated
     * for every modification to the datastore.</p>
     */
    public long getSequence() {
        return sequence;
    }

    public void initialiseSequence(long sequence) {
        if (this.sequence == -1) {
            this.sequence = sequence;
        }
    }

    @Override
    public String toString() {
        return "{ id: " + this.id + ", rev: " + this.revision + ", seq: " + sequence + ", parent: " + parent + " }";
    }

    public int getGeneration() {
        if(generation < 0) {
            generation = CouchUtils.generationFromRevId(revision);
        }
        return generation;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that)
            return true;
        if (that == null)
            return false;
        if (getClass() != that.getClass())
            return false;
        BasicDocumentRevision other = (BasicDocumentRevision) that;
        if (id == null) {
            if (other.getId() != null)
                return false;
        } else if (!id.equals(other.getId()))
            return false;
        return true;
    }

    @Override
    public int compareTo(BasicDocumentRevision o) {
        return Long.valueOf(getSequence()).compareTo(o.getSequence());
    }

    /**
     * <p>Return a mutable copy of this <code>DocumentRevision</code>.</p>
     *
     * <p>The mutable copy can be used to change the body and attachments.</p>
     *
     * <p>Mutable copies can be created or updated in the datastore by calling the appropriate
     * methods on the <code>Datastore</code></p>.
     *
     * @return a mutable copy of the document revision
     * @see Datastore#createDocumentFromRevision(MutableDocumentRevision)
     * @see Datastore#updateDocumentFromRevision(MutableDocumentRevision)
     */
    public MutableDocumentRevision mutableCopy() {
        MutableDocumentRevision rev = new MutableDocumentRevision(this.getRevision());
        rev.docId = this.getId();
        rev.body = this.getBody();
        rev.attachments = this.attachments;
        return rev;
    }

    @Override
    public Map<String, Attachment> getAttachments() {
        return Collections.unmodifiableMap(attachments);
    }

}
