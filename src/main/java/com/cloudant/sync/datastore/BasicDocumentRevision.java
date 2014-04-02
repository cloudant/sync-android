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

import java.util.Map;

/**
 *  Revision of a document.
 *
 */
class BasicDocumentRevision implements DocumentRevision {

    private final DocumentBody body;

    private long sequence = - 1;
    private long internalNumericId;

    private boolean deleted;
    private boolean current;
    private long parent = -1L;

    private int generation = -1;

    BasicDocumentRevision(String docId, String revId, DocumentBody body, long sequence, long docInternalId, boolean deleted, boolean current, long parent) {
        CouchUtils.validateRevisionId(revId);
        CouchUtils.validateDocumentId(docId);
        assert body != null;

        this.id = docId;
        this.revision = revId;
        this.deleted = deleted;
        this.body = body;
        this.sequence = sequence;
        this.current = current;
        this.internalNumericId = docInternalId;
        this.parent = parent;
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

    @Override
    public boolean isCurrent(){
        return current;
    }

    @Override
    public long getParent() {
        return this.parent;
    }

    public long getInternalNumericId(){
        return internalNumericId;
    }

    Map<String, Object> map = null;
    @Override
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

    @Override
    public byte[] asBytes() {
        byte[] result = null;
        if(body != null) {
            result = body.asBytes();
        }
        return result;
    }

    @Override
    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
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
        DocumentRevision other = (DocumentRevision) that;
        if (id == null) {
            if (other.getId() != null)
                return false;
        } else if (!id.equals(other.getId()))
            return false;
        return true;
    }

    @Override
    public int compareTo(DocumentRevision o) {
        return Long.valueOf(getSequence()).compareTo(o.getSequence());
    }
}
