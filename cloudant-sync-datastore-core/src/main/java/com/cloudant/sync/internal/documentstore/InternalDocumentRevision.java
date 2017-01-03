/*
 * Copyright © 2016 IBM Corp. All rights reserved.
 *
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

package com.cloudant.sync.internal.documentstore;

import com.cloudant.sync.documentstore.Attachment;
import com.cloudant.sync.documentstore.DocumentBody;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.internal.common.ChangeNotifyingMap;
import com.cloudant.sync.internal.common.CouchUtils;
import com.cloudant.sync.internal.common.SimpleChangeNotifyingMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalDocumentRevision extends DocumentRevision implements
        Comparable<InternalDocumentRevision> {

    private long sequence = -1L;

    private long internalNumericId;

    private long parent = -1L;

    private boolean bodyModified = false;

    public InternalDocumentRevision(String docId, String revId, DocumentBody body,
                                    DocumentRevisionBuilder.DocumentRevisionOptions options) {
        super(docId, revId, body);

        if (options != null) {
            this.deleted = options.deleted;
            this.sequence = options.sequence;
            this.current = options.current;
            this.internalNumericId = options.docInternalId;
            this.parent = options.parent;
            this.setAttachmentsInternal(options.attachments);
        } else {
            this.current = true;
            this.deleted = false;
            this.attachments = SimpleChangeNotifyingMap.wrap(new HashMap<String, Attachment>());
        }
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
     * <p>Returns the internal numeric ID of this document revision.</p>
     *
     * <p>This can be useful for efficient storage by plugins extending the
     * datastore.</p>
     *
     * @return the internal numeric ID of this document revision.
     */
    public long getInternalNumericId() {
        return internalNumericId;
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

    public void setRevision(String revision) {
        this.revision = revision;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    /*
     * Helper used by sub-classes to convert between list and map representation of attachments
     */
    protected void setAttachmentsInternal(List<? extends Attachment> attachments) {
        if (attachments != null) {
            // this awkward looking way of doing things is to avoid marking the map as being
            // modified
            HashMap<String, Attachment> m = new HashMap<String, Attachment>();
            for (Attachment att : attachments) {
                m.put(att.name, att);
            }
            this.attachments = SimpleChangeNotifyingMap.wrap(m);
        }
    }

    @Override
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
    public Map<String, Object> asMap() {
        Map<String, Object> map = getBody().asMap();
        // add meta properties: ID, rev, deleted
        map.put("_id", id);
        if (revision != null) {
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
        if (getBody() != null) {
            result = getBody().asBytes();
        }
        return result;
    }

    @Override
    public void setBody(DocumentBody body) {
        super.setBody(body);
        this.bodyModified = true;
    }

    /**
     * @return Whether the body has been modified since this DocumentRevision was constructed or
     * retrieved from the Datastore.
     */
    public boolean isBodyModified() {
        return bodyModified;
    }

    public int getGeneration() {
        return CouchUtils.generationFromRevId(revision);
    }

    @Override
    public ChangeNotifyingMap<String, Attachment> getAttachments() {
        return (ChangeNotifyingMap<String, Attachment>) attachments;
    }

    @Override
    public String toString() {
        return "{ id: " + this.id + ", rev: " + this.revision + ", seq: " + sequence + ", parent:" +
                " " + parent + ", current: " + current + ", deleted " + deleted + " }";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InternalDocumentRevision that = (InternalDocumentRevision) o;

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
    public int compareTo(InternalDocumentRevision o) {
        return Long.valueOf(getSequence()).compareTo(o.getSequence());
    }


}
