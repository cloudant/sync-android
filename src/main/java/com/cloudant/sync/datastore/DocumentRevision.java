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
 */
public interface DocumentRevision extends Comparable<DocumentRevision> {

    /**
     * <p>Returns the unique identifier of the document.</p>
     */
    public String getId();

    /**
     * <p>Returns the revision ID of this document revision.</p>
     */
    public String getRevision();

    /**
     * <p>Returns the {@code DocumentBody} of the document.</p>
     */
    public DocumentBody getBody();


    /**
     * <p>Returns the sequence number of this revision.</p>
     *
     * <p>The sequence number is unique across the database, it is updated
     * for every modification to the datastore.</p>
     */
    public long getSequence();

    public void setSequence(long sequence);

    /**
     * <p>Returns the sequence number of this revision's parent revision.</p>
     *
     * <p>If this number is less than or equal to zero, it means this
     * revision is the root of a document tree. A document may have more than
     * one tree, under certain circumstances, such as two documents with the
     * same ID being created in different datastores that are later replicated
     * across.</p>
     */
    public long getParent();

    /**
     * <p>Returns {@code true} if this revision is marked deleted.</p>
     */
    public boolean isDeleted();

    /**
     * <p>Returns {@code true} if this revision is the current winner for the
     * document.</p>
     */
    public boolean isCurrent();

    /**
     * <p>Returns the JSON body of the document revision as a {@code byte}
     * array.</p>
     *
     * <p>This byte array includes reserved fields such as {@code _id} and
     * {@code _rev}. Changing the byte array does not affect the document
     * revisions contents.</p>
     */
    public byte[] asBytes();

    /**
     * <p>Returns the JSON body of the document revision as a {@code Map}
     * object.</p>
     *
     * <p>This Map includes reserved fields such as {@code _id} and
     * {@code _rev}. Changing the byte array may affect the {@code DocumentRevision},
     * as only a shallow copy is returned.</p>
     */
    public Map<String, Object> asMap();

    /**
     * <p>Returns the internal numeric ID of this document revision.</p>
     *
     * <p>This can be useful for efficient storage by plugins extending the
     * datastore.</p>
     *
     * @return  the internal numeric ID of this document revision.
     */
    public long getInternalNumericId();

}
