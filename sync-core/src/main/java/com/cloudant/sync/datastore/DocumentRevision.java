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
public interface DocumentRevision {

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

    // NB the key is purely for the user's convenience and doesn't have to be the same as the attachment name
    public Map<String, Attachment> getAttachments();

}
