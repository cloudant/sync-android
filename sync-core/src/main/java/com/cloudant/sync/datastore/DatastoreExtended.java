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


import com.google.common.collect.Multimap;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * <p>{@code DatastoreExtended} adds further, lesser-used methods to the
 * Datastore interface.</p>
 *
 * <p>These methods are typically used by other packages extending the
 * datastore's functionality. For example, the replication package uses these
 * extended APIs to implement replication between local and remote
 * CouchDB-replication-protocol compatible datastores.</p>
 *
 */
public interface DatastoreExtended extends Datastore {

    /**
     * <p>Inserts a local document with an ID and body. Replacing the current local document of the
     * same id if one is present. </p>
     *
     * <p>Local documents are not replicated between datastores.</p>
     *
     * @param docId      The document id for the document
     * @param body       JSON body for the document
     * @return {@code DocumentRevision} of the newly created document
     * @throws DocumentException if there is an error inserting the local document into the database
     */
    LocalDocument insertLocalDocument(String docId, DocumentBody body) throws DocumentException;

    /**
     * <p>Returns the current winning revision of a local document.</p>
     *
     * @param documentId id of the local document
     * @return {@code LocalDocument} of the document
     * @throws DocumentNotFoundException if the document ID doesn't exist
     */
    LocalDocument getLocalDocument(String documentId) throws DocumentNotFoundException;

    /**
     * <p>Deletes a local document.</p>
     *
     * @param documentId documentId of the document to be deleted
     *
     * @throws DocumentNotFoundException if the document ID doesn't exist
     */
    void deleteLocalDocument(String documentId) throws DocumentNotFoundException;

    /**
     * <p>Returns {@code DocumentRevisionTree} of a document.</p>
     *
     * <p>The tree contains the complete revision history of the document,
     * including branches for conflicts and deleted leaves.</p>
     *
     * @param documentId  id of the document
     * @return {@code DocumentRevisionTree} of the specified document
     */
    DocumentRevisionTree getAllRevisionsOfDocument(String documentId);

    /**
     * <p>Inserts a revision of a document with an existing revision ID and
     * revision history.</p>
     *
     * <p>This method inserts a revision into the document tree for an existing
     * document. It checks every revision in the revision history, and adds
     * {@code rev} is it isn't already present in the datastore.</p>
     *
     * <p>Here is an example of revHistory:</p>
     *
     * <pre>
     * [
     *     "1-421ff3d58df47ea6c5e83ca65efb2fa9",
     *     "2-74e0572530e3b4cd4776616d2f591a96",
     *     "3-d8e1fb8127d8dd732d9ae46a6c38ae3c",
     *     "4-47d7102726fc89914431cb217ab7bace"
     * ]
     * </pre>
     *
     * <p>This method should only be called by the replicator. It's designed
     * to allow revisions from different datastores to be added to this
     * datastore during the replication process, when we wouldn't want to
     * create new revision IDs (as if we did, we wouldn't know that we already
     * had a particular revision from a previous replication).</p>
     *
     * <p>The {@code revisionHistory} is required so that we know whether
     * the {@code rev} is part of the existing document tree, so we can see
     * whether it's a conflict which needs to be grafted to the tree or
     * whether it's a newer version of the same branch we already have.</p>
     *
     * <p>If the document was successfully inserted, a
     * {@link com.cloudant.sync.notifications.DocumentCreated DocumentCreated},
     * {@link com.cloudant.sync.notifications.DocumentModified DocumentModified}, or
     * {@link com.cloudant.sync.notifications.DocumentDeleted DocumentDeleted}
     * event is posted on the event bus. The event will depend on the nature
     * of the update made.</p>
     *
     * @param rev A {@code DocumentRevision} containing the information for a revision
     *            from a remote datastore.
     * @param revisionHistory The history of the revision being inserted,
     *                        including the rev ID of {@code rev}. This list
     *                        needs to be sorted in ascending order
     * @param attachments Attachments metadata and optionally data if {@code pullAttachmentsInline} true
     * @param preparedAttachments Non-empty if {@code pullAttachmentsInline} false.
     *                            Attachments that have already been prepared, this is a
     *                            Map of String[docId,revId] → list of attachments
     * @param pullAttachmentsInline If true, use {@code attachments} metadata and data directly
     *                              from received JSON to add new attachments for this revision.
     *                              Else use {@code preparedAttachments} which were previously
     *                              downloaded and prepared by processOneChangesBatch in
     *                              BasicPullStrategy
     *
     * @see Datastore#getEventBus()
     * @throws DocumentException if there was an error inserting the revision or its attachments
     * into the database
     */
    void forceInsert(BasicDocumentRevision rev,
                            List<String> revisionHistory,
                            Map<String, Object> attachments,
                            Map<String[],List<PreparedAttachment>> preparedAttachments,
                            boolean pullAttachmentsInline) throws DocumentException;

    /**
     * <p>Inserts a revision of a document with an existing revision ID</p>
     *
     * <p>Equivalent to:</p>
     *
     * <code>
     *    forceInsert(rev, Arrays.asList(revisionHistory), null, null, false);
     * </code>
     *
     * @param rev A {@code DocumentRevision} containing the information for a revision
     *            from a remote datastore.
     * @param revisionHistory The history of the revision being inserted,
     *                        including the rev ID of {@code rev}. This list
     *                        needs to be sorted in ascending order
     *
     * @see DatastoreExtended#forceInsert(BasicDocumentRevision, java.util.List,java.util.Map, java.util.Map, boolean)
     * @throws DocumentException if there was an error inserting the revision into the database
     */
    void forceInsert(BasicDocumentRevision rev, String... revisionHistory) throws
            DocumentException;

    /**
     * <p>Returns the datastore's unique identifier.</p>
     *
     * <p>This is used for the checkpoint document in a remote datastore
     * during replication.</p>
     *
     * @return a unique identifier for the datastore.
     * @throws DatastoreException if there was an error retrieving the unique identifier from the
     * database
     */
    String getPublicIdentifier() throws DatastoreException;

    /**
     * <p>Returns the number of documents in the database, including deleted
     * documents.</p>
     *
     * @return document count, including deleted docs.
     */
    int getDocumentCount();

    /**
     * Returns the subset of given the document id/revisions that are not stored in the database.
     *
     * The input revisions is a map, whose key is document id, and value is a list of revisions.
     * An example input could be (in json format):
     *
     * { "03ee06461a12f3c288bb865b22000170":
     *     [
     *       "1-b2e54331db828310f3c772d6e042ac9c",
     *       "2-3a24009a9525bde9e4bfa8a99046b00d"
     *     ],
     *   "82e04f650661c9bdb88c57e044000a4b":
     *     [
     *       "3-bb39f8c740c6ffb8614c7031b46ac162"
     *     ]
     * }
     *
     * The output is in same format.
     *
     * @see <a href="http://wiki.apache.org/couchdb/HttpPostRevsDiff">HttpPostRevsDiff documentation</a>
     * @param revisions a Multimap of document id → revision id
     * @return a Map of document id → collection of revision id: the subset of given the document
     * id/revisions that are not stored in the database
     */
    Map<String, Collection<String>> revsDiff(Multimap<String, String> revisions);

    /**
     * Read attachment stream to a temporary location and calculate sha1,
     * prior to being added to the datastore.
     *
     * Used by replicator when receiving new/updated attachments
     *
     * @param att Attachment to be prepared, providing data either from a file or a stream
     * @return A prepared attachment, ready to be added to the datastore
     * @throws AttachmentException if there was an error preparing the attachment, e.g., reading
     *                  attachment data.
     */
    PreparedAttachment prepareAttachment(Attachment att) throws AttachmentException;

    /**
     * Add attachment to document revision without incrementing revision.
     *
     * Used by replicator when receiving new/updated attachments
     *
     * @param att The attachment to add
     * @param rev The DocumentRevision to add the attachment to
     * @throws AttachmentException if there was an error inserting the attachment metadata into the
     * database or if there was an error moving the attachment file on the file system
     */
    void addAttachment(PreparedAttachment att, BasicDocumentRevision rev) throws  AttachmentException;

    /**
     * <p>Returns attachment <code>attachmentName</code> for the revision.</p>
     *
     * <p>Used by replicator when pushing attachments</p>
     *
     * @param rev The revision with which the attachment is associated
     * @param attachmentName Name of the attachment
     * @return <code>Attachment</code> or null if there is no attachment with that name.
     */
    Attachment getAttachment(BasicDocumentRevision rev, String attachmentName);

    /**
     * <p>Returns all attachments for the revision.</p>
     *
     * <p>Used by replicator when pulling attachments</p>
     *
     * @param rev The revision with which the attachments are associated
     * @return List of <code>Attachment</code>
     * @throws AttachmentException if there was an error reading the attachment metadata from the
     * database
     */
    List<? extends Attachment> attachmentsForRevision(BasicDocumentRevision rev) throws
            AttachmentException;

}
