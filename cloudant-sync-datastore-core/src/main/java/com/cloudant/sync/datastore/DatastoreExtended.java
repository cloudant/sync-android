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
     * <p>
     * Inserts one or more revisions of a document into the database. For efficiency, this is
     * performed as one database transaction.
     * </p>
     * <p>
     * Each revision is inserted at a point in the tree expressed by the path described in the
     * {@code revisionHistory} field. If any non-leaf revisions do not exist locally, then they are
     * created as "stub" revisions.
     * </p>
     * <p>
     * This method should only be called by the replicator. It is designed
     * to allow revisions from remote databases to be added to this
     * database during the replication process: the documents in the remote database already have
     * revision IDs that need to be preserved for the two databases to be in sync (otherwise it
     * would not be possible to tell that the two represent the same revision). This is analogous to
     * using the _new_edits false option in CouchDB
     * (see <a href="https://wiki.apache.org/couchdb/HTTP_Bulk_Document_API#Posting_Existing_Revisions">
     * the CouchDB wiki</a> for more detail).
     * <p>
     * If the document was successfully inserted, a
     * {@link com.cloudant.sync.notifications.DocumentCreated DocumentCreated},
     * {@link com.cloudant.sync.notifications.DocumentModified DocumentModified}, or
     * {@link com.cloudant.sync.notifications.DocumentDeleted DocumentDeleted}
     * event is posted on the event bus. The event will depend on the nature
     * of the update made.
     * </p>
     *
     *
     * @param items one or more revisions to insert. Each {@code ForceInsertItem} consists of:
     * <ul>
     * <li>
     * <b>rev</b> A {@code DocumentRevision} containing the information for a revision
     * from a remote datastore.
     * </li>
     * <li>
     * <b>revisionHistory</b> The history of the revision being inserted,
     * including the rev ID of {@code rev}. This list
     * needs to be sorted in ascending order
     * </li>
     * <li>
     * <b>attachments</b> Attachments metadata and optionally data if {@code pullAttachmentsInline} true
     * </li>
     * <li>
     * <b>preparedAttachments</b> Non-empty if {@code pullAttachmentsInline} false.
     * Attachments that have already been prepared, this is a
     * Map of String[docId,revId] → list of attachments
     * </li>
     * <li>
     * <b>pullAttachmentsInline</b> If true, use {@code attachments} metadata and data directly
     * from received JSON to add new attachments for this revision.
     * Else use {@code preparedAttachments} which were previously
     * downloaded and prepared by processOneChangesBatch in
     * BasicPullStrategy
     * </li>
     * </ul>
     *
     * @see Datastore#getEventBus()
     * @throws DocumentException if there was an error inserting the revision or its attachments
     * into the database
     */
    void forceInsert(final List<ForceInsertItem> items) throws DocumentException;

    /**
     * This method has been deprecated and should not be used.
     * @see #forceInsert(List)
     */
    @Deprecated
    void forceInsert(DocumentRevision rev,
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
     * @see DatastoreExtended#forceInsert(DocumentRevision, java.util.List,java.util.Map, java.util.Map, boolean)
     * @throws DocumentException if there was an error inserting the revision into the database
     */
    void forceInsert(DocumentRevision rev, String... revisionHistory) throws
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
     * <p>
     * Read attachment stream to a temporary location and calculate sha1,
     * prior to being added to the datastore.
     * </p>
     * <p>
     * Used by replicator when receiving new/updated attachments
     *</p>
     *
     * @param att           Attachment to be prepared, providing data either from a file or a stream
     * @param length        Size in bytes of attachment as signalled by "length" metadata property
     * @param encodedLength Size in bytes of attachment, after encoding, as signalled by
     *                      "encoded_length" metadata property
     * @return A prepared attachment, ready to be added to the datastore
     * @throws AttachmentException if there was an error preparing the attachment, e.g., reading
     *                             attachment data.
     */
    PreparedAttachment prepareAttachment(Attachment att, long length, long encodedLength) throws AttachmentException;

    /**
     * <p>Returns attachment <code>attachmentName</code> for the revision.</p>
     *
     * <p>Used by replicator when pushing attachments</p>
     *
     * @param id The revision ID with which the attachment is associated
     * @param rev The document ID with which the attachment is associated
     * @param attachmentName Name of the attachment
     * @return <code>Attachment</code> or null if there is no attachment with that name.
     */
    Attachment getAttachment(String id, String rev, String attachmentName);

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
    List<? extends Attachment> attachmentsForRevision(DocumentRevision rev) throws
            AttachmentException;

}
