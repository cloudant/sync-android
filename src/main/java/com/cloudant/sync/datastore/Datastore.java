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

import com.google.common.eventbus.EventBus;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * <p>The Datastore is the core interaction point for create, delete and update
 * operations (CRUD) for within Cloudant Sync.</p>
 *
 * <p>The Datastore can be viewed as a pool of heterogeneous JSON documents. One
 * datastore can hold many different types of document, unlike tables within a
 * relational model. The datastore provides hooks, particularly in the
 * {@link DatastoreExtended} interface, which allow for various querying models
 * to be built on top of its simpler key-value model.</p>
 *
 * <p>Each document consists of a set of revisions, hence most methods within
 * this class operating on {@link DocumentRevision} objects, which carry both a
 * document ID and a revision ID. This forms the basis of the MVCC data model,
 * used to ensure safe peer-to-peer replication is possible.</p>
 *
 * <p>Each document is formed of a tree of revisions. Replication can create
 * branches in this tree when changes have been made in two or more places to
 * the same document in-between replications. MVCC exposes these branches as
 * conflicted documents. These conflicts should be resolved by user-code, by
 * marking all but one of the leaf nodes of the branches as "deleted", using
 * the {@link Datastore#deleteDocument(String, String)} method. When the
 * datastore is next replicated with a remote datastore, this fix will be
 * propagated, thereby resolving the conflicted document across the set of
 * peers.</p>
 *
 * <p><strong>WARNING:</strong> conflict resolution is coming in the next
 * release, where we'll be adding methods to:</p>
 *
 * <ul>
 *     <li>Get the IDs of all conflicted documents within the datastore.</li>
 *     <li>Get a list of all current revisions for a given document, so they
 *     can be merged to resolve the conflict.</li>
 * </ul>
 *
 * @see DatastoreExtended
 * @see DocumentRevision
 *
 */
public interface Datastore {

    /**
     * The sequence number of the datastore when no updates have been made,
     * {@value}.
     */
    long SEQUENCE_NUMBER_START = -1l;

    /**
     * <p>Returns the name of this datastore.</p>
     *
     * @return the name of this datastore
     */
    public String getDatastoreName();

    /**
     * <p>Adds a new document with an ID and body.</p>
     *
     * <p>If the document was successfully created, a 
     * {@link com.cloudant.sync.notifications.DocumentCreated DocumentCreated} 
     * event is posted on the event bus.</p>
     *
     * @param documentId id for the document
     * @param body       JSON body for the document
     * @return {@code DocumentRevision} of the newly created document
     *
     * @see Datastore#getEventBus()
     */
    public DocumentRevision createDocument(String documentId, final DocumentBody body);

    /**
     * <p>Adds a new document with an auto-generated ID.</p>
     *
     * <p>The document's ID will be auto-generated, and can be found by
     * inspecting the returned {@code DocumentRevision}.</p>
     *
     * <p>If the document was successfully created, a 
     * {@link com.cloudant.sync.notifications.DocumentCreated DocumentCreated} 
     * event is posted on the event bus.</p>
     *
     * @param body JSON body for the document
     * @return {@code DocumentRevision} of the newly created document
     *
     * @see Datastore#getEventBus()
     */
    public DocumentRevision createDocument(final DocumentBody body);

    /**
     * <p>Returns the current winning revision of a document.</p>
     *
     * <p>Previously deleted documents can be retrieved
     * (via tombstones, see {@link Datastore#deleteDocument(String, String)})
     * </p>
     *
     * @param documentId ID of document to retrieve.
     * @return {@code DocumentRevision} of the document or null if it doesn't exist.
     */
    public DocumentRevision getDocument(String documentId);

    /**
     * <p>Retrieves a given revision of a document.</p>
     *
     * <p>This method gets the revision of a document with a given ID. As the
     * datastore prunes the content of old revisions to conserve space, this
     * revision may contain the metadata but not content of the revision.</p>
     *
     * <p>Previously deleted documents can be retrieved
     * (via tombstones, see {@link Datastore#deleteDocument(String, String)})
     * </p>
     *
     * @param documentId ID of the document
     * @param revisionId Revision of the document
     * @return {@code DocumentRevision} of the document or null if it doesn't exist.
     */
    public DocumentRevision getDocument(String documentId, String revisionId);

    /**
     * <p>Returns whether this datastore contains a particular revision of
     * a document.</p>
     *
     * <p>{@code true} will still be returned if the document is deleted.</p>
     *
     * @param documentId id of the document
     * @param revisionId revision of the document
     * @return {@code true} if specified document's particular revision exists
     *         in the datastore, {@code false} otherwise.
     */
    public boolean containsDocument(String documentId, String revisionId);

    /**
     * <p>Returns whether this datastore contains any revisions of a document.
     * </p>
     *
     * <p>{@code true} will still be returned if the document is deleted.</p>
     *
     * @param documentId id of the document
     * @return {@code true} if specified document exists
     *         in the datastore, {@code false} otherwise.
     */
    public boolean containsDocument(String documentId);

    /**
     * <p>Enumerates the current winning revision for all documents in the
     * datastore.</p>
     *
     * <p>Logically, this method takes all the documents in either ascending
     * or descending order, skips all documents up to {@code offset} then
     * returns up to {@code limit} document revisions, stopping either
     * at {@code limit} or when the list of document is exhausted.</p>
     *
     * @param offset start position
     * @param limit maximum number of documents to return
     * @param descending whether the documents are read in ascending or
     *                   descending order.
     * @return list of {@code DBObjects}, maximum length {@code limit}.
     */
    public List<DocumentRevision> getAllDocuments(int offset, int limit, boolean descending);

    /**
     * <p>Returns the current winning revisions for a set of documents.</p>
     *
     * <p>If the {@code documentIds} list contains document IDs not present
     * in the datastore, they will be skipped and there will be no entry for
     * them in the returned list.</p>
     *
     * @param documentIds list of document id
     * @return list of {@code DocumentRevision} objects.
     */
    public List<DocumentRevision> getDocumentsWithIds(List<String> documentIds);

    /**
     * <p>Updates a document that exists in the datastore with a new revision.
     * </p>
     *
     * <p>The {@code prevRevisionId} must match the current winning revision
     * for the document.</p>
     *
     * <p>If the document was successfully updated, a 
     * {@link com.cloudant.sync.notifications.DocumentUpdated DocumentUpdated} 
     * event is posted on the event bus.</p>
     *
     * @param documentId ID of the document
     * @param prevRevisionId revision id of the document's current winning
     *                       revision
     * @param body body of the new revision
     * @return @{code DocumentRevision} for the updated revision
     *
     * @throws ConflictException if the {@code prevRevisionId} is incorrect.
     *
     * @see Datastore#getEventBus()
     */
    public DocumentRevision updateDocument(String documentId, String prevRevisionId, final DocumentBody body) throws ConflictException;

    /**
     * <p>Deletes a document from the datastore.</p>
     *
     * <p>This operation leaves a "tombstone" for the deleted document, so that
     * future replication operations can successfully replicate the deletion.
     * </p>
     *
     * <p>If the document was successfully deleted, a 
     * {@link com.cloudant.sync.notifications.DocumentDeleted DocumentDeleted} 
     * event is posted on the event bus.</p>
     *
     * <p>If the input revision is already deleted, nothing will be changed. </p>
     *
     * <p>This operation also allows to delete any non-deleted leaf revision of
     * the given document. It adds a "deleted" revision of the branch specified
     * the given revision. This is mainly useful for resolving conflicts.</p>
     *
     * @param documentId ID of the document to delete.
     * @param revisionId revision id of the document's current winning revision
     * @throws ConflictException if the revisionId is not the current revision
     *
     * @see Datastore#getEventBus()
     */
    public void deleteDocument(String documentId, String revisionId) throws ConflictException;

    /**
     * <p>Retrieves the datastore's current sequence number.</p>
     *
     * <p>The datastore's sequence number is incremented every time the
     * content of the datastore is changed. Each document revision within the
     * datastore has an associated sequence number, describing when the change
     * took place.</p>
     *
     * <p>The sequence number is particularly useful to find out the changes
     * to the database since a given time. For example, replication uses the
     * sequence number so only the changes since the last replication are
     * sent. Indexing could also use this in a similar manner.</p>
     *
     * @return the last sequence number
     */
    public long getLastSequence();

    /**
     * <p>Return the number of documents in the datastore</p>
     *
     * @return number of non-deleted documents in datastore
     */
    public int getDocumentCount();

    /**
     * <p>Returns a list of changed documents, from {@code since} to
     * {@code since + limit}, inclusive.</p>
     *
     * @param since the lower bound (exclusive) of the change set
     *              sequence number
     * @param limit {@code since + limit} is the upper bound (inclusive) of the
     *              change set sequence number
     * @return list of the documents and last sequence number of the change set
     *      (checkpoint)
     */
    public Changes changes(long since, int limit);

    /**
     * <p>Returns the EventBus which this Datastore posts
     * {@link com.cloudant.sync.notifications.DocumentModified Document Notification Events} to.</p>
     * @return the Datastore's EventBus
     *
     * @see <a href="https://code.google.com/p/guava-libraries/wiki/EventBusExplained">Google Guava EventBus documentation</a>
     */
    public EventBus getEventBus();

    /**
     * <p>Return the directory for specified extensionName</p>
     *
     * @param extensionName name of the extension
     *
     * @return the directory for specified extensionName
     */
    public String extensionDataFolder(String extensionName);

    /**
     * <p>Return {@code @Iterable<String>} over ids to all the Documents with
     * conflicted revisions.</p>
     *
     * <p>Document is modeled as a tree. If a document has at least two leaf
     * revisions that not deleted, those leaf revisions considered
     * conflicted revisions of the document.
     * </p>
     *
     * @return Iterable of String over ids of all Documents with
     *         conflicted revisions
     *
     * @see <a href="http://wiki.apache.org/couchdb/Replication_and_conflicts">Replication and conflicts</a>
     */
    public Iterator<String> getConflictedDocumentIds();

    /**
     * <p>
     * Resolve conflicts for specified Document using the
     * given {@code ConflictResolver}
     * </p>
     *
     * @param docId id of Document to resolve conflicts
     * @param resolver the ConflictResolver used to resolve
     *                 conflicts
     * @throws ConflictException Found new conflicts while
     *         resolving the existing conflicted revision.
     *         This is very likely caused by new conflicted
     *         revision are added while the resolver is
     *         running.
     *
     * @see com.cloudant.sync.datastore.ConflictResolver
     */
    public void resolveConflictsForDocument(String docId, ConflictResolver resolver)
        throws ConflictException;

    /**
     * Returns attachment `attachmentName` for the revision.
     *
     * @return SavedAttachment or null no attachment with that name.
     */
    Attachment getAttachment(DocumentRevision rev, String attachmentName);

    /**
     * Returns all attachments revision, creating a new revision.
     *
     * @return SavedAttachment or null no attachment with that name.
     */
    List<? extends Attachment> attachmentsForRevision(DocumentRevision rev);

    /**
     Set the content of attachments on a document, creating
     new revision of the document.

     Existing attachments with the same name will be replaced,
     new attachments will be created, and attachments already
     existing on the document which are not included in
     `attachments` will remain as attachments on the document.

     @return New revision.
     */
    DocumentRevision updateAttachments(DocumentRevision rev, List<? extends Attachment> attachments) throws ConflictException, IOException;

    /**
     Remove attachment `name` from a document, creating a new revision.
     @return New revision.
     */
    DocumentRevision removeAttachments(DocumentRevision rev, String[] attachmentNames) throws ConflictException;

    /**
     * Close the datastore
     */
    public void close();
}

