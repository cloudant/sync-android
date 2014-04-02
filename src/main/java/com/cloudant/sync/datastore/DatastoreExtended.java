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


import com.cloudant.mazha.DocumentRevs;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
     * <p>Adds a new local document with an ID and body.</p>
     *
     * <p>Local documents are not replicated between datastores.</p>
     *
     * @param documentId id for the document
     * @param body       JSON body for the document
     * @return {@code DocumentRevision} of the newly created document
     */
    public DocumentRevision createLocalDocument(String documentId, DocumentBody body);

    /**
     * <p>Adds a new local document with an auto-generated ID.</p>
     *
     * <p>The document's ID will be auto-generated, and can be found by
     * inspecting the returned {@code DocumentRevision}.</p>
     *
     * @param body JSON body for the document
     * @return {@code DocumentRevision} of the newly created document
     *
     * @see DatastoreExtended#createLocalDocument(String, DocumentBody)
     */
    public DocumentRevision createLocalDocument(DocumentBody body);

    /**
     * <p>Returns the current winning revision of a local document.</p>
     *
     * @param documentId id of the local document
     * @return {@code DocumentRevision} of the document
     */
    public DocumentRevision getLocalDocument(String documentId);

    /**
     * <p>Returns a given revision local document.</p>
     *
     * @param documentId id of the document
     * @param revisionId id of the revision
     * @return specified revision as DocumentRevision of a local document
     */
    public DocumentRevision getLocalDocument(String documentId, String revisionId);

    /**
     * <p>Updates a local document that exists in the datastore with a new
     * revision.</p>
     *
     * <p>The {@code prevRevisionId} must match the current winning revision
     * for the document.</p>
     *
     * @param documentId ID of the document
     * @param prevRevisionId revision id of the document's current winning
     *                       revision
     * @param body body of the new revision
     * @return @{code DocumentRevision} for the updated revision
     */
    public DocumentRevision updateLocalDocument(String documentId, String prevRevisionId, DocumentBody body);

    /**
     * <p>Deletes a local document.</p>
     *
     * @param documentId documentId of the document to be deleted
     *
     * @throws DocumentNotFoundException if the document ID doesn't exist
     */
    public void deleteLocalDocument(String documentId);

    /**
     * <p>Returns {@code DocumentRevisionTree} of a document.</p>
     *
     * <p>The tree contains the complete revision history of the document,
     * including branches for conflicts and deleted leaves.</p>
     *
     * @param documentId  id of the document
     * @return {@code DocumentRevisionTree} of the specified document
     */
    public DocumentRevisionTree getAllRevisionsOfDocument(String documentId);

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
     *                        needs to be sorted in ascending order.
     *
     * @see Datastore#getEventBus() 
     */
    public void forceInsert(DocumentRevision rev, List<String> revisionHistory, Map<String, Object> attachments);

    /**
     * <p>Inserts a revision of a document with an existing revision ID</p>
     *
     * <p>Equivalent to:</p>
     *
     * <code>
     *    forceInsert(rev, Arrays.asList(revisionHistory));
     * </code>
     *
     * @param rev
     * @param revisionHistory
     *
     * @see DatastoreExtended#forceInsert(DocumentRevision, java.util.List)
     */
    public void forceInsert(DocumentRevision rev, String... revisionHistory);

    /**
     * <p>Returns a handle to the SQLite database used for low-level
     * storage.</p>
     *
     * <p>This should rarely be called by developers.</p>
     *
     * @return Handle the {@code Datastore} is using to access the underlying
     *          SQLite database file.
     */
    public SQLDatabase getSQLDatabase();

    /**
     * <p>Returns the datastore's unique identifier.</p>
     *
     * <p>This is used for the checkpoint document in a remote datastore
     * during replication.</p>
     *
     * @return a unique identifier for the datastore.
     */
    public String getPublicIdentifier();

    /**
     * <p>Returns the number of documents in the database, including deleted
     * documents.</p>
     *
     * @return document count, including deleted docs.
     */
    public int getDocumentCount();

    /**
     * <p>Returns the SQLite primary key of a document.</p>
     *
     * <p>Internally, each document has a row in a SQLite database table.
     * This method gets the primary key of that row. It's meaningless
     * outside of the local datastore, and dependent on the insertion
     * order of documents.</p>
     *
     * @param documentId id of the specified document
     * @return internal id of the given document
     */
    public long getDocNumericId(String documentId);
    /**
     * Returns the subset of given the documentId/revisions that are not stored in the database.
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
     */
    public Map<String, Collection<String>> revsDiff(Multimap<String, String> revisions);
}
