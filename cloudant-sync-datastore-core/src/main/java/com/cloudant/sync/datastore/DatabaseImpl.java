/**
 * Copyright © 2016 IBM Corp. All rights reserved.
 *
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

import com.cloudant.common.ChangeNotifyingMap;
import com.cloudant.common.ValueListMap;
import com.cloudant.sync.datastore.callables.ChangesCallable;
import com.cloudant.sync.datastore.callables.CompactCallable;
import com.cloudant.sync.datastore.callables.DeleteAllRevisionsCallable;
import com.cloudant.sync.datastore.callables.DeleteDocumentCallable;
import com.cloudant.sync.datastore.callables.DeleteLocalDocumentCallable;
import com.cloudant.sync.datastore.callables.ForceInsertCallable;
import com.cloudant.sync.datastore.callables.GetAllDocumentIdsCallable;
import com.cloudant.sync.datastore.callables.GetAllDocumentsCallable;
import com.cloudant.sync.datastore.callables.GetAllRevisionsOfDocumentCallable;
import com.cloudant.sync.datastore.callables.GetConflictedDocumentIdsCallable;
import com.cloudant.sync.datastore.callables.GetDocumentCallable;
import com.cloudant.sync.datastore.callables.GetDocumentCountCallable;
import com.cloudant.sync.datastore.callables.GetDocumentsWithIdsCallable;
import com.cloudant.sync.datastore.callables.GetDocumentsWithInternalIdsCallable;
import com.cloudant.sync.datastore.callables.GetLastSequenceCallable;
import com.cloudant.sync.datastore.callables.GetLocalDocumentCallable;
import com.cloudant.sync.datastore.callables.GetPossibleAncestorRevisionIdsCallable;
import com.cloudant.sync.datastore.callables.GetSequenceCallable;
import com.cloudant.sync.datastore.callables.InsertDocumentIDCallable;
import com.cloudant.sync.datastore.callables.InsertLocalDocumentCallable;
import com.cloudant.sync.datastore.callables.InsertRevisionCallable;
import com.cloudant.sync.datastore.callables.ResolveConflictsForDocumentCallable;
import com.cloudant.sync.datastore.callables.RevsDiffBatchCallable;
import com.cloudant.sync.datastore.callables.SetCurrentCallable;
import com.cloudant.sync.datastore.callables.UpdateDocumentFromRevisionCallable;
import com.cloudant.sync.datastore.encryption.KeyProvider;
import com.cloudant.sync.datastore.encryption.NullKeyProvider;
import com.cloudant.sync.datastore.migrations.MigrateDatabase100To200;
import com.cloudant.sync.datastore.migrations.MigrateDatabase6To100;
import com.cloudant.sync.datastore.migrations.SchemaOnlyMigration;
import com.cloudant.sync.event.EventBus;
import com.cloudant.sync.notifications.DatabaseClosed;
import com.cloudant.sync.notifications.DocumentCreated;
import com.cloudant.sync.notifications.DocumentDeleted;
import com.cloudant.sync.notifications.DocumentModified;
import com.cloudant.sync.notifications.DocumentUpdated;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLDatabaseQueue;
import com.cloudant.sync.util.CollectionUtils;
import com.cloudant.sync.util.CouchUtils;
import com.cloudant.sync.util.DatabaseUtils;
import com.cloudant.sync.util.JSONUtils;
import com.cloudant.sync.util.Misc;

import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @api_private
 */
public class DatabaseImpl implements Database {

    private static final Logger logger = Logger.getLogger(DatabaseImpl.class.getCanonicalName());

    // for building up the strings below
    public static final String METADATA_COLS = "docs.docid, docs.doc_id, revid, sequence, " +
            "current, deleted, parent";

    public static final String FULL_DOCUMENT_COLS = METADATA_COLS + ", json";

    public static final String CURRENT_REVISION_CLAUSES =
            "FROM revs, docs WHERE docs.docid=? AND revs.doc_id=docs.doc_id AND current=1 ORDER " +
                    "BY revid DESC LIMIT 1";

    private static final String GIVEN_REVISION_CLAUSES =
            "FROM revs, docs WHERE docs.docid=? AND revs.doc_id=docs.doc_id AND revid=? ORDER BY " +
                    "revs.sequence LIMIT 1";

    // get all document columns for current ("winning") revision of a given doc id
    public static final String GET_DOCUMENT_CURRENT_REVISION =
            "SELECT " + FULL_DOCUMENT_COLS + " " + CURRENT_REVISION_CLAUSES;

    // get metadata (everything except json) for current ("winning") revision of a given doc id
    public static final String GET_METADATA_CURRENT_REVISION =
            "SELECT " + METADATA_COLS + " " + CURRENT_REVISION_CLAUSES;

    // get all document columns for a given revision and doc id† (see below)
    public static final String GET_DOCUMENT_GIVEN_REVISION =
            "SELECT " + FULL_DOCUMENT_COLS + " " + GIVEN_REVISION_CLAUSES;

    // get metadata (everything except json) for a given revision and doc id† (see below)
    public static final String GET_METADATA_GIVEN_REVISION =
            "SELECT " + METADATA_COLS + " " + GIVEN_REVISION_CLAUSES;

    public static final String GET_DOC_NUMERIC_ID = "SELECT doc_id from docs WHERE docid=?";

    public static final String SQL_CHANGE_IDS_SINCE_LIMIT = "SELECT doc_id, max(sequence) FROM " +
            "revs WHERE sequence > ? AND sequence <= ? GROUP BY doc_id ";

    // † N.B. whilst there should only ever be a single result bugs have resulted in duplicate
    // revision IDs in the tree. Whilst it appears that the lowest sequence number is always
    // returned by these queries we use ORDER BY sequence to guarantee that and lock down a
    // behaviour for any future occurrences of duplicate revs in a tree.

    // Limit of parameters (placeholders) one query can have.
    // SQLite has limit on the number of placeholders on a single query, default 999.
    // http://www.sqlite.org/limits.html
    public static final int SQLITE_QUERY_PLACEHOLDERS_LIMIT = 500;

    private final EventBus eventBus;

    final File datastoreDir;
    final File extensionsDir;

    private static final String DB_FILE_NAME = "db.sync";

    /**
     * Stores a reference to the encryption key provider so
     * it can be passed to extensions.
     */
    private final KeyProvider keyProvider;

    /**
     * Queue for all database tasks.
     */
    private final SQLDatabaseQueue queue;

    /** Name used to get storage folder for attachments */
    private static final String ATTACHMENTS_EXTENSION_NAME = "com.cloudant.attachments";

    /** Directory where attachments are stored for this datastore */
    private final String attachmentsDir;

    /**
     * Creates streams used for encrypting and encoding (gzip etc.) attachments when
     * reading to and from disk.
     */
    private final AttachmentStreamFactory attachmentStreamFactory;

    /**
     * Constructor for single thread SQLCipher-based datastore.
     * @param dir The directory where the datastore will be created
     * @param provider The key provider object that contains the user-defined SQLCipher key
     * @throws SQLException
     * @throws IOException
     */
    DatabaseImpl(File dir, KeyProvider provider) throws SQLException,
            IOException, DatastoreException {
        Misc.checkNotNull(dir, "Directory");
        Misc.checkNotNull(provider, "Key provider");

        this.keyProvider = provider;
        this.datastoreDir = dir;
        this.extensionsDir = new File(dir, "extensions");
        final String dbFilename = new File(this.datastoreDir, DB_FILE_NAME).getAbsolutePath();
        queue = new SQLDatabaseQueue(dbFilename, provider);

        int dbVersion = queue.getVersion();
        // Increment the hundreds position if a schema change means that older
        // versions of the code will not be able to read the migrated database.
        int highestSupportedVersionExclusive = 300;
        if (dbVersion >= highestSupportedVersionExclusive) {
            throw new DatastoreException(String.format("Database version is higher than the " +
                    "version supported by this library, current version %d , highest supported " +
                    "version %d", dbVersion, highestSupportedVersionExclusive - 1));
        }
        queue.updateSchema(new SchemaOnlyMigration(DatastoreConstants.getSchemaVersion3()), 3);
        queue.updateSchema(new SchemaOnlyMigration(DatastoreConstants.getSchemaVersion4()), 4);
        queue.updateSchema(new SchemaOnlyMigration(DatastoreConstants.getSchemaVersion5()), 5);
        queue.updateSchema(new SchemaOnlyMigration(DatastoreConstants.getSchemaVersion6()), 6);
        queue.updateSchema(new MigrateDatabase6To100(), 100);
        queue.updateSchema(new MigrateDatabase100To200(DatastoreConstants.getSchemaVersion200()),
                200);
        this.eventBus = new EventBus();

        this.attachmentsDir = this.extensionDataFolder(ATTACHMENTS_EXTENSION_NAME);
        this.attachmentStreamFactory = new AttachmentStreamFactory(this.getKeyProvider());
    }

    @Override
    public String getDatastoreName() {
        return this.datastoreDir.getName();
    }

    public KeyProvider getKeyProvider() {
        return this.keyProvider;
    }

    @Override
    public long getLastSequence() {
        Misc.checkState(this.isOpen(), "Database is closed");

        try {
            return queue.submit(new GetLastSequenceCallable()).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Failed to get last Sequence", e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get last Sequence", e);
            if (e.getCause() != null) {
                if (e.getCause() instanceof IllegalStateException) {
                    throw (IllegalStateException) e.getCause();
                }
            }
        }
        return 0;

    }

    @Override
    public int getDocumentCount() {
        Misc.checkState(this.isOpen(), "Database is closed");
        try {
            return get(queue.submit(new GetDocumentCountCallable()));
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get document count", e);
        }
        return 0;

    }

    @Override
    public boolean containsDocument(String docId, String revId) {
        Misc.checkState(this.isOpen(), "Database is closed");
        try {
            // TODO this can be made quicker than getting the whole document
            return getDocument(docId, revId) != null;
        } catch (DocumentNotFoundException e) {
            return false;
        }
    }

    @Override
    public boolean containsDocument(String docId) {
        Misc.checkState(this.isOpen(), "Database is closed");
        try {
            // TODO this can be made quicker than getting the whole document
            return getDocument(docId) != null;
        } catch (DocumentNotFoundException e) {
            return false;
        }
    }

    @Override
    public DocumentRevision getDocument(String id) throws DocumentNotFoundException {
        Misc.checkState(this.isOpen(), "Database is closed");
        return getDocument(id, null);
    }

    @Override
    public DocumentRevision getDocument(final String id, final String rev) throws
            DocumentNotFoundException {
        Misc.checkState(this.isOpen(), "Database is closed");
        Misc.checkNotNullOrEmpty(id, "Document id");

        try {
            return get(queue.submit(new GetDocumentCallable(id, rev, this.attachmentsDir, this.attachmentStreamFactory)));
        } catch (ExecutionException e) {
            throw new DocumentNotFoundException(id, rev, e.getCause());
        }
    }

    /**
     * <p>Returns {@code DocumentRevisionTree} of a document.</p>
     *
     * <p>The tree contains the complete revision history of the document,
     * including branches for conflicts and deleted leaves.</p>
     *
     * @param docId  id of the document
     * @return {@code DocumentRevisionTree} of the specified document
     */
    public DocumentRevisionTree getAllRevisionsOfDocument(final String docId) {

        try {
            return get(queue.submit(new GetAllRevisionsOfDocumentCallable(docId, this.attachmentsDir, this.attachmentStreamFactory)));
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get all revisions of document", e);
        }
        return null;
    }

    @Override
    public Changes changes(long since, final int limit) {
        Misc.checkState(this.isOpen(), "Database is closed");
        Misc.checkArgument(limit > 0, "Limit must be positive number");
        final long verifiedSince = since >= 0 ? since : 0;

        try {
            return get(queue.submit(new ChangesCallable(verifiedSince, limit, attachmentsDir, attachmentStreamFactory)));
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get changes", e);
            if (e.getCause() instanceof IllegalStateException) {
                throw new IllegalStateException("Failed to get changes, SQLite version: " + queue
                        .getSQLiteVersion(), e);
            }
        }

        return null;

    }

    /**
     * Get list of documents for given list of numeric ids. The result list is ordered by
     * sequence number,
     * and only the current revisions are returned.
     *
     * @param docIds given list of internal ids
     * @return list of documents ordered by sequence number
     */
    List<DocumentRevision> getDocumentsWithInternalIds(final List<Long> docIds) {
        Misc.checkNotNull(docIds, "Input document internal id list");

        try {
            return get(queue.submit(new GetDocumentsWithInternalIdsCallable(docIds, this.attachmentsDir, this.attachmentStreamFactory)));
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get documents using internal ids", e);
        }
        return null;
    }

    @Override
    public List<DocumentRevision> getAllDocuments(final int offset, final int limit, final
    boolean descending) {
        Misc.checkState(this.isOpen(), "Database is closed");
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be >= 0");
        }
        try {
            return get(queue.submit(new GetAllDocumentsCallable(offset, limit, descending, this.attachmentsDir, this.attachmentStreamFactory)));
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get all documents", e);
        }
        return null;

    }

    @Override
    public List<String> getAllDocumentIds() {
        Misc.checkState(this.isOpen(), "Database is closed");
        try {
            return get(queue.submit(new GetAllDocumentIdsCallable()));
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get all document ids", e);
        }
        return null;
    }

    @Override
    public List<DocumentRevision> getDocumentsWithIds(final List<String> docIds) throws
            DocumentException {
        Misc.checkState(this.isOpen(), "Database is closed");
        Misc.checkNotNull(docIds, "Input document id list");
        try {
            return get (queue.submit(new GetDocumentsWithIdsCallable(docIds, attachmentsDir, attachmentStreamFactory)));
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get documents with ids", e);
            throw new DocumentException("Failed to get documents with ids", e);
        }
    }

    public List<String> getPossibleAncestorRevisionIDs(final String docId,
                                                       final String revId,
                                                       final int limit) {
        try {
            return get(queue.submit(new GetPossibleAncestorRevisionIdsCallable(docId, revId, limit)));
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<DocumentRevision> sortDocumentsAccordingToIdList(List<String> docIds,
                                                                  List<DocumentRevision> docs) {
        Map<String, DocumentRevision> idToDocs = putDocsIntoMap(docs);
        List<DocumentRevision> results = new ArrayList<DocumentRevision>();
        for (String id : docIds) {
            if (idToDocs.containsKey(id)) {
                results.add(idToDocs.remove(id));
            } else {
                logger.fine("No document found for id: " + id);
            }
        }
        assert idToDocs.size() == 0;
        return results;
    }

    private static Map<String, DocumentRevision> putDocsIntoMap(List<DocumentRevision> docs) {
        Map<String, DocumentRevision> map = new HashMap<String, DocumentRevision>();
        for (DocumentRevision doc : docs) {
            // id should be unique cross all docs
            assert !map.containsKey(doc.getId());
            map.put(doc.getId(), doc);
        }
        return map;
    }

    /**
     * <p>Returns the current winning revision of a local document.</p>
     *
     * @param docId id of the local document
     * @return {@code LocalDocument} of the document
     * @throws DocumentNotFoundException if the document ID doesn't exist
     */
    public LocalDocument getLocalDocument(final String docId) throws DocumentNotFoundException {
        Misc.checkState(this.isOpen(), "Database is closed");
        try {
            return get(queue.submit(new GetLocalDocumentCallable(docId)));
        } catch (ExecutionException e) {
            throw new DocumentNotFoundException(e);
        }
    }

    // TODO move to callable
    private DocumentRevision createDocumentBody(SQLDatabase db, String docId, final DocumentBody
            body)
            throws AttachmentException, ConflictException, DatastoreException {
        Misc.checkState(this.isOpen(), "Database is closed");
        CouchUtils.validateDocumentId(docId);
        Misc.checkNotNull(body, "Input document body");
        this.validateDBBody(body);

        // check if the docid exists first:

        // if it does exist:
        // * if winning leaf deleted, root the 'created' document there
        // * else raise error
        // if it does not exist:
        // * normal insert logic for a new document

        InsertRevisionCallable callable = new InsertRevisionCallable();
        DocumentRevision potentialParent = null;

        try {
            potentialParent = new GetDocumentCallable(docId, null, this.attachmentsDir, this.attachmentStreamFactory).call(db);
        } catch (DocumentNotFoundException e) {
            //this is an expected exception, it just means we are
            // resurrecting the document
        }

        if (potentialParent != null) {
            if (!potentialParent.isDeleted()) {
                // current winner not deleted, can't insert
                throw new ConflictException(String.format("Cannot create doc, document with id %s" +
                        " already exists "
                        , docId));
            }
            // if we got here, parent rev was deleted
            new SetCurrentCallable(potentialParent.getSequence(), false).call(db);
            callable.revId = CouchUtils.generateNextRevisionId(potentialParent.getRevision());
            callable.docNumericId = potentialParent.getInternalNumericId();
            callable.parentSequence = potentialParent.getSequence();
        } else {
            // otherwise we are doing a normal create document
            long docNumericId = new InsertDocumentIDCallable(docId).call(db);
            callable.revId = CouchUtils.getFirstRevisionId();
            callable.docNumericId = docNumericId;
            callable.parentSequence = -1l;
        }
        callable.deleted = false;
        callable.current = true;
        callable.data = body.asBytes();
        callable.available = true;
        callable.call(db);

        try {
            DocumentRevision doc = new GetDocumentCallable(docId, callable.revId, this.attachmentsDir, this.attachmentStreamFactory).call(db);
            logger.finer("New document created: " + doc.toString());
            return doc;
        } catch (DocumentNotFoundException e) {
            throw new RuntimeException(String.format("Could not get document we just inserted " +
                    "(id: %s); this should not happen, please file an issue with as much detail " +
                    "as possible.", docId), e);
        }

    }

    public static void validateDBBody(DocumentBody body) {
        for (String name : body.asMap().keySet()) {
            if (name.startsWith("_")) {
                throw new InvalidDocumentException("Field name start with '_' is not allowed. ");
            }
        }
    }

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
    public LocalDocument insertLocalDocument(final String docId, final DocumentBody body) throws
            DocumentException {
        Misc.checkState(this.isOpen(), "Database is closed");
        CouchUtils.validateDocumentId(docId);
        Misc.checkNotNull(body, "Input document body");
        try {
            return get(queue.submit(new InsertLocalDocumentCallable(docId, body)));
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to insert local document", e);
            throw new DocumentException("Cannot insert local document", e);
        }
    }

    /**
     * <p>Deletes a local document.</p>
     *
     * @param docId documentId of the document to be deleted
     *
     * @throws DocumentNotFoundException if the document ID doesn't exist
     */
    public void deleteLocalDocument(final String docId) throws DocumentNotFoundException {
        Misc.checkState(this.isOpen(), "Database is closed");
        Misc.checkNotNullOrEmpty(docId, "Input document id");

        try {
            get(queue.submit(new DeleteLocalDocumentCallable(docId)));
        } catch (ExecutionException e) {
            throw new DocumentNotFoundException(docId, null, e);
        }

    }

    public static InsertRevisionCallable insertStubRevisionAdaptor(long docNumericId, String revId, long
            parentSequence) {
        // don't copy attachments
        InsertRevisionCallable callable = new InsertRevisionCallable();
        callable.docNumericId = docNumericId;
        callable.revId = revId;
        callable.parentSequence = parentSequence;
        callable.deleted = false;
        callable.current = false;
        callable.data = JSONUtils.emptyJSONObjectAsBytes();
        callable.available = false;
        return callable;
    }

    public static List<DocumentRevision> getRevisionsFromRawQuery(SQLDatabase db, String sql, String[]
            args, String attachmentsDir, AttachmentStreamFactory attachmentStreamFactory)
            throws DocumentException,
            DatastoreException {
        List<DocumentRevision> result = new ArrayList<DocumentRevision>();
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, args);
            while (cursor.moveToNext()) {
                long sequence = cursor.getLong(3);
                List<? extends Attachment> atts = AttachmentManager.attachmentsForRevision(db, attachmentsDir, attachmentStreamFactory
                        , sequence);
                DocumentRevision row = getFullRevisionFromCurrentCursor(cursor, atts);
                result.add(row);
            }
        } catch (SQLException e) {
            throw new DatastoreException(e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
        return result;
    }

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
    public String getPublicIdentifier() throws DatastoreException {
        Misc.checkState(this.isOpen(), "Database is closed");
        try {
            return get(queue.submit(new SQLCallable<String>() {
                @Override
                public String call(SQLDatabase db) throws Exception {
                    Cursor cursor = null;
                    try {
                        cursor = db.rawQuery("SELECT value FROM info WHERE key='publicUUID'", null);
                        if (cursor.moveToFirst()) {
                            return "touchdb_" + cursor.getString(0);
                        } else {
                            throw new IllegalStateException("Error querying PublicUUID, " +
                                    "it is probably because the sqlDatabase is not probably " +
                                    "initialized.");
                        }
                    } finally {
                        DatabaseUtils.closeCursorQuietly(cursor);
                    }
                }
            }));
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get public ID", e);
            throw new DatastoreException("Failed to get public ID", e);
        }
    }

    /**
     * This method has been deprecated and should not be used.
     * @see #forceInsert(List)
     */
    @Deprecated
    public void forceInsert(final DocumentRevision rev,
                            final List<String> revisionHistory,
                            final Map<String, Object> attachments,
                            final Map<String[], List<PreparedAttachment>> preparedAttachments,
                            final boolean pullAttachmentsInline) throws DocumentException {
        forceInsert(Collections.singletonList(new ForceInsertItem(rev, revisionHistory,
                attachments, preparedAttachments, pullAttachmentsInline)));
    }

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
     * (see
     * <a href="https://wiki.apache.org/couchdb/HTTP_Bulk_Document_API#Posting_Existing_Revisions">
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
     * <b>attachments</b> Attachments metadata and optionally data if {@code
     * pullAttachmentsInline} true
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
     * @see Database#getEventBus()
     * @throws DocumentException if there was an error inserting the revision or its attachments
     * into the database
     */
    public void forceInsert(final List<ForceInsertItem> items) throws DocumentException {
        Misc.checkState(this.isOpen(), "Database is closed");

        for (ForceInsertItem item : items) {
            Misc.checkNotNull(item.rev, "Input document revision");
            Misc.checkNotNull(item.revisionHistory, "Input revision history");
            Misc.checkArgument(item.revisionHistory.size() > 0, "Input revision history " +
                    "must not be empty");

            Misc.checkArgument(checkCurrentRevisionIsInRevisionHistory(item.rev, item
                    .revisionHistory), "Current revision must exist in revision history.");
            Misc.checkArgument(checkRevisionIsInCorrectOrder(item.revisionHistory),
                    "Revision history must be in right order.");
            CouchUtils.validateDocumentId(item.rev.getId());
            CouchUtils.validateRevisionId(item.rev.getRevision());
        }

        try {
            // for raising events after completing database transaction
            List<DocumentModified> events = queue.submitTransaction(new ForceInsertCallable(items, attachmentsDir, attachmentStreamFactory)).get();

            // if we got here, everything got written to the database successfully
            // now raise any events we stored up
            for (DocumentModified event : events) {
                eventBus.post(event);
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new DocumentException(e);
        }

    }

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
     * @see DatabaseImpl#forceInsert(DocumentRevision, List, Map, Map, boolean)
     * @throws DocumentException if there was an error inserting the revision into the database
     */
    public void forceInsert(DocumentRevision rev, String... revisionHistory) throws
            DocumentException {
        Misc.checkState(this.isOpen(), "Database is closed");
        this.forceInsert(rev, Arrays.asList(revisionHistory), null, null, false);
    }

    private boolean checkRevisionIsInCorrectOrder(List<String> revisionHistory) {
        for (int i = 0; i < revisionHistory.size() - 1; i++) {
            CouchUtils.validateRevisionId(revisionHistory.get(i));
            int l = CouchUtils.generationFromRevId(revisionHistory.get(i));
            int m = CouchUtils.generationFromRevId(revisionHistory.get(i + 1));
            if (l >= m) {
                return false;
            }
        }
        return true;
    }

    public static boolean checkCurrentRevisionIsInRevisionHistory(DocumentRevision rev, List<String>
            revisionHistory) {
        return revisionHistory.get(revisionHistory.size() - 1).equals(rev.getRevision());
    }

    // TODO can this run async? if so no need to call get()
    @Override
    public void compact() {
        try {
            get(queue.submit(new CompactCallable(this.attachmentsDir)));
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to compact database", e);
        }

    }

    public void close() {
        queue.shutdown();
    }

    boolean isOpen() {
        return !queue.isShutdown();
    }

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
     * @see
     * <a href="http://wiki.apache.org/couchdb/HttpPostRevsDiff">HttpPostRevsDiff documentation</a>
     * @param revisions a Multimap of document id → revision id
     * @return the subset of given the document id/revisions that are already stored in the database
     */
    public Map<String, List<String>> revsDiff(final Map<String, List<String>> revisions) {
        Misc.checkState(this.isOpen(), "Database is closed");
        Misc.checkNotNull(revisions, "Input revisions");

        // TODO hoist down queue.submit to just surround the call to RevsDiffBatchCallable
        try {
            return get(queue.submit(new SQLCallable<Map<String, List<String>>>() {
                @Override
                public Map<String, List<String>> call(SQLDatabase db) throws Exception {

                    ValueListMap<String, String> missingRevs = new ValueListMap<String, String>();

                    // Break down by docId first to avoid potential rev ID clashes between doc IDs
                    for (Map.Entry<String, List<String>> entry : revisions.entrySet()) {
                        String docId = entry.getKey();
                        List<String> revs = entry.getValue();
                        // Partition into batches to avoid exceeding placeholder limit
                        // The doc ID will use one placeholder, so use limit - 1 for the number of
                        // revs for the remaining placeholders.
                        List<List<String>> batches = CollectionUtils.partition(revs,
                                SQLITE_QUERY_PLACEHOLDERS_LIMIT - 1);

                        for (List<String> revsBatch : batches) {
                            missingRevs.addValuesToKey(docId, new RevsDiffBatchCallable(docId, revsBatch).call(db));
                        }
                    }
                    return missingRevs;
                }
            }));
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to do revsdiff", e);
        }

        return null;
    }

    public String extensionDataFolder(String extensionName) {
        Misc.checkState(this.isOpen(), "Database is closed");
        Misc.checkNotNullOrEmpty(extensionName, "Extension name");
        return new File(this.extensionsDir, extensionName).getAbsolutePath();
    }

    @Override
    public Iterator<String> getConflictedDocumentIds() {

        try {
            return get(queue.submit(new GetConflictedDocumentIdsCallable())).iterator();
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get conflicted document Ids", e);
        }
        return null;

    }

    @Override
    public void resolveConflictsForDocument(final String docId, final ConflictResolver resolver)
            throws ConflictException {


        try {

            // before starting the tx, get the 'new winner' and see if we need to prepare its
            // attachments
            final DocumentRevisionTree docTree = get(queue.submit(new GetAllRevisionsOfDocumentCallable(docId, attachmentsDir, attachmentStreamFactory)));
            if (!docTree.hasConflicts()) {
                return;
            }
            DocumentRevision newWinner = null;
            try {
                newWinner = resolver.resolve(docId, docTree.leafRevisions(true));
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Exception when calling ConflictResolver", e);
            }
            if (newWinner == null) {
                // resolve() threw an exception or returned null, exit early
                return;
            }

            final String revIdKeep = newWinner.getRevision();
            if (revIdKeep == null) {
                throw new IllegalArgumentException("Winning revision must have a revision" +
                        " id");
            }

            final DocumentRevision newWinnerTx = newWinner;
            get(queue.submitTransaction(
                    new SQLCallable<Void>() {
                        @Override
                        public Void call(SQLDatabase db) throws Exception {

                            new ResolveConflictsForDocumentCallable(docTree, revIdKeep).call(db);

                            // if this is a new or modified revision: graft the new revision on
                            if (newWinnerTx.isBodyModified() || (newWinnerTx.getAttachments() != null && ((ChangeNotifyingMap<String, Attachment>) newWinnerTx
                                    .getAttachments()).hasChanged())) {

                                // We need to work out which of the attachments for the revision are ones
                                // we can copy over because they exist in the attachment store already and
                                // which are new, that we need to prepare for insertion.
                                Collection<Attachment> attachments = newWinnerTx.getAttachments() != null ?
                                        newWinnerTx.getAttachments().values() : new ArrayList<Attachment>();
                                final List<PreparedAttachment> preparedNewAttachments =
                                        AttachmentManager.prepareAttachments(attachmentsDir,
                                                attachmentStreamFactory,
                                                AttachmentManager.findNewAttachments(attachments));
                                final List<SavedAttachment> existingAttachments =
                                        AttachmentManager.findExistingAttachments(attachments);

                                new UpdateDocumentFromRevisionCallable(newWinnerTx,
                                        preparedNewAttachments,
                                        existingAttachments, attachmentsDir, attachmentStreamFactory).call(db);
                            }
                            return null;
                        }
                    }));
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to resolve Conflicts", e);
            if (e.getCause() != null) {
                if (e.getCause() instanceof IllegalArgumentException) {
                    throw (IllegalArgumentException) e.getCause();
                }
            }
        }

    }

    public static InsertRevisionCallable insertNewWinnerRevisionAdaptor(DocumentBody newWinner,
                                           DocumentRevision oldWinner)
            throws AttachmentException, DatastoreException {
        String newRevisionId = CouchUtils.generateNextRevisionId(oldWinner.getRevision());

        InsertRevisionCallable callable = new InsertRevisionCallable();
        callable.docNumericId = oldWinner.getInternalNumericId();
        callable.revId = newRevisionId;
        callable.parentSequence = oldWinner.getSequence();
        callable.deleted = false;
        callable.current = true;
        callable.data = newWinner.asBytes();
        callable.available = true;
        return callable;
    }

    public static DocumentRevision getFullRevisionFromCurrentCursor(Cursor cursor,
                                                                     List<? extends Attachment>
                                                                             attachments) {
        String docId = cursor.getString(cursor.getColumnIndex("docid"));
        long internalId = cursor.getLong(cursor.getColumnIndex("doc_id"));
        String revId = cursor.getString(cursor.getColumnIndex("revid"));
        long sequence = cursor.getLong(cursor.getColumnIndex("sequence"));
        byte[] json = cursor.getBlob(cursor.getColumnIndex("json"));
        boolean current = cursor.getInt(cursor.getColumnIndex("current")) > 0;
        boolean deleted = cursor.getInt(cursor.getColumnIndex("deleted")) > 0;

        long parent = -1L;
        if (cursor.columnType(cursor.getColumnIndex("parent")) == Cursor.FIELD_TYPE_INTEGER) {
            parent = cursor.getLong(cursor.getColumnIndex("parent"));
        } else if (cursor.columnType(cursor.getColumnIndex("parent")) == Cursor.FIELD_TYPE_NULL) {
        } else {
            throw new RuntimeException("Unexpected type: " + cursor.columnType(cursor
                    .getColumnIndex("parent")));
        }

        DocumentRevisionBuilder builder = new DocumentRevisionBuilder()
                .setDocId(docId)
                .setRevId(revId)
                .setBody(DocumentBodyImpl.bodyWith(json))
                .setDeleted(deleted)
                .setSequence(sequence)
                .setInternalId(internalId)
                .setCurrent(current)
                .setParent(parent)
                .setAttachments(attachments);

        return builder.build();
    }

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
    public PreparedAttachment prepareAttachment(Attachment att, long length, long encodedLength)
            throws AttachmentException {
        PreparedAttachment pa = AttachmentManager.prepareAttachment(attachmentsDir,
                attachmentStreamFactory, att, length, encodedLength);
        return pa;
    }

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
    public Attachment getAttachment(final String id, final String rev, final String
            attachmentName) {
        try {
            return get(queue.submit(new SQLCallable<Attachment>() {
                @Override
                public Attachment call(SQLDatabase db) throws Exception {
                    long sequence = new GetSequenceCallable(id, rev).call(db);
                    return AttachmentManager.getAttachment(db, attachmentsDir,
                            attachmentStreamFactory, sequence, attachmentName);
                }
            }));
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get attachment", e);
        }

        return null;
    }

    /**
     * <p>Returns all attachments for the revision.</p>
     *
     * <p>Used by replicator when pushing attachments</p>
     *
     * @param rev The revision with which the attachments are associated
     * @return List of <code>Attachment</code>
     * @throws AttachmentException if there was an error reading the attachment metadata from the
     * database
     */
    public List<? extends Attachment> attachmentsForRevision(final DocumentRevision rev) throws
            AttachmentException {
        try {
            return get(queue.submit(new SQLCallable<List<? extends Attachment>>() {

                @Override
                public List<? extends Attachment> call(SQLDatabase db) throws Exception {
                    return AttachmentManager.attachmentsForRevision(db, attachmentsDir,
                            attachmentStreamFactory, rev.getSequence());
                }
            }));
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get attachments for revision");
            throw new AttachmentException(e);
        }

    }


    @Override
    public EventBus getEventBus() {
//        Misc.checkState(this.isOpen(), "Database is closed");
        return eventBus;
    }

    @Override
    public DocumentRevision createDocumentFromRevision(final DocumentRevision rev)
            throws DocumentException {
        Misc.checkNotNull(rev, "DocumentRevision");
        Misc.checkState(isOpen(), "Datastore is closed");
        Misc.checkArgument(rev.getRevision() == null, "Revision ID must be null for new " +
                "DocumentRevisions");
        Misc.checkArgument(rev.isFullRevision(), "Projected revisions cannot be used to " +
                "create documents");
        final String docId;
        // create docid if docid is null
        if (rev.getId() == null) {
            docId = CouchUtils.generateDocumentId();
        } else {
            docId = rev.getId();
        }

        // We need to work out which of the attachments for the revision are ones
        // we can copy over because they exist in the attachment store already and
        // which are new, that we need to prepare for insertion.
        Collection<Attachment> attachments = rev.getAttachments() != null ? rev.getAttachments()
                .values() : new ArrayList<Attachment>();
        final List<PreparedAttachment> preparedNewAttachments =
                AttachmentManager.prepareAttachments(attachmentsDir,
                        attachmentStreamFactory,
                        AttachmentManager.findNewAttachments(attachments));
        final List<SavedAttachment> existingAttachments =
                AttachmentManager.findExistingAttachments(attachments);

        DocumentRevision created = null;
        try {
            created = get(queue.submitTransaction(new SQLCallable<DocumentRevision>() {
                @Override
                public DocumentRevision call(SQLDatabase db) throws Exception {

                    // Save document with new JSON body, add new attachments and copy over
                    // existing attachments
                    DocumentRevision saved = createDocumentBody(db, docId, rev.getBody());
                    AttachmentManager.addAttachmentsToRevision(db, attachmentsDir, saved,
                            preparedNewAttachments);
                    AttachmentManager.copyAttachmentsToRevision(db, existingAttachments, saved);

                    // now re-fetch the revision with updated attachments
                    DocumentRevision updatedWithAttachments = new GetDocumentCallable(
                            saved.getId(), saved.getRevision(), attachmentsDir, attachmentStreamFactory).call(db);
                    return updatedWithAttachments;
                }
            }));
            return created;
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to create document", e);
            throw new DocumentException(e);
        } finally {
            if (created != null) {
                eventBus.post(new DocumentCreated(created));
            }
        }
    }

    @Override
    public DocumentRevision updateDocumentFromRevision(final DocumentRevision rev)
            throws DocumentException {

        Misc.checkNotNull(rev, "DocumentRevision");
        Misc.checkState(isOpen(), "Datastore is closed");
        Misc.checkArgument(rev.isFullRevision(), "Projected revisions cannot be used to " +
                "create documents");

        // We need to work out which of the attachments for the revision are ones
        // we can copy over because they exist in the attachment store already and
        // which are new, that we need to prepare for insertion.
        Collection<Attachment> attachments = rev.getAttachments() != null ? rev.getAttachments()
                .values() : new ArrayList<Attachment>();
        final List<PreparedAttachment> preparedNewAttachments =
                AttachmentManager.prepareAttachments(attachmentsDir,
                        attachmentStreamFactory,
                        AttachmentManager.findNewAttachments(attachments));
        final List<SavedAttachment> existingAttachments =
                AttachmentManager.findExistingAttachments(attachments);

        try {
            DocumentRevision revision = get(queue.submitTransaction(new UpdateDocumentFromRevisionCallable(
                            rev, preparedNewAttachments, existingAttachments, this.attachmentsDir, this.attachmentStreamFactory)));

            if (revision != null) {
                eventBus.post(new DocumentUpdated(getDocument(rev.getId(), rev.getRevision()),
                        revision));
            }

            return revision;
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to updated document", e);
            throw new DocumentException(e);
        }

    }

    @Override
    public DocumentRevision deleteDocumentFromRevision(final DocumentRevision rev) throws
            ConflictException {
        Misc.checkNotNull(rev, "DocumentRevision");
        Misc.checkState(isOpen(), "Datastore is closed");

        try {
            DocumentRevision deletedRevision = get(queue.submit(new DeleteDocumentCallable(rev.getId(), rev.getRevision())));

            if (deletedRevision != null) {
                eventBus.post(new DocumentDeleted(rev, deletedRevision));
            }

            return deletedRevision;
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to delete document", e);
            if (e.getCause() != null) {
                if (e.getCause() instanceof ConflictException) {
                    throw (ConflictException) e.getCause();
                }
            }
        }

        return null;
    }

    // delete all leaf nodes
    @Override
    public List<DocumentRevision> deleteDocument(final String id)
            throws DocumentException {
        Misc.checkNotNull(id, "ID");
        try {
            return get(queue.submitTransaction(new DeleteAllRevisionsCallable(id)));
        } catch (ExecutionException e) {
            throw new DocumentException("Failed to delete document", e);
        }
    }

    <T> Future<T> runOnDbQueue(SQLCallable<T> callable) {
        return queue.submit(callable);
    }

    // helper to avoid having to catch ExecutionExceptions
    public static <T> T get(Future<T> future) throws ExecutionException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Re-throwing InterruptedException as ExecutionException");
            throw new ExecutionException(e);
        }
    }


}
