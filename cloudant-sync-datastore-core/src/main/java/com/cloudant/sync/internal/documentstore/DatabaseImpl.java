/*
 * Copyright © 2016, 2018 IBM Corp. All rights reserved.
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
import com.cloudant.sync.documentstore.AttachmentException;
import com.cloudant.sync.documentstore.Changes;
import com.cloudant.sync.documentstore.ConflictException;
import com.cloudant.sync.documentstore.ConflictResolver;
import com.cloudant.sync.documentstore.Database;
import com.cloudant.sync.documentstore.DocumentBody;
import com.cloudant.sync.documentstore.DocumentException;
import com.cloudant.sync.documentstore.DocumentNotFoundException;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.documentstore.InvalidDocumentException;
import com.cloudant.sync.documentstore.LocalDocument;
import com.cloudant.sync.documentstore.encryption.KeyProvider;
import com.cloudant.sync.event.EventBus;
import com.cloudant.sync.event.notifications.DocumentCreated;
import com.cloudant.sync.event.notifications.DocumentDeleted;
import com.cloudant.sync.event.notifications.DocumentModified;
import com.cloudant.sync.event.notifications.DocumentUpdated;
import com.cloudant.sync.internal.common.CouchConstants;
import com.cloudant.sync.internal.common.CouchUtils;
import com.cloudant.sync.internal.common.ValueListMap;
import com.cloudant.sync.internal.documentstore.callables.ChangesCallable;
import com.cloudant.sync.internal.documentstore.callables.CompactCallable;
import com.cloudant.sync.internal.documentstore.callables.DeleteAllRevisionsCallable;
import com.cloudant.sync.internal.documentstore.callables.DeleteDocumentCallable;
import com.cloudant.sync.internal.documentstore.callables.DeleteLocalDocumentCallable;
import com.cloudant.sync.internal.documentstore.callables.ForceInsertCallable;
import com.cloudant.sync.internal.documentstore.callables.GetAllDocumentIdsCallable;
import com.cloudant.sync.internal.documentstore.callables.GetAllDocumentsCallable;
import com.cloudant.sync.internal.documentstore.callables.GetAllRevisionsOfDocumentCallable;
import com.cloudant.sync.internal.documentstore.callables.GetConflictedDocumentIdsCallable;
import com.cloudant.sync.internal.documentstore.callables.GetDocumentCallable;
import com.cloudant.sync.internal.documentstore.callables.GetDocumentCountCallable;
import com.cloudant.sync.internal.documentstore.callables.GetDocumentsWithIdsCallable;
import com.cloudant.sync.internal.documentstore.callables.GetLastSequenceCallable;
import com.cloudant.sync.internal.documentstore.callables.GetLocalDocumentCallable;
import com.cloudant.sync.internal.documentstore.callables.GetPossibleAncestorRevisionIdsCallable;
import com.cloudant.sync.internal.documentstore.callables.GetPublicIdentifierCallable;
import com.cloudant.sync.internal.documentstore.callables.GetSequenceCallable;
import com.cloudant.sync.internal.documentstore.callables.InsertDocumentIDCallable;
import com.cloudant.sync.internal.documentstore.callables.InsertLocalDocumentCallable;
import com.cloudant.sync.internal.documentstore.callables.InsertRevisionCallable;
import com.cloudant.sync.internal.documentstore.callables.ResolveConflictsForDocumentCallable;
import com.cloudant.sync.internal.documentstore.callables.RevsDiffBatchCallable;
import com.cloudant.sync.internal.documentstore.callables.SetCurrentCallable;
import com.cloudant.sync.internal.documentstore.callables.UpdateDocumentFromRevisionCallable;
import com.cloudant.sync.internal.documentstore.migrations.MigrateDatabase100To200;
import com.cloudant.sync.internal.documentstore.migrations.MigrateDatabase6To100;
import com.cloudant.sync.internal.documentstore.migrations.SchemaOnlyMigration;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.sqlite.SQLDatabaseQueue;
import com.cloudant.sync.internal.util.CollectionUtils;
import com.cloudant.sync.internal.util.DatabaseUtils;
import com.cloudant.sync.internal.util.JSONUtils;
import com.cloudant.sync.internal.util.Misc;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatabaseImpl implements Database, com.cloudant.sync.documentstore.advanced.Database {

    private static final Logger logger = Logger.getLogger(DatabaseImpl.class.getCanonicalName());

    // Limit of parameters (placeholders) one query can have.
    // SQLite has limit on the number of placeholders on a single query, default 999.
    // http://www.sqlite.org/limits.html
    public static final int SQLITE_QUERY_PLACEHOLDERS_LIMIT = 500;

    private final EventBus eventBus;

    final File datastoreDir;

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

    /** Directory where attachments are stored for this DocumentStore */
    private final String attachmentsDir;

    /**
     * Creates streams used for encrypting and encoding (gzip etc.) attachments when
     * reading to and from disk.
     */
    private final AttachmentStreamFactory attachmentStreamFactory;

    /**
     * Constructor for single thread SQLCipher-based DocumentStore.
     * @param location The location where the DocumentStore will be opened/created
     * @param extensionsLocation The location where the DocumentStore's extensions are stored
     * @param provider The key provider object that contains the user-defined SQLCipher key
     * @throws SQLException
     * @throws IOException
     */
    public DatabaseImpl(File location, File extensionsLocation, KeyProvider provider) throws SQLException,
            IOException, DocumentStoreException {
        Misc.checkNotNull(location, "location");
        Misc.checkNotNull(extensionsLocation, "extensionsLocation");
        Misc.checkNotNull(provider, "Key provider");

        this.keyProvider = provider;
        this.datastoreDir = location;
        this.attachmentsDir = new File(extensionsLocation, ATTACHMENTS_EXTENSION_NAME).getAbsolutePath();

        final File dbFile = new File(this.datastoreDir, DB_FILE_NAME);
        queue = new SQLDatabaseQueue(dbFile, provider);

        int dbVersion = queue.getVersion();
        // Increment the hundreds position if a schema change means that older
        // versions of the code will not be able to read the migrated database.
        int highestSupportedVersionExclusive = 300;
        if (dbVersion >= highestSupportedVersionExclusive) {
            throw new DocumentStoreException(String.format("Database version is higher than the " +
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

        this.attachmentStreamFactory = new AttachmentStreamFactory(this.getKeyProvider());
    }

    @Override
    public File getPath() {
        return this.datastoreDir;
    }

    public KeyProvider getKeyProvider() {
        return this.keyProvider;
    }

    @Override
    public long getLastSequence() throws DocumentStoreException {
        Misc.checkState(this.isOpen(), "Database is closed");

        try {
            return get(queue.submit(new GetLastSequenceCallable()));
        } catch (ExecutionException e) {
            throwCauseAs(e, IllegalStateException.class);
            String message = "Failed to get last Sequence";
            logger.log(Level.SEVERE, message, e);
            throw new DocumentStoreException(message, e.getCause());
        }
    }

    @Override
    public int getDocumentCount() throws DocumentStoreException {
        Misc.checkState(this.isOpen(), "Database is closed");
        try {
            return get(queue.submit(new GetDocumentCountCallable()));
        } catch (ExecutionException e) {
            String message = "Failed to get document count";
            logger.log(Level.SEVERE, message, e);
            throw new DocumentStoreException(message, e.getCause());
        }
    }

    @Override
    public boolean contains(String docId, String revId) throws DocumentStoreException {
        Misc.checkState(this.isOpen(), "Database is closed");
        try {
            // TODO this can be made quicker than getting the whole document
            read(docId, revId);
            return true;
        } catch (DocumentNotFoundException e) {
            return false;
        }
    }

    @Override
    public boolean contains(String docId) throws DocumentStoreException {
        Misc.checkState(this.isOpen(), "Database is closed");
        try {
            // TODO this can be made quicker than getting the whole document
            read(docId);
            return true;
        } catch (DocumentNotFoundException e) {
            return false;
        }
    }

    @Override
    public InternalDocumentRevision read(String id) throws DocumentNotFoundException, DocumentStoreException {
        Misc.checkState(this.isOpen(), "Database is closed");
        return read(id, null);
    }

    @Override
    public InternalDocumentRevision read(final String id, final String rev) throws
            DocumentNotFoundException, DocumentStoreException {
        Misc.checkState(this.isOpen(), "Database is closed");
        Misc.checkNotNullOrEmpty(id, "Document id");
        try {
            if (id.startsWith(CouchConstants._local_prefix)) {
                Misc.checkArgument(rev == null, "Local documents must have a null revision ID");
                String localId = id.substring(CouchConstants._local_prefix.length());
                LocalDocument ld = get(queue.submit(new GetLocalDocumentCallable(localId)));
                // convert to DocumentRevision, adding back "_local/" prefix which was stripped off when document was written
                return new DocumentRevisionBuilder().setDocId(CouchConstants._local_prefix + ld.docId).setBody(ld.body).build();
            } else {
                return get(queue.submit(new GetDocumentCallable(id, rev, this.attachmentsDir, this.attachmentStreamFactory)));
            }
        } catch (ExecutionException e) {
            throwCauseAs(e, DocumentNotFoundException.class);
            String message = String.format(Locale.ENGLISH, "Failed to get document id %s at revision %s", id, rev);
            logger.log(Level.SEVERE, message, e);
            throw new DocumentStoreException(message, e.getCause());
        }
    }

    /**
     * <p>Returns {@code DocumentRevisionTree} of a document.</p>
     *
     * <p>The tree contains the complete revision history of the document,
     * including branches for conflicts and deleted leaves.</p>
     *
     * @param docId  ID of the document
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
    public Changes changes(long since, final int limit) throws DocumentStoreException {
        Misc.checkState(this.isOpen(), "Database is closed");
        Misc.checkArgument(limit > 0, "Limit must be positive number");
        final long verifiedSince = since >= 0 ? since : 0;

        try {
            return get(queue.submit(new ChangesCallable(verifiedSince, limit, attachmentsDir, attachmentStreamFactory)));
        } catch (ExecutionException e) {
            String message = "Failed to get changes";
            logger.log(Level.SEVERE, message, e);
            throw new DocumentStoreException(message, e.getCause());
        }
    }

    @Override
    public List<DocumentRevision> read(final int offset, final int limit, final
    boolean descending) throws DocumentStoreException {
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
            String message = "Failed to get all documents";
            logger.log(Level.SEVERE, message, e);
            throw new DocumentStoreException(message, e.getCause());
        }
    }

    @Override
    public List<String> getIds() throws DocumentStoreException {
        Misc.checkState(this.isOpen(), "Database is closed");
        try {
            return get(queue.submit(new GetAllDocumentIdsCallable()));
        } catch (ExecutionException e) {
            String message = "Failed to get all document ids";
            logger.log(Level.SEVERE, message, e);
            throw new DocumentStoreException(message, e.getCause());
        }
    }

    @Override
    public List<DocumentRevision> read(final List<String> docIds) throws
            DocumentStoreException {
        Misc.checkState(this.isOpen(), "Database is closed");
        Misc.checkNotNull(docIds, "Input document id list");
        Misc.checkArgument(!docIds.isEmpty(), "Input document id list must contain document ids");
        try {
            return get (queue.submit(new GetDocumentsWithIdsCallable(docIds, attachmentsDir, attachmentStreamFactory)));
        } catch (ExecutionException e) {
            String message = "Failed to get documents with ids";
            logger.log(Level.SEVERE, message, e);
            throw new DocumentStoreException(message, e);
        }
    }

    public List<String> getPossibleAncestorRevisionIDs(final String docId,
                                                       final String revId,
                                                       final int limit) throws DocumentStoreException {
        try {
            return get(queue.submit(new GetPossibleAncestorRevisionIdsCallable(docId, revId, limit)));
        } catch (ExecutionException e) {
            throw new DocumentStoreException(e);
        }
    }

    /**
     * <p>Returns the current winning revision of a local document.</p>
     *
     * @param docId ID of the local document
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
    private InternalDocumentRevision createDocumentBody(SQLDatabase db, String docId, final DocumentBody
            body)
            throws AttachmentException, ConflictException, DocumentStoreException {
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
        InternalDocumentRevision potentialParent = null;

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
            InternalDocumentRevision doc = new GetDocumentCallable(docId, callable.revId, this.attachmentsDir, this.attachmentStreamFactory).call(db);
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
     * same ID if one is present. </p>
     *
     * <p>Local documents are not replicated between DocumentStores.</p>
     *
     * @param docId      The document ID for the document
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

    /**
     * <p>Returns the DocumentStore's unique identifier.</p>
     *
     * <p>This is used for the checkpoint document in a remote DocumentStore
     * during replication.</p>
     *
     * @return a unique identifier for the DocumentStore.
     * @throws DocumentStoreException if there was an error retrieving the unique identifier from the
     * database
     */
    public String getPublicIdentifier() throws DocumentStoreException {
        Misc.checkState(this.isOpen(), "Database is closed");
        try {
            return get(queue.submit(new GetPublicIdentifierCallable()));
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get public ID", e);
            throw new DocumentStoreException("Failed to get public ID", e);
        }
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
     * <a target="_blank" href="https://wiki.apache.org/couchdb/HTTP_Bulk_Document_API#Posting_Existing_Revisions">
     * the CouchDB wiki</a> for more detail).
     * <p>
     * If the document was successfully inserted, a
     * {@link com.cloudant.sync.event.notifications.DocumentCreated DocumentCreated},
     * {@link com.cloudant.sync.event.notifications.DocumentModified DocumentModified}, or
     * {@link com.cloudant.sync.event.notifications.DocumentDeleted DocumentDeleted}
     * event is posted on the event bus. The event will depend on the nature
     * of the update made.
     * </p>
     *
     *
     * @param items one or more revisions to insert.
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
     * forceInsert(rev, Arrays.asList(revisionHistory), null, null, false);
     * </code>
     *
     * @param rev             A {@code DocumentRevision} containing the information for a revision
     *                        from a remote DocumentStore.
     * @param revisionHistory The history of the revision being inserted,
     *                        including the rev ID of {@code rev}. This list
     *                        needs to be sorted in ascending order
     * @throws DocumentException if there was an error inserting the revision into the database
     */
    public void forceInsert(InternalDocumentRevision rev, String... revisionHistory) throws
            DocumentException {
        Misc.checkState(this.isOpen(), "Database is closed");
        this.forceInsert(Collections.singletonList(new ForceInsertItem(rev, Arrays.asList
                (revisionHistory), null, null, false)));
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

    public static boolean checkCurrentRevisionIsInRevisionHistory(InternalDocumentRevision rev, List<String>
            revisionHistory) {
        return revisionHistory.get(revisionHistory.size() - 1).equals(rev.getRevision());
    }

    // TODO can this run async? if so no need to call get()
    @Override
    public void compact() throws DocumentStoreException {
        try {
            get(queue.submit(new CompactCallable(this.attachmentsDir)));
        } catch (ExecutionException e) {
            String message = "Failed to compact database";
            logger.log(Level.SEVERE, message, e);
            throw new DocumentStoreException(message, e.getCause());
        }
    }

    public void close() {
        queue.shutdown();
    }

    boolean isOpen() {
        return !queue.isShutdown();
    }

    /**
     * Returns the subset of given the document ID/revisions that are not stored in the database.
     *
     * The input revisions is a map, whose key is document ID, and value is a list of revisions.
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
     * <a target="_blank" href="http://wiki.apache.org/couchdb/HttpPostRevsDiff">HttpPostRevsDiff documentation</a>
     * @param revisions a Multimap of document ID → revision ID
     * @return the subset of given the document ID/revisions that are already stored in the database
     * @throws IllegalArgumentException if {@code revisions} is empty.
     * @throws DocumentStoreException If it was not possible to calculate the difference between revs.
     */
    public Map<String, List<String>> revsDiff(final Map<String, List<String>> revisions) throws DocumentStoreException {
        Misc.checkState(this.isOpen(), "Database is closed");
        Misc.checkNotNull(revisions, "Input revisions");
        Misc.checkArgument(!revisions.isEmpty(), "revisions cannot be empty");

        try {

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
                    missingRevs.addValuesToKey(docId, get(queue.submit(new RevsDiffBatchCallable(docId, revsBatch))));
                }
            }
            return missingRevs;
        } catch (ExecutionException e) {
            String message = "Failed to calculate difference in revisions";
            logger.log(Level.SEVERE, message, e);
            throw new DocumentStoreException(message, e);
        }
    }

    @Override
    public Iterable<String> getConflictedIds() throws DocumentStoreException {
        try {
            return get(queue.submit(new GetConflictedDocumentIdsCallable()));
        } catch (ExecutionException e) {
            String message = "Failed to get conflicted document ids";
            logger.log(Level.SEVERE, message, e);
            throw new DocumentStoreException(message, e.getCause());
        }
    }

    @Override
    public void resolveConflicts(final String docId, final ConflictResolver resolver)
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

                            // is newWinnerTx a new or updated (as opposed to existing) revision?
                            boolean isNewOrUpdatedRevision = false;
                            if (newWinnerTx.getClass().equals(DocumentRevision.class)) {
                                // user gave us a new DocumentRevision instance
                                isNewOrUpdatedRevision = true;
                            } else if (newWinnerTx instanceof InternalDocumentRevision) {
                                // user gave us an existing InternalDocumentRevision instance - did the body or attachments change?
                                InternalDocumentRevision newWinnerTxInternal = (InternalDocumentRevision)newWinnerTx;
                                if (newWinnerTxInternal.isBodyModified()) {
                                    isNewOrUpdatedRevision = true;
                                } else if (newWinnerTxInternal.getAttachments() != null) {
                                    if (newWinnerTxInternal.getAttachments().hasChanged()) {
                                        isNewOrUpdatedRevision = true;
                                    }
                                }
                            }

                            // if this is a new or modified revision: graft the new revision on
                            if (isNewOrUpdatedRevision) {

                                // We need to work out which of the attachments for the revision are ones
                                // we can copy over because they exist in the attachment store already and
                                // which are new, that we need to prepare for insertion.
                                Map<String, Attachment> attachments = newWinnerTx.getAttachments() != null ?
                                        newWinnerTx.getAttachments() : new HashMap<String, Attachment>();
                                final Map<String, PreparedAttachment> preparedNewAttachments =
                                        AttachmentManager.prepareAttachments(attachmentsDir,
                                                attachmentStreamFactory,
                                                AttachmentManager.findNewAttachments(attachments));
                                final Map<String, SavedAttachment> existingAttachments =
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
            Throwable cause = e.getCause();
            if (cause != null) {
                if (cause instanceof IllegalArgumentException) {
                    throw (IllegalArgumentException) cause;
                }
            }
        }

    }

    /**
     * <p>
     * Read attachment stream to a temporary location and calculate sha1,
     * prior to being added to the DocumentStore.
     * </p>
     * <p>
     * Used by replicator when receiving new/updated attachments
     *</p>
     *
     * @param att           Attachment to be prepared, providing data either from a file or a stream
     * @param length        Size in bytes of attachment as signalled by "length" metadata property
     * @param encodedLength Size in bytes of attachment, after encoding, as signalled by
     *                      "encoded_length" metadata property
     * @return A prepared attachment, ready to be added to the DocumentStore
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
    public Map<String, ? extends Attachment> attachmentsForRevision(final InternalDocumentRevision rev) throws
            AttachmentException {
        try {
            return get(queue.submit(new SQLCallable<Map<String, ? extends Attachment>>() {

                @Override
                public Map<String, ? extends Attachment> call(SQLDatabase db) throws Exception {
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
    public DocumentRevision create(final DocumentRevision rev)
            throws AttachmentException, InvalidDocumentException, ConflictException, DocumentStoreException {
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

        // check to see if we are creating a local (non-replicating) document
        if (docId.startsWith(CouchConstants._local_prefix)) {
            String localId = docId.substring(CouchConstants._local_prefix.length());
            try {
                insertLocalDocument(localId, rev.getBody());
                // we can return the input document as-is since there was no doc id or rev id to generate
                return rev;
            } catch (DocumentException e) {
                throw new DocumentStoreException(e.getMessage(), e.getCause());
            } finally {
                eventBus.post(new DocumentCreated(rev));
            }
        }

        // We need to work out which of the attachments for the revision are ones
        // we can copy over because they exist in the attachment store already and
        // which are new, that we need to prepare for insertion.
        Map<String, Attachment> attachments = rev.getAttachments() != null ? rev.getAttachments() : new HashMap<String, Attachment>();
        final Map<String, PreparedAttachment> preparedNewAttachments =
                AttachmentManager.prepareAttachments(attachmentsDir,
                        attachmentStreamFactory,
                        AttachmentManager.findNewAttachments(attachments));
        final Map<String, SavedAttachment> existingAttachments =
                AttachmentManager.findExistingAttachments(attachments);

        InternalDocumentRevision created = null;
        try {
            created = get(queue.submitTransaction(new SQLCallable<InternalDocumentRevision>() {
                @Override
                public InternalDocumentRevision call(SQLDatabase db) throws Exception {

                    // Save document with new JSON body, add new attachments and copy over
                    // existing attachments
                    InternalDocumentRevision saved = createDocumentBody(db, docId, rev.getBody());
                    AttachmentManager.addAttachmentsToRevision(db, attachmentsDir, saved,
                            preparedNewAttachments);
                    AttachmentManager.copyAttachmentsToRevision(db, existingAttachments, saved);

                    // now re-fetch the revision with updated attachments
                    InternalDocumentRevision updatedWithAttachments = new GetDocumentCallable(
                            saved.getId(), saved.getRevision(), attachmentsDir, attachmentStreamFactory).call(db);
                    return updatedWithAttachments;
                }
            }));
            return created;
        } catch (ExecutionException e) {
            // invalid if eg there are keys starting with _
            throwCauseAs(e, InvalidDocumentException.class);
            // conflictexception if doc ID already exists
            throwCauseAs(e, ConflictException.class);
            String message = "Failed to create document";
            logger.log(Level.SEVERE, message, e);
            throw new DocumentStoreException(message, e.getCause());
        } finally {
            if (created != null) {
                eventBus.post(new DocumentCreated(created));
            }
        }
    }

    @Override
    public DocumentRevision update(final DocumentRevision rev)
            throws AttachmentException, DocumentNotFoundException, ConflictException,
            DocumentStoreException  {

        Misc.checkNotNull(rev, "DocumentRevision");
        Misc.checkState(isOpen(), "Datastore is closed");
        Misc.checkArgument(rev.isFullRevision(), "Projected revisions cannot be used to " +
                "create documents");

        if (rev.getId().startsWith(CouchConstants._local_prefix)) {
            throw new IllegalArgumentException("Use create(final DocumentRevision rev) create or update local documents");
        }

        // Shortcut if this is a deletion
        if (rev.isDeleted()) {
            return delete(rev);
        }

        // We need to work out which of the attachments for the revision are ones
        // we can copy over because they exist in the attachment store already and
        // which are new, that we need to prepare for insertion.
        Map<String, Attachment> attachments = rev.getAttachments() != null ? rev.getAttachments() : new HashMap<String, Attachment>();
        final Map<String, PreparedAttachment> preparedNewAttachments =
                AttachmentManager.prepareAttachments(attachmentsDir,
                        attachmentStreamFactory,
                        AttachmentManager.findNewAttachments(attachments));
        final Map<String, SavedAttachment> existingAttachments =
                AttachmentManager.findExistingAttachments(attachments);

        try {
            InternalDocumentRevision revision = get(queue.submitTransaction(new UpdateDocumentFromRevisionCallable(
                            rev, preparedNewAttachments, existingAttachments, this.attachmentsDir, this.attachmentStreamFactory)));

            if (revision != null) {
                try {
                    eventBus.post(new DocumentUpdated(read(rev.getId(), rev.getRevision()),
                            revision));
                } catch (DocumentStoreException de) {
                    ; // TODO couldn't re-fetch document to post event
                } catch (DocumentException de) {
                    ; // TODO couldn't re-fetch document to post event
                }
            }

            return revision;
        } catch (ExecutionException e) {
            // invalid if eg there are keys starting with _
            throwCauseAs(e, InvalidDocumentException.class);
            // conflictexception if rev ID is not winning rev
            throwCauseAs(e, ConflictException.class);
            // not found if tried to update something that doesn't exist
            throwCauseAs(e, DocumentNotFoundException.class);
            String message = "Failed to update document";
            logger.log(Level.SEVERE, message, e);
            throw new DocumentStoreException(message, e.getCause());
        }

    }

    @Override
    public DocumentRevision delete(final DocumentRevision rev) throws
            ConflictException, DocumentNotFoundException, DocumentStoreException {
        Misc.checkNotNull(rev, "DocumentRevision");
        Misc.checkState(isOpen(), "Datastore is closed");

        if (rev.getId().startsWith(CouchConstants._local_prefix)) {
            Misc.checkArgument(rev.getRevision() == null, "Local documents must have a null revision ID");
            String localId = rev.getId().substring(CouchConstants._local_prefix.length());
            deleteLocalDocument(localId);
            // for local documents there is no "new document" to post on the event bus or return as
            // the document is removed rather than updated with a tombstone
            eventBus.post(new DocumentDeleted(rev, null));
            return null;
        }

        try {
            InternalDocumentRevision deletedRevision = get(queue.submit(new DeleteDocumentCallable(rev.getId(), rev.getRevision())));
            if (deletedRevision != null) {
                eventBus.post(new DocumentDeleted(rev, deletedRevision));
            }
            return deletedRevision;
        } catch (ExecutionException e) {
            // conflictexception if source revision isn't current rev
            throwCauseAs(e, ConflictException.class);
            // documentnotfoundexception if it's already deleted
            throwCauseAs(e, DocumentNotFoundException.class);
            String message = "Failed to delete document";
            logger.log(Level.SEVERE, message, e);
            throw new DocumentStoreException(message, e.getCause());
        }
    }

    // delete all leaf nodes
    @Override
    public List<DocumentRevision> delete(final String id)
            throws DocumentStoreException {
        Misc.checkNotNull(id, "ID");
        // check for local prefix:
        // there are two reasons not to delete local documents here:
        // 1) "delete all leaf nodes" has no meaning for local documents
        // 2) DeleteAllRevisionsCallable can throw DocumentNotFoundException which would mean a
        // breaking API change or downcasting of that exception, which is Not Good.
        if (id.startsWith(CouchConstants._local_prefix)) {
            throw new IllegalArgumentException("Use delete(final DocumentRevision rev) to delete local documents");
        }
        try {
            return get(queue.submitTransaction(new DeleteAllRevisionsCallable(id)));
        } catch (ExecutionException e) {
            String message = "Failed to delete document";
            logger.log(Level.SEVERE, message, e);
            throw new DocumentStoreException(message, e.getCause());
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

    private <T extends Throwable> void throwCauseAs(Throwable t, Class<T> type) throws T {
        if (t.getCause().getClass().equals(type)) {
            throw (T)t.getCause();
        }
    }

    @Override
    public void createWithHistory(DocumentRevision revision, int revisionsStart, List<String>
            revisionsIDs) throws DocumentException {

        // Check the arguments for validity
        Misc.checkNotNull(revision, "DocumentRevision");
        Misc.checkArgument(revisionsStart > 0, "revisionsStart must be greater than zero, but was" +
                " " + revisionsStart + ".");
        Misc.checkArgument(revisionsIDs != null && revisionsIDs.size() > 0, "revisionsIDs history" +
                " list must not be null or empty.");

        Map<String, Attachment> attachments = revision.getAttachments();
        Collections.list(Collections.enumeration(revision.getAttachments().values()));

        InternalDocumentRevision internalRev = new DocumentRevisionBuilder()
                .setDocId(revision.getId())
                .setRevId(revision.getRevision())
                .setBody(revision.getBody())
                .setAttachments(attachments)
                .setDeleted(revision.isDeleted())
                .build();

        Map<String[], Map<String, PreparedAttachment>> preparedAttachments = Collections
                .singletonMap
                        (new String[]{revision.getId(), revision.getRevision()}, AttachmentManager
                                .prepareAttachments(attachmentsDir, attachmentStreamFactory,
                                        attachments));

        // Couch _revisions.ids are newest -> oldest without a generation, so we need to manipulate
        // them for forceInsert to an ascending order list with generational prefix
        List<String> revIDs = CouchUtils.couchStyleRevisionHistoryToFullRevisionIDs
                (revisionsStart, revisionsIDs);


        // Note attachmentsMetadata map is not used when pullAttachmentsInline=false so call with
        // null. See com.cloudant.sync.internal.documentstore.callables.ForceInsertCallable.call()
        // for reference.
        forceInsert(Collections.singletonList(new ForceInsertItem(internalRev,
                revIDs, null, preparedAttachments, false)));
    }
}
