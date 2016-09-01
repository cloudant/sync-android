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

import com.cloudant.android.Base64InputStreamFactory;
import com.cloudant.android.ContentValues;
import com.cloudant.sync.datastore.callables.GetAllDocumentIdsCallable;
import com.cloudant.sync.datastore.callables.GetPossibleAncestorRevisionIdsCallable;
import com.cloudant.sync.datastore.callables.InsertRevisionCallable;
import com.cloudant.sync.datastore.callables.PickWinningRevisionCallable;
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
import com.cloudant.sync.util.CouchUtils;
import com.cloudant.sync.util.DatabaseUtils;
import com.cloudant.sync.util.JSONUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import org.apache.commons.io.FilenameUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @api_private
 */
public class DatastoreImpl implements Datastore {

    private static final String LOG_TAG = "BasicDatastore";
    private static final Logger logger = Logger.getLogger(DatastoreImpl.class.getCanonicalName());

    private static final String FULL_DOCUMENT_COLS = "docs.docid, docs.doc_id, revid, sequence, json, current, deleted, parent";

    private static final String GET_DOC_NUMERIC_ID =
            "SELECT doc_id from docs WHERE docid=?";

    // get all document columns for current ("winning") revision of a given doc id
    private static final String GET_DOCUMENT_CURRENT_REVISION =
            "SELECT " + FULL_DOCUMENT_COLS + " FROM revs, docs WHERE docs.docid=? AND revs.doc_id=docs.doc_id " +
                    "AND current=1 ORDER BY revid DESC LIMIT 1";

    // get sequence number for current ("winning") revision of a given doc id
    private static final String GET_SEQUENCE_CURRENT_REVISION =
            "SELECT revs.sequence FROM revs, docs WHERE docs.docid=? AND revs.doc_id=docs.doc_id " +
                    "AND current=1 ORDER BY revid DESC LIMIT 1";

    // get all document columns for a given revision and doc id† (see below)
    private static final String GET_DOCUMENT_GIVEN_REVISION =
            "SELECT " + FULL_DOCUMENT_COLS + " FROM revs, docs WHERE docs.docid=? AND revs" +
                    ".doc_id=docs.doc_id AND revid=? ORDER BY revs.sequence LIMIT 1";

    // get sequence number for a given revision and doc id† (see below)
    private static final String GET_SEQUENCE_GIVEN_REVISION =
            "SELECT revs.sequence FROM revs, docs WHERE docs.docid=? AND revs.doc_id=docs.doc_id " +
                    "AND revid=? ORDER BY revs.sequence LIMIT 1";

    public static final String SQL_CHANGE_IDS_SINCE_LIMIT = "SELECT doc_id, max(sequence) FROM revs " +
            "WHERE sequence > ? AND sequence <= ? GROUP BY doc_id ";

    // † N.B. whilst there should only ever be a single result bugs have resulted in duplicate
    // revision IDs in the tree. Whilst it appears that the lowest sequence number is always
    // returned by these queries we use ORDER BY sequence to guarantee that and lock down a
    // behaviour for any future occurrences of duplicate revs in a tree.

    // Limit of parameters (placeholders) one query can have.
    // SQLite has limit on the number of placeholders on a single query, default 999.
    // http://www.sqlite.org/limits.html
    public static final int SQLITE_QUERY_PLACEHOLDERS_LIMIT = 500;

    private final String datastoreName;
    private final EventBus eventBus;

    final String datastoreDir;
    final String extensionsDir;

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

    public DatastoreImpl(String dir, String name) throws SQLException, IOException, DatastoreException {
        this(dir, name, new NullKeyProvider());
    }

    /**
     * Constructor for single thread SQLCipher-based datastore.
     * @param dir The directory where the datastore will be created
     * @param name The user-defined name of the datastore
     * @param provider The key provider object that contains the user-defined SQLCipher key
     * @throws SQLException
     * @throws IOException
     */
    public DatastoreImpl(String dir, String name, KeyProvider provider) throws SQLException, IOException, DatastoreException {
        Preconditions.checkNotNull(dir);
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(provider);

        this.keyProvider = provider;
        this.datastoreDir = dir;
        this.datastoreName = name;
        this.extensionsDir = FilenameUtils.concat(this.datastoreDir, "extensions");
        final String dbFilename = FilenameUtils.concat(this.datastoreDir, DB_FILE_NAME);
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
        queue.updateSchema(new MigrateDatabase100To200(DatastoreConstants.getSchemaVersion200()), 200);
        this.eventBus = new EventBus();

        this.attachmentsDir = this.extensionDataFolder(ATTACHMENTS_EXTENSION_NAME);
        this.attachmentStreamFactory = new AttachmentStreamFactory(this.getKeyProvider());
    }

    @Override
    public String getDatastoreName() {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        return this.datastoreName;
    }

    public KeyProvider getKeyProvider() {
        return this.keyProvider;
    }

    @Override
    public long getLastSequence() {
        Preconditions.checkState(this.isOpen(), "Database is closed");

        try {

            return queue.submit(new SQLCallable<Long>() {
                @Override
                public Long call(SQLDatabase db) throws Exception {
                    String sql = "SELECT MAX(sequence) FROM revs";
                    Cursor cursor = null;
                    long result = 0;
                    try {
                        cursor = db.rawQuery(sql, null);
                        if (cursor.moveToFirst()) {
                            if (cursor.columnType(0) == Cursor.FIELD_TYPE_INTEGER) {
                                result = cursor.getLong(0);
                            } else if (cursor.columnType(0) == Cursor.FIELD_TYPE_NULL) {
                                result = SEQUENCE_NUMBER_START;
                            } else {
                                throw new IllegalStateException("SQLite return an unexpected value.");
                            }
                        }
                    } catch (SQLException e) {
                        logger.log(Level.SEVERE, "Error getting last sequence", e);
                        throw new DatastoreException(e);
                    } finally {
                        DatabaseUtils.closeCursorQuietly(cursor);
                    }
                    return result;
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get last Sequence",e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get last Sequence", e);
            if(e.getCause()!=null){
                if(e.getCause() instanceof IllegalStateException){
                    throw (IllegalStateException) e.getCause();
                }
            }
        }
        return 0;

    }

    @Override
    public int getDocumentCount() {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        try {
            return queue.submit(new SQLCallable<Integer>(){
                @Override
                public Integer call(SQLDatabase db) throws Exception {
                    String sql = "SELECT COUNT(DISTINCT doc_id) FROM revs WHERE current=1 AND deleted=0";
                    Cursor cursor = null;
                    int result = 0;
                    try {
                        cursor = db.rawQuery(sql, null);
                        if (cursor.moveToFirst()) {
                            result = cursor.getInt(0);
                        }
                    } catch (SQLException e) {
                        logger.log(Level.SEVERE, "Error getting document count", e);
                        throw new DatastoreException(e);
                    } finally {
                        DatabaseUtils.closeCursorQuietly(cursor);
                    }
                    return result;
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get document count",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get document count", e);
        }
        return 0;

    }

    @Override
    public boolean containsDocument(String docId, String revId) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        try {
            return getDocument(docId, revId) != null;
        } catch (DocumentNotFoundException e) {
            return false;
        }
    }

    @Override
    public boolean containsDocument(String docId) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        try {
            return getDocument(docId) != null;
        } catch (DocumentNotFoundException e) {
            return false;
        }
    }

    @Override
    public DocumentRevision getDocument(String id) throws DocumentNotFoundException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        return getDocument(id, null);
    }

    private long getSequenceInQueue(SQLDatabase db, String id, String rev)
            throws DatastoreException {
        Cursor cursor = null;
        try {
            String[] args = (rev == null) ? new String[]{id} : new String[]{id, rev};
            String sql = (rev == null) ? GET_SEQUENCE_CURRENT_REVISION : GET_SEQUENCE_GIVEN_REVISION;
            cursor = db.rawQuery(sql, args);
            if (cursor.moveToFirst()) {
                long sequence = cursor.getLong(0);
                return sequence;
            } else {
                return -1;
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE,"Error sequence with id: " + id + "and rev " + rev,e);
            throw new DatastoreException(String.format("Could not find sequence with id %s at revision %s",id,rev),e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    private long getNumericIdInQueue(SQLDatabase db, String id)
            throws AttachmentException, DocumentNotFoundException, DatastoreException {
        Cursor cursor = null;
        try {
            String sql = GET_DOC_NUMERIC_ID;
            cursor = db.rawQuery(sql, new String[]{id});
            if (cursor.moveToFirst()) {
                long sequence = cursor.getLong(0);
                return sequence;
            } else {
                return -1;
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE,"Error sequence with id: " + id);
            throw new DatastoreException(String.format("Could not find sequence with id %s",id),e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    /**
     * Gets a document with the specified ID at the specified revision.
     * @param db The database from which to load the document
     * @param id The id of the document to be loaded
     * @param rev The revision of the document to load
     * @return The loaded document revision.
     * @throws AttachmentException If an error occurred loading the document's attachment
     * @throws DocumentNotFoundException If the document was not found.
     */
    private DocumentRevision getDocumentInQueue(SQLDatabase db, String id, String rev)
            throws AttachmentException, DocumentNotFoundException, DatastoreException {
        Cursor cursor = null;
        try {
            String[] args = (rev == null) ? new String[]{id} : new String[]{id, rev};
            String sql = (rev == null) ? GET_DOCUMENT_CURRENT_REVISION : GET_DOCUMENT_GIVEN_REVISION;
            cursor = db.rawQuery(sql, args);
            if (cursor.moveToFirst()) {
                long sequence = cursor.getLong(3);
                List<? extends Attachment> atts = AttachmentManager.attachmentsForRevision(db,
                        this.attachmentsDir, this.attachmentStreamFactory, sequence);
                return getFullRevisionFromCurrentCursor(cursor, atts);
            } else {
                throw new DocumentNotFoundException(id,rev);
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE,"Error getting document with id: " + id + "and rev " + rev,e);
            throw new DatastoreException(String.format("Could not find document with id %s at revision %s",id,rev),e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    @Override
    public DocumentRevision getDocument(final String id, final String rev) throws DocumentNotFoundException{
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "DocumentRevisionTree id cannot " +
                "be empty");

        try {
            return queue.submit(new SQLCallable<DocumentRevision>(){
                @Override
                public DocumentRevision call(SQLDatabase db) throws Exception {
                    return getDocumentInQueue(db, id, rev);
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get document",e);
        } catch (ExecutionException e) {
            throw new DocumentNotFoundException(e);
        }
        return null;
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
            return queue.submit(new SQLCallable<DocumentRevisionTree>() {
                @Override
                public DocumentRevisionTree call(SQLDatabase db) throws Exception {

                    return getAllRevisionsOfDocumentInQueue(db, docId);
                }

            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Failed to get all revisions of document", e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get all revisions of document", e);
        }
        return null;
    }

    private DocumentRevisionTree getAllRevisionsOfDocumentInQueue(SQLDatabase db, String docId)
            throws DocumentNotFoundException, AttachmentException, DatastoreException {
        String sql = "SELECT " + FULL_DOCUMENT_COLS + " FROM revs, docs " +
                "WHERE docs.docid=? AND revs.doc_id = docs.doc_id ORDER BY sequence ASC";

        String[] args = {docId};
        Cursor cursor = null;

        try {
            DocumentRevisionTree tree = new DocumentRevisionTree();
            cursor = db.rawQuery(sql, args);
            while (cursor.moveToNext()) {
                long sequence = cursor.getLong(3);
                List<? extends Attachment> atts = AttachmentManager.attachmentsForRevision(db,
                        this.attachmentsDir, this.attachmentStreamFactory, sequence);
                DocumentRevision rev = getFullRevisionFromCurrentCursor(cursor, atts);
                logger.finer("Rev: " + rev);
                tree.add(rev);
            }
            return tree;
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error getting all revisions of document", e);
            throw new DatastoreException("DocumentRevisionTree not found with id: " + docId, e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }

    }


    @Override
    public Changes changes(long since, final int limit) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(limit > 0, "Limit must be positive number");
        final long verifiedSince = since >= 0 ? since : 0;

        try {
            return queue.submit(new SQLCallable<Changes>() {
                @Override
                public Changes call(SQLDatabase db) throws Exception {
                    String[] args = {Long.toString(verifiedSince), Long.toString(verifiedSince + limit)};
                    Cursor cursor = null;
                    try {
                        Long lastSequence = verifiedSince;
                        List<Long> ids = new ArrayList<Long>();
                        cursor = db.rawQuery(SQL_CHANGE_IDS_SINCE_LIMIT, args);
                        while (cursor.moveToNext()) {
                            ids.add(cursor.getLong(0));
                            lastSequence = Math.max(lastSequence, cursor.getLong(1));
                        }
                        List<DocumentRevision> results = getDocumentsWithInternalIdsInQueue(db, ids);
                        if(results.size() != ids.size()) {
                            throw new IllegalStateException(String.format(Locale.ENGLISH,
                                    "The number of documents does not match number of ids, " +
                                    "something must be wrong here. Number of IDs: %s, number of documents: %s",
                                    ids.size(),
                                    results.size()
                                    ));
                        }

                        return new Changes(lastSequence, results);
                    } catch (SQLException e) {
                        throw new IllegalStateException("Error querying all changes since: " + verifiedSince + ", limit: " + limit, e);
                    } finally {
                        DatabaseUtils.closeCursorQuietly(cursor);
                    }
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Failed to get changes",e);
        } catch (ExecutionException e) {
           logger.log(Level.SEVERE, "Failed to get changes",e);
                if(e.getCause() instanceof IllegalStateException) {
                    throw new IllegalStateException("Failed to get changes, SQLite version: "+queue.getSQLiteVersion(),e);
                }
        }

        return null;

    }

    /**
     * Get list of documents for given list of numeric ids. The result list is ordered by sequence number,
     * and only the current revisions are returned.
     *
     * @param docIds given list of internal ids
     * @return list of documents ordered by sequence number
     */
    List<DocumentRevision> getDocumentsWithInternalIds(final List<Long> docIds) {
        Preconditions.checkNotNull(docIds, "Input document internal id list cannot be null");

        try {
            return queue.submit(new SQLCallable<List<DocumentRevision>>() {
                @Override
                public List<DocumentRevision> call(SQLDatabase db) throws Exception {
                    return getDocumentsWithInternalIdsInQueue(db, docIds);
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Failed to get documents using internal ids",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get documents using internal ids",e);
        }
        return null;


    }

    private List<DocumentRevision> getDocumentsWithInternalIdsInQueue(SQLDatabase db,
                                                                           final List<Long> docIds)
            throws AttachmentException, DocumentNotFoundException, DocumentException, DatastoreException {

        if(docIds.size() == 0) {
            return Collections.emptyList();
        }

        final String GET_DOCUMENTS_BY_INTERNAL_IDS = "SELECT " + FULL_DOCUMENT_COLS + " FROM revs, docs " +
                "WHERE revs.doc_id IN ( %s ) AND current = 1 AND docs.doc_id = revs.doc_id";

        // Split into batches because SQLite has a limit on the number
        // of placeholders we can use in a single query. 999 is the default
        // value, but it can be lower. It's hard to find this out from Java,
        // so we use a value much lower.
        List<DocumentRevision> result = new ArrayList<DocumentRevision>(docIds.size());

        List<List<Long>> batches = Lists.partition(docIds, SQLITE_QUERY_PLACEHOLDERS_LIMIT);
        for (List<Long> batch : batches) {
            String sql = String.format(
                    GET_DOCUMENTS_BY_INTERNAL_IDS,
                    DatabaseUtils.makePlaceholders(batch.size())
            );
            String[] args = new String[batch.size()];
            for(int i = 0 ; i < batch.size() ; i ++) {
                args[i] = Long.toString(batch.get(i));
            }
            result.addAll(getRevisionsFromRawQuery(db,sql, args));
        }

        // Contract is to sort by sequence number, which we need to do
        // outside the sqlDb as we're batching requests.
        Collections.sort(result, new Comparator<DocumentRevision>() {
            @Override
            public int compare(DocumentRevision documentRevision, DocumentRevision
                    documentRevision2) {
                long a = documentRevision.getSequence();
                long b = documentRevision2.getSequence();
                return (int) (a - b);
            }
        });

        return result;
    }

    @Override
    public List<DocumentRevision> getAllDocuments(final int offset,final  int limit,final boolean descending) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be >= 0");
        }
        try {
            return queue.submit(new SQLCallable<List<DocumentRevision>>(){
                @Override
                public List<DocumentRevision> call(SQLDatabase db) throws Exception {
                    // Generate the SELECT statement, based on the options:
                    String sql = String.format("SELECT " + FULL_DOCUMENT_COLS + " FROM revs, docs " +
                                    "WHERE deleted = 0 AND current = 1 AND docs.doc_id = revs.doc_id " +
                                    "ORDER BY docs.doc_id %1$s, revid DESC LIMIT %2$s OFFSET %3$s ",
                            (descending ? "DESC" : "ASC"), limit, offset);
                    return getRevisionsFromRawQuery(db, sql, new String[]{});
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get all documents",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE,"Failed to get all documents",e);
        }
        return null;

    }

    @Override
    public List<String> getAllDocumentIds() {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        try {
            return queue.submit(new GetAllDocumentIdsCallable()).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get all document ids",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE,"Failed to get all document ids",e);
        }
        return null;
    }

    @Override
    public List<DocumentRevision> getDocumentsWithIds(final List<String> docIds)  throws
            DocumentException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkNotNull(docIds, "Input document id list cannot be null");
        try {
            return queue.submit(new SQLCallable<List<DocumentRevision>>(){
                @Override
                public List<DocumentRevision> call(SQLDatabase db) throws Exception {
                    String sql = String.format("SELECT " + FULL_DOCUMENT_COLS + " FROM revs, docs" +
                            " WHERE docid IN ( %1$s ) AND current = 1 AND docs.doc_id = revs.doc_id " +
                            " ORDER BY docs.doc_id ", DatabaseUtils.makePlaceholders(docIds.size()));
                    String[] args = docIds.toArray(new String[docIds.size()]);
                    List<DocumentRevision> docs = getRevisionsFromRawQuery(db,sql, args);
                    // Sort in memory since seems not able to sort them using SQL
                    return sortDocumentsAccordingToIdList(docIds, docs);
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Failed to get documents with ids", e);
            throw new DocumentException("Failed to get documents with ids", e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get documents with ids", e);
            throw new DocumentException("Failed to get documents with ids", e);
        }
    }

    public List<String> getPossibleAncestorRevisionIDs(final String docId,
                                                       final String revId,
                                                       final int limit) {
        try {
            return queue.submit(new GetPossibleAncestorRevisionIdsCallable(docId, revId, limit)).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
           throw new RuntimeException(e);
        }
    }

    private List<DocumentRevision> sortDocumentsAccordingToIdList(List<String> docIds,
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

    private Map<String, DocumentRevision> putDocsIntoMap(List<DocumentRevision> docs) {
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
        Preconditions.checkState(this.isOpen(), "Database is closed");
        try {
            return queue.submit(new SQLCallable<LocalDocument>(){
                @Override
                public LocalDocument call(SQLDatabase db) throws Exception {
                    return doGetLocalDocument(db, docId);
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get local document",e);
        } catch (ExecutionException e) {
            throw new DocumentNotFoundException(e);
        }
        return null;
    }

    private DocumentRevision createDocumentBody(SQLDatabase db, String docId, final DocumentBody body)
            throws AttachmentException, ConflictException, DatastoreException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        CouchUtils.validateDocumentId(docId);
        Preconditions.checkNotNull(body, "Input document body cannot be null");
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
            potentialParent = this.getDocumentInQueue(db, docId, null);
        } catch (DocumentNotFoundException e) {
            //this is an expected exception, it just means we are
            // resurrecting the document
        }

        if (potentialParent != null) {
            if (!potentialParent.isDeleted()) {
                // current winner not deleted, can't insert
                throw new ConflictException(String.format("Cannot create doc, document with id %s already exists "
                        , docId));
            }
            // if we got here, parent rev was deleted
            this.setCurrent(db, potentialParent.getSequence(), false);
            callable.revId = CouchUtils.generateNextRevisionId(potentialParent.getRevision());
            callable.docNumericId = potentialParent.getInternalNumericId();
            callable.parentSequence = potentialParent.getSequence();
        } else {
            // otherwise we are doing a normal create document
            long docNumericId = insertDocumentID(db,docId);
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
            DocumentRevision doc =  getDocumentInQueue(db, docId, callable.revId);
            logger.finer("New document created: " + doc.toString());
            return doc;
        } catch (DocumentNotFoundException e){
            throw new RuntimeException(String.format("Could not get document we just inserted " +
                    "(id: %s); this should not happen, please file an issue with as much detail " +
                    "as possible.", docId), e);
        }

    }

    private void validateDBBody(DocumentBody body) {
        for(String name : body.asMap().keySet()) {
            if(name.startsWith("_")) {
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
    public LocalDocument insertLocalDocument(final String docId, final DocumentBody body) throws DocumentException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        CouchUtils.validateDocumentId(docId);
        Preconditions.checkNotNull(body, "Input document body cannot be null");
        try {
            return queue.submitTransaction(new SQLCallable<LocalDocument>() {
                @Override
                public LocalDocument call(SQLDatabase db) throws Exception {
                    ContentValues values = new ContentValues();
                    values.put("docid", docId);
                    values.put("json", body.asBytes());

                    long rowId = db.insertWithOnConflict("localdocs", values, SQLDatabase
                            .CONFLICT_REPLACE);
                    if (rowId < 0) {
                        throw new DocumentException("Failed to insert local document");
                    } else {
                        logger.finer(String.format("Local doc inserted: %d , %s", rowId, docId));
                    }

                    return doGetLocalDocument(db, docId);
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Failed to insert local document", e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE,"Failed to insert local document",e);
            throw new DocumentException("Cannot insert local document",e);
        }

        return null;
    }

    private DocumentRevision updateDocumentBody(SQLDatabase db, String docId,
                                                     String prevRevId,
                                                     final DocumentBody body)
            throws ConflictException, AttachmentException, DocumentNotFoundException, DatastoreException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(docId),
                "Input document id cannot be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(prevRevId),
                "Input previous revision id cannot be empty");
        Preconditions.checkNotNull(body, "Input document body cannot be null");

        this.validateDBBody(body);
        CouchUtils.validateRevisionId(prevRevId);

        DocumentRevision preRevision = this.getDocumentInQueue(db, docId, prevRevId);

        if (!preRevision.isCurrent()) {
            throw new ConflictException("Revision to be updated is not current revision.");
        }

        this.setCurrent(db, preRevision.getSequence(), false);
        String newRevisionId = this.insertNewWinnerRevision(db, body, preRevision);
        return this.getDocumentInQueue(db, preRevision.getId(), newRevisionId);
    }

    private DocumentRevision deleteDocumentInQueue(SQLDatabase db, final String docId,
                                                        final String prevRevId) throws ConflictException, DocumentNotFoundException, DatastoreException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(docId),
                "Input document id cannot be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(prevRevId),
                "Input previous revision id cannot be empty");

        CouchUtils.validateRevisionId(prevRevId);

        DocumentRevision prevRevision;
        try {
            prevRevision = getDocumentInQueue(db, docId, prevRevId);
        }  catch (AttachmentException e){
            throw new DocumentNotFoundException(e);
        }


        DocumentRevisionTree revisionTree;
        try {
            revisionTree = getAllRevisionsOfDocumentInQueue(db, docId);
        } catch(AttachmentException e){
            // we can't get load the document, due to an attachment error,
            // throw an exception saying we couldn't find the document.
            throw new DocumentNotFoundException(e);
        }

         if (!revisionTree.leafRevisionIds().contains(prevRevId)) {
            throw new ConflictException("Document has newer revisions than the revision " +
                    "passed to delete; get the newest revision of the document and try again.");
        }

        if (prevRevision.isDeleted()) {
            throw new DocumentNotFoundException("Previous Revision is already deleted");
        }
        setCurrent(db, prevRevision.getSequence(), false);
        String newRevisionId = CouchUtils.generateNextRevisionId(prevRevision.getRevision());
        // Previous revision to be deleted could be winner revision ("current" == true),
        // or a non-winner leaf revision ("current" == false), the new inserted
        // revision must have the same flag as it previous revision.
        // Deletion of non-winner leaf revision is mainly used when resolving
        // conflicts.
        InsertRevisionCallable callable = new InsertRevisionCallable();
        callable.docNumericId = prevRevision.getInternalNumericId();
        callable.revId = newRevisionId;
        callable.parentSequence = prevRevision.getSequence();
        callable.deleted = true;
        callable.current = prevRevision.isCurrent();
        callable.data = JSONUtils.emptyJSONObjectAsBytes();
        callable.available = false;
        callable.call(db);


        try {
            //get the deleted document revision to return to the user
            return getDocumentInQueue(db, prevRevision.getId(), newRevisionId);
        } catch (AttachmentException e){
            //throw document not found since we failed to load the document
            throw new DocumentNotFoundException(e);
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
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(docId),
                "Input document id cannot be empty");

        try {
            queue.submit(new SQLCallable<Object>() {
                @Override
                public Object call(SQLDatabase db) throws Exception {
                    String[] whereArgs = {docId};
                    int rowsDeleted = db.delete("localdocs", "docid=? ", whereArgs);
                    if (rowsDeleted == 0) {
                        throw new DocumentNotFoundException(docId, (String) null);
                    }
                    return null;
                }
            }).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
           throw new DocumentNotFoundException(docId,null,e);
        }

    }

    private long insertDocumentID(SQLDatabase db, String docId) {
        ContentValues args = new ContentValues();
        args.put("docid", docId);
        return db.insert("docs", args);
    }

    private long insertStubRevision(SQLDatabase db, long docNumericId, String revId, long parentSequence) throws AttachmentException {
        // don't copy attachments
        InsertRevisionCallable callable = new InsertRevisionCallable();
        callable.docNumericId = docNumericId;
        callable.revId = revId;
        callable.parentSequence = parentSequence;
        callable.deleted = false;
        callable.current = false;
        callable.data = JSONUtils.emptyJSONObjectAsBytes();
        callable.available = false;
        return callable.call(db);
    }

    /**
     * <p>Returns the current winning revision of a local document.</p>
     *
     * @param docId id of the local document
     * @return {@code LocalDocument} of the document
     * @throws DocumentNotFoundException if the document ID doesn't exist
     */
    private LocalDocument doGetLocalDocument(SQLDatabase db, String docId)
            throws  DocumentNotFoundException, DocumentException, DatastoreException {
        assert !Strings.isNullOrEmpty(docId);
        Cursor cursor = null;
        try {
            String[] args = {docId};
            cursor = db.rawQuery("SELECT json FROM localdocs WHERE docid=?", args);
            if (cursor.moveToFirst()) {
                byte[] json = cursor.getBlob(0);

                return new LocalDocument(docId, DocumentBodyImpl.bodyWith(json));
            } else {
                throw new DocumentNotFoundException(String.format("No local document found with id: %s", docId));
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, String.format("Error getting local document with id: %s",docId),e);
            throw new DatastoreException("Error getting local document with id: " + docId, e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    private List<DocumentRevision> getRevisionsFromRawQuery(SQLDatabase db, String sql, String[] args)
            throws DocumentNotFoundException, AttachmentException, DocumentException, DatastoreException {
        List<DocumentRevision> result = new ArrayList<DocumentRevision>();
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, args);
            while (cursor.moveToNext()) {
                long sequence = cursor.getLong(3);
                List<? extends Attachment> atts = AttachmentManager.attachmentsForRevision(db,
                        this.attachmentsDir, this.attachmentStreamFactory, sequence);
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
        Preconditions.checkState(this.isOpen(), "Database is closed");
        try {
            return queue.submit(new SQLCallable < String > () {
                @Override
                public String call(SQLDatabase db) throws Exception {
                    Cursor cursor = null;
                    try {
                        cursor = db.rawQuery("SELECT value FROM info WHERE key='publicUUID'", null);
                        if (cursor.moveToFirst()) {
                            return "touchdb_" + cursor.getString(0);
                        } else {
                            throw new IllegalStateException("Error querying PublicUUID, " +
                                    "it is probably because the sqlDatabase is not probably initialized.");
                        }
                    } finally {
                        DatabaseUtils.closeCursorQuietly(cursor);
                    }
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get public ID",e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get public ID", e);
            throw new DatastoreException("Failed to get public ID",e);
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
                            final Map<String[],List<PreparedAttachment>>preparedAttachments,
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
    public void forceInsert(final List<ForceInsertItem> items) throws DocumentException {
        Preconditions.checkState(this.isOpen(), "Database is closed");

        for (ForceInsertItem item : items) {
            Preconditions.checkNotNull(item.rev, "Input document revision cannot be null");
            Preconditions.checkNotNull(item.revisionHistory, "Input revision history must not be null");
            Preconditions.checkArgument(item.revisionHistory.size() > 0, "Input revision history must not be empty");

            Preconditions.checkArgument(checkCurrentRevisionIsInRevisionHistory(item.rev, item.revisionHistory),
                    "Current revision must exist in revision history.");
            Preconditions.checkArgument(checkRevisionIsInCorrectOrder(item.revisionHistory),
                    "Revision history must be in right order.");
            CouchUtils.validateDocumentId(item.rev.getId());
            CouchUtils.validateRevisionId(item.rev.getRevision());
        }

        // for raising events after completing database transaction
        final List<DocumentModified> events = new LinkedList<DocumentModified>();

        try {
            queue.submitTransaction(new SQLCallable<Object>() {
                @Override
                public Object call(SQLDatabase db) throws Exception {
                    for (ForceInsertItem item : items) {

                        logger.finer("forceInsert(): " + item.rev.toString());

                        DocumentCreated documentCreated = null;
                        DocumentUpdated documentUpdated = null;

                        boolean ok = true;

                        long docNumericId = getNumericIdInQueue(db, item.rev.getId());
                        long seq = 0;

                        if (docNumericId != -1) {
                            seq = doForceInsertExistingDocumentWithHistory(db, item.rev, docNumericId, item.revisionHistory,
                                    item.attachments);
                            item.rev.initialiseSequence(seq);
                            // TODO fetch the parent doc?
                            documentUpdated = new DocumentUpdated(null, item.rev);
                        } else {
                            seq = doForceInsertNewDocumentWithHistory(db, item.rev, item.revisionHistory);
                            item.rev.initialiseSequence(seq);
                            documentCreated = new DocumentCreated(item.rev);
                        }

                        // now deal with any attachments
                        if (item.pullAttachmentsInline) {
                            if (item.attachments != null) {
                                for (String att : item.attachments.keySet()) {
                                    Map attachmentMetadata = (Map) item.attachments.get(att);
                                    Boolean stub = (Boolean) attachmentMetadata.get("stub");

                                    if (stub != null && stub) {
                                    // stubs get copied forward at the end of
                                    // insertDocumentHistoryIntoExistingTree - nothing to do here
                                    continue;
                                }
                                String data = (String) attachmentMetadata.get("data");
                                String type = (String) attachmentMetadata.get("content_type");
                                InputStream is = Base64InputStreamFactory.get(new
                                        ByteArrayInputStream(data.getBytes("UTF-8")));
                                // inline attachments are automatically decompressed,
                                // so we don't have to worry about that
                                UnsavedStreamAttachment usa = new UnsavedStreamAttachment(is,
                                        att, type);
                                    try {
                                        PreparedAttachment pa = AttachmentManager.prepareAttachment(
                                                attachmentsDir, attachmentStreamFactory, usa);
                                        AttachmentManager.addAttachment(db, attachmentsDir, item.rev, pa);
                                    } catch (Exception e) {
                                        logger.log(Level.SEVERE, "There was a problem adding the " +
                                                        "attachment "
                                                        + usa + "to the datastore for document " + item.rev,
                                                e);
                                        throw e;
                                    }
                            }
                        }
                        } else {

                            try {
                                if (item.preparedAttachments != null) {
                                    for (String[] key : item.preparedAttachments.keySet()) {
                                        String id = key[0];
                                        String rev = key[1];
                                        try {
                                            DocumentRevision doc = getDocumentInQueue(db, id, rev);
                                            if (doc != null) {
                                                AttachmentManager.addAttachmentsToRevision(db,
                                                        attachmentsDir, doc, item.preparedAttachments.get(key));
                                            }
                                        } catch (DocumentNotFoundException e) {
                                            //safe to continue, previously getDocumentInQueue could return
                                            // null and this was deemed safe and expected behaviour
                                            // DocumentNotFoundException is thrown instead of returning
                                        // null now.
                                        continue;
                                    }
                                }
                            }
                        } catch (Exception e) {
                            logger.log(Level.SEVERE, "There was a problem adding an " +
                                    "attachment to the datastore", e);
                            throw e;
                        }


                        }
                        if (ok) {
                            logger.log(Level.FINER, "Inserted revision: %s", item.rev);
                            if (documentCreated != null) {
                                events.add(documentCreated);
                            } else if (documentUpdated != null) {
                                events.add(documentUpdated);
                            }
                        }
                    }
                    return null;
                }
            }).get();

            // if we got here, everything got written to the database successfully
            // now raise any events we stored up
            for(DocumentModified event : events) {
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
     * @see DatastoreImpl#forceInsert(DocumentRevision, java.util.List,java.util.Map, java.util.Map, boolean)
     * @throws DocumentException if there was an error inserting the revision into the database
     */
    public void forceInsert(DocumentRevision rev, String... revisionHistory) throws DocumentException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
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

    private boolean checkCurrentRevisionIsInRevisionHistory(DocumentRevision rev, List<String> revisionHistory) {
        return revisionHistory.get(revisionHistory.size() - 1).equals(rev.getRevision());
    }

    /**
     *
     * @param newRevision DocumentRevision to insert
     * @param revisions   revision history to insert, it includes all revisions (include the revision of the DocumentRevision
     *                    as well) sorted in ascending order.
     */
    private long doForceInsertExistingDocumentWithHistory(SQLDatabase db,DocumentRevision newRevision,
                                                          long docNumericId,
                                                          List<String> revisions,
                                                          Map<String, Object> attachments)
            throws AttachmentException, DocumentNotFoundException, DatastoreException {
        logger.entering("BasicDatastore",
                "doForceInsertExistingDocumentWithHistory",
                new Object[]{newRevision, revisions, attachments});
        Preconditions.checkNotNull(newRevision, "New document revision must not be null.");
        Preconditions.checkArgument(this.getDocumentInQueue(db, newRevision.getId(), null) != null, "DocumentRevisionTree must exist.");
        Preconditions.checkNotNull(revisions, "Revision history should not be null.");
        Preconditions.checkArgument(revisions.size() > 0, "Revision history should have at least one revision." );

        // do we have a common ancestor?
        long ancestorSequence = getSequenceInQueue(db, newRevision.getId(), revisions.get(0));

        long sequence;

        if(ancestorSequence == -1) {
            sequence = insertDocumentHistoryToNewTree(db,newRevision, revisions, docNumericId);
        } else {
            sequence = insertDocumentHistoryIntoExistingTree(db,newRevision, revisions, docNumericId, attachments);
        }
        return sequence;
    }

    private long insertDocumentHistoryIntoExistingTree(SQLDatabase db, DocumentRevision newRevision, List<String> revisions,
                                                       Long docNumericID,
                                                       Map<String, Object> attachments)
            throws AttachmentException, DocumentNotFoundException, DatastoreException {

        // get info about previous "winning" rev
        long previousLeafSeq = getSequenceInQueue(db, newRevision.getId(), null);
        Preconditions.checkArgument(previousLeafSeq > 0, "Parent revision must exist");

        // Insert the new stub revisions, going down the tree
        // at the end of the loop, parentSeq will be the parent of our doc to insert
        long parentSeq = 0L;
        for (int i=0; i<revisions.size()-1; i++) {
            String revId = revisions.get(i);
            long seq = getSequenceInQueue(db, newRevision.getId(), revId);
            if (seq == -1) {
                seq = insertStubRevision(db,docNumericID, revId, parentSeq);
                this.setCurrent(db, parentSeq, false);
            }
            parentSeq = seq;
        }

        // Insert the new leaf revision
        String newLeafRev = revisions.get(revisions.size() - 1);
        logger.finer("Inserting new revision, id: " + docNumericID + ", rev: " + newLeafRev);
        this.setCurrent(db, parentSeq, false);
        // don't copy over attachments
        InsertRevisionCallable callable = new InsertRevisionCallable();
        callable.docNumericId = docNumericID;
        callable.revId = newLeafRev;
        callable.parentSequence = parentSeq;
        callable.deleted = newRevision.isDeleted();
        callable.current = false; // we'll call pickWinnerOfConflicts to set this if it needs it
        callable.data = newRevision.asBytes();
        callable.available = true;
        long newLeafSeq = callable.call(db);

        pickWinnerOfConflicts(docNumericID);

        // copy stubbed attachments forward from last real revision to this revision
        if (attachments != null) {
            for (Map.Entry<String, Object> att : attachments.entrySet()) {
                Boolean stub = ((Map<String, Boolean>) att.getValue()).get("stub");
                if (stub != null && stub.booleanValue()) {
                    try {
                        AttachmentManager.copyAttachment(db, previousLeafSeq, newLeafSeq, att
                                .getKey());
                    } catch (SQLException sqe) {
                        logger.log(Level.SEVERE, "Error copying stubbed attachments", sqe);
                        throw new DatastoreException("Error copying stubbed attachments", sqe);
                    }
                }
            }
        }

        return newLeafSeq;
    }

    private long insertDocumentHistoryToNewTree(SQLDatabase db, DocumentRevision newRevision,
                                                List<String> revisions,
                                                Long docNumericID)
            throws AttachmentException, DocumentNotFoundException, DatastoreException {
        Preconditions.checkArgument(checkCurrentRevisionIsInRevisionHistory(newRevision, revisions),
                "Current revision must exist in revision history.");

        // Adding a brand new tree
        logger.finer("Inserting a brand new tree for an existing document.");
        long parentSequence = 0L;
        for(int i = 0 ; i < revisions.size() - 1 ; i ++) {
            //we copy attachments here so allow the exception to propagate
            parentSequence = insertStubRevision(db,docNumericID, revisions.get(i), parentSequence);
        }
        // don't copy attachments
        String newLeafRev = newRevision.getRevision();
        InsertRevisionCallable callable = new InsertRevisionCallable();
        callable.docNumericId = docNumericID;
        callable.revId = newLeafRev;
        callable.parentSequence = parentSequence;
        callable.deleted = newRevision.isDeleted();
        callable.current = false; // we'll call pickWinnerOfConflicts to set this if it needs it
        callable.data = newRevision.asBytes();
        callable.available = !newRevision.isDeleted();
        long newLeafSeq = callable.call(db);

        pickWinnerOfConflicts(docNumericID);
        return newLeafSeq;
    }

    private void pickWinnerOfConflicts(long docNumericId) throws DatastoreException {
        try {
            queue.submit(new PickWinningRevisionCallable(docNumericId));
        } catch (Exception e) {
            throw new DatastoreException(e);
        }
    }

    /**
     * @param rev        DocumentRevision to insert
     * @param revHistory revision history to insert, it includes all revisions (include the revision of the DocumentRevision
     *                   as well) sorted in ascending order.
     */
    private long doForceInsertNewDocumentWithHistory(SQLDatabase db,
                                                     DocumentRevision rev,
                                                     List<String> revHistory)
            throws AttachmentException{
        logger.entering("DocumentRevision",
                "doForceInsertNewDocumentWithHistory()",
                new Object[]{rev, revHistory});

        long docNumericID = insertDocumentID(db,rev.getId());
        long parentSequence = 0L;
        for (int i = 0; i < revHistory.size() - 1; i++) {
            // Insert stub node
            parentSequence = insertStubRevision(db,docNumericID, revHistory.get(i), parentSequence);
        }
        // Insert the leaf node (don't copy attachments)
        InsertRevisionCallable callable = new InsertRevisionCallable();
        callable.docNumericId = docNumericID;
        callable.revId = revHistory.get(revHistory.size() - 1);
        callable.parentSequence = parentSequence;
        callable.deleted = rev.isDeleted();
        callable.current = true;
        callable.data = rev.getBody().asBytes();
        callable.available = true;
        long sequence = callable.call(db);
        return sequence;
    }

    @Override
    public void compact() {
        try {
            queue.submit(new SQLCallable<Object>() {
                @Override
                public Object call(SQLDatabase db) {
                    logger.finer("Deleting JSON of old revisions...");
                    ContentValues args = new ContentValues();
                    args.put("json", (String) null);
                    int i = db.update("revs", args, "current=0", null);

                    logger.finer("Deleting old attachments...");
                    AttachmentManager.purgeAttachments(db, attachmentsDir);
                    logger.finer("Vacuuming SQLite database...");
                    db.compactDatabase();
                    return null;
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to compact database",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE,"Failed to compact database",e);
        }

    }

    @Override
    public void close() {
        queue.shutdown();
        eventBus.post(new DatabaseClosed(datastoreName));

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
     * @see <a href="http://wiki.apache.org/couchdb/HttpPostRevsDiff">HttpPostRevsDiff documentation</a>
     * @param revisions a Multimap of document id → revision id
     * @return a Map of document id → collection of revision id: the subset of given the document
     * id/revisions that are not stored in the database
     */
    public Map<String, Collection<String>> revsDiff(final Multimap<String, String> revisions) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkNotNull(revisions, "Input revisions must not be null");

        try {
            return queue.submit(new SQLCallable<Map<String,Collection<String>>>(){
                @Override
                public Map<String, Collection<String>> call(SQLDatabase db) throws Exception {
                    Multimap<String, String> missingRevs = ArrayListMultimap.create();
                    // Break the potentially big multimap into small ones so for each map,
                    // a single query can be use to check if the <id, revision> pairs in sqlDb or not
                    List<Multimap<String, String>> batches =
                            multiMapPartitions(revisions, SQLITE_QUERY_PLACEHOLDERS_LIMIT);
                    for(Multimap<String, String> batch : batches) {
                        revsDiffBatch(db,batch);
                        missingRevs.putAll(batch);
                    }
                    return missingRevs.asMap();
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to do revsdiff",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE,"Failed to do revsdiff",e);
        }

        return null;
    }

    List<Multimap<String, String>> multiMapPartitions(
            Multimap<String, String> revisions, int size) {

        List<Multimap<String, String>> partitions = new ArrayList<Multimap<String, String>>();
        Multimap<String, String> current = HashMultimap.create();
        for(Map.Entry<String, String> e : revisions.entries()) {
            current.put(e.getKey(), e.getValue());
            // the query uses below (see revsDiffBatch())
            // `multimap.size() + multimap.keySet().size()` placeholders
            // and SQLite has limit on the number of placeholders on a single query.
            if(current.size() + current.keySet().size() >= size) {
                partitions.add(current);
                current = HashMultimap.create();
            }
        }

        if(current.size() > 0) {
            partitions.add(current);
        }

        return partitions;
    }

    /**
     * Removes revisions present in the datastore from the input map.
     *
     * @param revisions an multimap from document id to set of revisions. The
     *                  map is modified in place for performance consideration.
     */
    void revsDiffBatch(SQLDatabase db,Multimap<String, String> revisions) throws DatastoreException {

        final String sql = String.format(
                "SELECT docs.docid, revs.revid FROM docs, revs " +
                "WHERE docs.doc_id = revs.doc_id AND docs.docid IN (%s) AND revs.revid IN (%s) " +
                "ORDER BY docs.docid",
                DatabaseUtils.makePlaceholders(revisions.keySet().size()),
                DatabaseUtils.makePlaceholders(revisions.size()));

        String[] args = new String[revisions.keySet().size() + revisions.size()];
        String[] keys = revisions.keySet().toArray(new String[revisions.keySet().size()]);
        String[] values = revisions.values().toArray(new String[revisions.size()]);
        System.arraycopy(keys, 0, args, 0, revisions.keySet().size());
        System.arraycopy(values, 0, args, revisions.keySet().size(), revisions.size());

        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, args);
            while (cursor.moveToNext()) {
                String docId = cursor.getString(0);
                String revId = cursor.getString(1);
                revisions.remove(docId, revId);
            }
        } catch (SQLException e) {
           throw new DatastoreException(e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    public String extensionDataFolder(String extensionName) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(extensionName),
                "extension name cannot be null or empty");
        return FilenameUtils.concat(this.extensionsDir, extensionName);
    }

    @Override
    public Iterator<String> getConflictedDocumentIds() {

        // the "SELECT DISTINCT ..." subquery selects all the parent
        // sequence, and so the outer "SELECT ..." practically selects
        // all the leaf nodes. The "GROUP BY" and "HAVING COUNT(*) > 1"
        // make sure only those document with more than one leafs are
        // returned.
        final String sql = "SELECT docs.docid, COUNT(*) FROM docs,revs " +
                "WHERE revs.doc_id = docs.doc_id " +
                "AND deleted = 0 AND revs.sequence NOT IN " +
                "(SELECT DISTINCT parent FROM revs WHERE parent NOT NULL) " +
                "GROUP BY docs.docid HAVING COUNT(*) > 1";

        try {
            return queue.submit(new SQLCallable<Iterator<String>>() {
                @Override
                public Iterator<String> call(SQLDatabase db) throws Exception {

                    List<String> conflicts = new ArrayList<String>();
                    Cursor cursor = null;
                    try {
                        cursor = db.rawQuery(sql, new String[]{});
                        while (cursor.moveToNext()) {
                            String docId = cursor.getString(0);
                            conflicts.add(docId);
                        }
                    }  catch (SQLException e) {
                        logger.log(Level.SEVERE, "Error getting conflicted document: ", e);
                        throw new DatastoreException(e);
                    } finally {
                        DatabaseUtils.closeCursorQuietly(cursor);
                    }
                    return conflicts.iterator();
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get conflicted document Ids",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get conflicted document Ids", e);
        }
        return null;

    }

    @Override
    public void resolveConflictsForDocument(final String docId, final ConflictResolver resolver)
            throws ConflictException {

        // before starting the tx, get the 'new winner' and see if we need to prepare its attachments


        try {
            queue.submitTransaction(new SQLCallable<Object>() {
                @Override
                public Object call(SQLDatabase db) throws Exception {
                    DocumentRevisionTree docTree = getAllRevisionsOfDocumentInQueue(db, docId);
                    if(!docTree.hasConflicts()) {
                        return null;
                    }
                    DocumentRevision newWinner = null;
                    try {
                        newWinner = resolver.resolve(docId, docTree.leafRevisions(true));
                    } catch (Exception e) {
                        logger.log(Level.SEVERE, "Exception when calling ConflictResolver", e);
                    }
                    if(newWinner == null) {
                        // resolve() threw an exception or returned null, exit early
                        return null;
                    }

                        String revIdKeep = newWinner.getRevision();
                        if (revIdKeep == null) {
                            throw new IllegalArgumentException("Winning revision must have a revision id");
                        }

                        for(DocumentRevision revision : docTree.leafRevisions()) {
                            if(revision.getRevision().equals(revIdKeep)) {
                                // this is the one we want to keep, set it to current
                                setCurrent(db, revision.getSequence(), true);
                            } else {
                                if (revision.isDeleted()) {
                                    // if it is deleted, just make it non-current
                                    setCurrent(db, revision.getSequence(), false);
                                } else {
                                    // if it's not deleted, deleted and make it non-current
                                    DocumentRevision deleted = deleteDocumentInQueue(db,
                                            revision.getId(), revision.getRevision());
                                    setCurrent(db, deleted.getSequence(), false);
                                }
                            }
                        }

                        // if this is a new or modified revision: graft the new revision on
                        if (newWinner.bodyModified || (newWinner.attachments != null && newWinner.attachments.hasChanged())) {

                            // We need to work out which of the attachments for the revision are ones
                            // we can copy over because they exist in the attachment store already and
                            // which are new, that we need to prepare for insertion.
                            Collection<Attachment> attachments = newWinner.getAttachments() != null ? newWinner.getAttachments().values() : new ArrayList<Attachment>();
                            final List<PreparedAttachment> preparedNewAttachments =
                                    AttachmentManager.prepareAttachments(attachmentsDir,
                                            attachmentStreamFactory,
                                            AttachmentManager.findNewAttachments(attachments));
                            final List<SavedAttachment> existingAttachments =
                                    AttachmentManager.findExistingAttachments(attachments);

                            updateDocumentFromRevision(db, newWinner,
                                    preparedNewAttachments,
                                    existingAttachments);
                        }



                    return null;
                }
            }).get();
        } catch (InterruptedException e) {
           logger.log(Level.SEVERE, "Failed to resolve conflicts", e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to resolve Conflicts",e);
            if(e.getCause() !=null){
                if(e.getCause() instanceof  IllegalArgumentException){
                    throw (IllegalArgumentException)e.getCause();
                }
            }
        }

    }

    private String insertNewWinnerRevision(SQLDatabase db,DocumentBody newWinner,
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
        callable.call(db);

        return newRevisionId;
    }

    /**
     * <p>
     *     Set the {@code current} field in the revs table to true or false.
     * </p>
     * <p>
     *     The {@code current} field is used to track the "current" or "winning" revision in the
     *     case of conflicted document trees. This is updated according to the standard couch
     *     algorithm.
     * </p>
     * @param db             Database instance containing revision to modify
     * @param sequence       Sequence number of revision
     * @param valueOfCurrent New value of {@code current} (true/false)
     *
     * @see #pickWinnerOfConflicts(long)
     */
    private void setCurrent(SQLDatabase db, long sequence, boolean valueOfCurrent) {
        ContentValues updateContent = new ContentValues();
        updateContent.put("current", valueOfCurrent ? 1 : 0);
        String[] whereArgs = new String[]{String.valueOf(sequence)};
        db.update("revs", updateContent, "sequence=?", whereArgs);
    }

    private static DocumentRevision getFullRevisionFromCurrentCursor(Cursor cursor,
                                                                          List<? extends Attachment> attachments) {
        String docId = cursor.getString(cursor.getColumnIndex("docid"));
        long internalId = cursor.getLong(cursor.getColumnIndex("doc_id"));
        String revId = cursor.getString(cursor.getColumnIndex("revid"));
        long sequence = cursor.getLong(cursor.getColumnIndex("sequence"));
        byte[] json = cursor.getBlob(cursor.getColumnIndex("json"));
        boolean current = cursor.getInt(cursor.getColumnIndex("current")) > 0;
        boolean deleted = cursor.getInt(cursor.getColumnIndex("deleted")) > 0;

        long parent = -1L;
        if(cursor.columnType(cursor.getColumnIndex("parent")) == Cursor.FIELD_TYPE_INTEGER) {
            parent = cursor.getLong(cursor.getColumnIndex("parent"));
        } else if(cursor.columnType(cursor.getColumnIndex("parent")) == Cursor.FIELD_TYPE_NULL) {
        } else {
            throw new RuntimeException("Unexpected type: " + cursor.columnType(cursor.getColumnIndex("parent")));
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
    public PreparedAttachment prepareAttachment(Attachment att, long length, long encodedLength) throws AttachmentException {
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
    public Attachment getAttachment(final String id, final String rev, final String attachmentName) {
        try {
            return queue.submit(new SQLCallable<Attachment>() {
                @Override
                public Attachment call(SQLDatabase db) throws Exception {
                    long sequence = getSequenceInQueue(db, id, rev);
                    return AttachmentManager.getAttachment(db, attachmentsDir,
                            attachmentStreamFactory, sequence, attachmentName);
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get attachment",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE,"Failed to get attachment",e);
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
    public List<? extends Attachment> attachmentsForRevision(final DocumentRevision rev) throws AttachmentException {
        try {
            return queue.submit(new SQLCallable<List<? extends Attachment>>(){

                @Override
                public List<? extends Attachment> call(SQLDatabase db) throws Exception {
                    return AttachmentManager.attachmentsForRevision(db, attachmentsDir,
                            attachmentStreamFactory, rev.getSequence());
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Failed to get attachments for revision");
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get attachments for revision");
            throw new AttachmentException(e);
        }

    }


    @Override
    public EventBus getEventBus() {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        return eventBus;
    }

    @Override
    public DocumentRevision createDocumentFromRevision(final DocumentRevision rev)
            throws DocumentException {
        Preconditions.checkNotNull(rev, "DocumentRevision cannot be null");
        Preconditions.checkState(isOpen(), "Datastore is closed");
        Preconditions.checkArgument(rev.getRevision() == null, "Revision ID must be null for new DocumentRevisions");
        Preconditions.checkArgument(rev.isFullRevision(), "Projected revisions cannot be used to create documents");
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
        Collection<Attachment> attachments = rev.getAttachments() != null ? rev.getAttachments().values() : new ArrayList<Attachment>();
        final List<PreparedAttachment> preparedNewAttachments =
                AttachmentManager.prepareAttachments(attachmentsDir,
                        attachmentStreamFactory,
                        AttachmentManager.findNewAttachments(attachments));
        final List<SavedAttachment> existingAttachments =
                AttachmentManager.findExistingAttachments(attachments);

        DocumentRevision created = null;
        try {
            created = queue.submitTransaction(new SQLCallable<DocumentRevision>(){
                @Override
                public DocumentRevision call(SQLDatabase db) throws Exception {

                        // Save document with new JSON body, add new attachments and copy over existing attachments
                    DocumentRevision saved = createDocumentBody(db, docId, rev.getBody());
                        AttachmentManager.addAttachmentsToRevision(db, attachmentsDir, saved, preparedNewAttachments);
                        AttachmentManager.copyAttachmentsToRevision(db, existingAttachments, saved);

                        // now re-fetch the revision with updated attachments
                    DocumentRevision updatedWithAttachments = getDocumentInQueue(db,
                                saved.getId(), saved.getRevision());
                        return updatedWithAttachments;
                }
            }).get();
            return created;
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to create document",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE,"Failed to create document",e);
            throw new DocumentException(e);
        }finally {
            if(created != null){
                eventBus.post(new DocumentCreated(created));
            }
        }
        return null;

    }

    @Override
    public DocumentRevision updateDocumentFromRevision(final DocumentRevision rev)
            throws DocumentException {

        Preconditions.checkNotNull(rev, "DocumentRevision cannot be null");
        Preconditions.checkState(isOpen(),"Datastore is closed");
        Preconditions.checkArgument(rev.isFullRevision(), "Projected revisions cannot be used to create documents");

        // We need to work out which of the attachments for the revision are ones
        // we can copy over because they exist in the attachment store already and
        // which are new, that we need to prepare for insertion.
        Collection<Attachment> attachments = rev.getAttachments() != null ? rev.getAttachments().values() : new ArrayList<Attachment>();
        final List<PreparedAttachment> preparedNewAttachments =
                AttachmentManager.prepareAttachments(attachmentsDir,
                        attachmentStreamFactory,
                        AttachmentManager.findNewAttachments(attachments));
        final List<SavedAttachment> existingAttachments =
                AttachmentManager.findExistingAttachments(attachments);

        try {
            DocumentRevision revision = queue.submitTransaction(new SQLCallable<DocumentRevision>(){
                @Override
                public DocumentRevision call(SQLDatabase db) throws Exception {
                    return updateDocumentFromRevision(db,
                            rev, preparedNewAttachments, existingAttachments);
                }
            }).get();

            if (revision != null) {
                eventBus.post(new DocumentUpdated(getDocument(rev.getId(), rev.getRevision()),revision));
            }

            return revision;
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Failed to update document", e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to updated document", e);
            throw new DocumentException(e);
        }

    }

    private DocumentRevision updateDocumentFromRevision(SQLDatabase db, DocumentRevision rev,
                                                             List<PreparedAttachment> preparedNewAttachments,
                                                             List<SavedAttachment> existingAttachments)
            throws ConflictException, AttachmentException, DocumentNotFoundException, DatastoreException {
        Preconditions.checkNotNull(rev, "DocumentRevision cannot be null");

        DocumentRevision updated = updateDocumentBody(db, rev.getId(), rev.getRevision(), rev.getBody());
            AttachmentManager.addAttachmentsToRevision(db, attachmentsDir, updated, preparedNewAttachments);

            AttachmentManager.copyAttachmentsToRevision(db, existingAttachments, updated);
        
            // now re-fetch the revision with updated attachments
        DocumentRevision updatedWithAttachments = this.getDocumentInQueue(db, updated.getId(), updated.getRevision());
            return updatedWithAttachments;
    }

    @Override
    public DocumentRevision deleteDocumentFromRevision(final DocumentRevision rev) throws ConflictException {
        Preconditions.checkNotNull(rev, "DocumentRevision cannot be null");
        Preconditions.checkState(isOpen(),"Datastore is closed");

        try {
            DocumentRevision deletedRevision = queue.submitTransaction(new SQLCallable<DocumentRevision>() {
                @Override
                public DocumentRevision call(SQLDatabase db) throws Exception {
                    return deleteDocumentInQueue(db, rev.getId(), rev.getRevision());
                }
            }).get();


            if (deletedRevision != null) {
                eventBus.post(new DocumentDeleted(rev,deletedRevision));
            }

            return deletedRevision;
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Failed to delete document", e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to delete document", e);
            if(e.getCause() != null){
                if(e.getCause() instanceof ConflictException){
                    throw (ConflictException)e.getCause();
                }
            }
        }

        return null;
    }

    // delete all leaf nodes
    @Override
    public List<DocumentRevision> deleteDocument(final String id)
            throws DocumentException  {
        Preconditions.checkNotNull(id, "id cannot be null");
        // to return

        try {
            return queue.submitTransaction(new SQLCallable<List<DocumentRevision>>(){

                @Override
                public List<DocumentRevision> call(SQLDatabase db) throws Exception {
                    ArrayList<DocumentRevision> deleted = new ArrayList<DocumentRevision>();
                    Cursor cursor = null;
                    // delete all in one tx
                    try {
                        // get revid for each leaf
                        final String sql = "SELECT revs.revid FROM docs,revs " +
                                "WHERE revs.doc_id = docs.doc_id " +
                                "AND docs.docid = ? " +
                                "AND deleted = 0 AND revs.sequence NOT IN " +
                                "(SELECT DISTINCT parent FROM revs WHERE parent NOT NULL) ";

                        cursor = db.rawQuery(sql, new String[]{id});
                        while (cursor.moveToNext()) {
                            String revId = cursor.getString(0);
                            deleted.add(deleteDocumentInQueue(db, id, revId));
                        }
                        return deleted;
                    } catch (SQLException sqe) {
                        throw new DatastoreException("SQLException in deleteDocument, not deleting revisions", sqe);
                    } finally {
                        DatabaseUtils.closeCursorQuietly(cursor);
                    }
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to delete document",e);
        } catch (ExecutionException e) {
            throw new DocumentException("Failed to delete document",e);
        }

        return null;
    }

    <T> Future<T> runOnDbQueue(SQLCallable<T> callable){
        return queue.submit(callable);
    }
}
