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

import com.cloudant.android.Base64InputStreamFactory;
import com.cloudant.sync.datastore.callables.GetAllDocumentIdsCallable;
import com.cloudant.sync.datastore.callables.GetPossibleAncestorRevisionIdsCallable;
import com.cloudant.sync.datastore.encryption.KeyProvider;
import com.cloudant.sync.datastore.encryption.NullKeyProvider;
import com.cloudant.sync.datastore.migrations.SchemaOnlyMigration;
import com.cloudant.sync.datastore.migrations.MigrateDatabase6To100;
import com.cloudant.sync.notifications.DatabaseClosed;
import com.cloudant.sync.notifications.DocumentCreated;
import com.cloudant.sync.notifications.DocumentDeleted;
import com.cloudant.sync.notifications.DocumentUpdated;
import com.cloudant.sync.sqlite.ContentValues;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLDatabaseQueue;
import com.cloudant.sync.sqlite.SQLQueueCallable;
import com.cloudant.sync.util.CouchUtils;
import com.cloudant.sync.util.DatabaseUtils;
import com.cloudant.sync.util.JSONUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.eventbus.EventBus;

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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

class BasicDatastore implements Datastore, DatastoreExtended {

    private static final String LOG_TAG = "BasicDatastore";
    private static final Logger logger = Logger.getLogger(BasicDatastore.class.getCanonicalName());

    private static final String FULL_DOCUMENT_COLS = "docs.docid, docs.doc_id, revid, sequence, json, current, deleted, parent";

    private static final String GET_DOCUMENT_CURRENT_REVISION =
            "SELECT " + FULL_DOCUMENT_COLS + " FROM revs, docs WHERE docs.docid=? AND revs.doc_id=docs.doc_id " +
                    "AND current=1 ORDER BY revid DESC LIMIT 1";

    private static final String GET_DOCUMENT_GIVEN_REVISION =
            "SELECT " + FULL_DOCUMENT_COLS + " FROM revs, docs WHERE docs.docid=? AND revs.doc_id=docs.doc_id " +
                    "AND revid=? LIMIT 1";

    public static final String SQL_CHANGE_IDS_SINCE_LIMIT = "SELECT doc_id, max(sequence) FROM revs " +
            "WHERE sequence > ? AND sequence <= ? GROUP BY doc_id ";

    // Limit of parameters (placeholders) one query can have.
    // SQLite has limit on the number of placeholders on a single query, default 999.
    // http://www.sqlite.org/limits.html
    public static final int SQLITE_QUERY_PLACEHOLDERS_LIMIT = 500;

    private final String datastoreName;
    private final EventBus eventBus;
    private final AttachmentManager attachmentManager;

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

    public BasicDatastore(String dir, String name) throws SQLException, IOException, DatastoreException {
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
    public BasicDatastore(String dir, String name, KeyProvider provider) throws SQLException, IOException, DatastoreException {
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
        if(dbVersion >= 200){
            throw new DatastoreException(String.format("Database version is higher than the version supported " +
                    "by this library, current version %d , highest supported version %d",dbVersion, 99));
        }
        queue.updateSchema(new SchemaOnlyMigration(DatastoreConstants.getSchemaVersion3()), 3);
        queue.updateSchema(new SchemaOnlyMigration(DatastoreConstants.getSchemaVersion4()), 4);
        queue.updateSchema(new SchemaOnlyMigration(DatastoreConstants.getSchemaVersion5()), 5);
        queue.updateSchema(new SchemaOnlyMigration(DatastoreConstants.getSchemaVersion6()), 6);
        queue.updateSchema(new MigrateDatabase6To100(), 100);
        this.eventBus = new EventBus();
        this.attachmentManager = new AttachmentManager(this);
    }

    @Override
    public String getDatastoreName() {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        return this.datastoreName;
    }

    @Override
    public KeyProvider getKeyProvider() {
        return this.keyProvider;
    }

    @Override
    public long getLastSequence() {
        Preconditions.checkState(this.isOpen(), "Database is closed");

        try {

            return queue.submit(new SQLQueueCallable<Long>() {
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
            return queue.submit(new SQLQueueCallable<Integer>(){
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
    public BasicDocumentRevision getDocument(String id) throws DocumentNotFoundException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        return getDocument(id, null);
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
    private BasicDocumentRevision getDocumentInQueue(SQLDatabase db, String id, String rev)
            throws AttachmentException, DocumentNotFoundException, DatastoreException {
        Cursor cursor = null;
        try {
            String[] args = (rev == null) ? new String[]{id} : new String[]{id, rev};
            String sql = (rev == null) ? GET_DOCUMENT_CURRENT_REVISION : GET_DOCUMENT_GIVEN_REVISION;
            cursor = db.rawQuery(sql, args);
            if (cursor.moveToFirst()) {
                long sequence = cursor.getLong(3);
                List<? extends Attachment> atts = attachmentManager.attachmentsForRevision(db,sequence);
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
    public BasicDocumentRevision getDocument(final String id, final String rev) throws DocumentNotFoundException{
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "DocumentRevisionTree id can not " +
                "be empty");

        try {
            return queue.submit(new SQLQueueCallable<BasicDocumentRevision>(){
                @Override
                public BasicDocumentRevision call(SQLDatabase db) throws Exception {
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

    public DocumentRevisionTree getAllRevisionsOfDocument(final String docId) {

        try {
            return queue.submit(new SQLQueueCallable<DocumentRevisionTree>() {
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
                List<? extends Attachment> atts = attachmentManager.attachmentsForRevision(db,sequence);
                BasicDocumentRevision rev = getFullRevisionFromCurrentCursor(cursor, atts);
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
    public Changes changes(long since,final int limit) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(limit > 0, "Limit must be positive number");
        final long verifiedSince = since >= 0 ? since : 0;

        try {
            return queue.submit(new SQLQueueCallable<Changes>() {
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
                        List<BasicDocumentRevision> results = getDocumentsWithInternalIdsInQueue(db, ids);
                        if(results.size() != ids.size()) {
                            throw new IllegalStateException("The number of document does not match number of ids, " +
                                    "something must be wrong here.");
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
            if(e.getCause()!= null){
                if(e.getCause() instanceof IllegalStateException) {
                    throw (IllegalStateException) e.getCause();
                }
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
    List<BasicDocumentRevision> getDocumentsWithInternalIds(final List<Long> docIds) {
        Preconditions.checkNotNull(docIds, "Input document internal id list can not be null");

        try {
            return queue.submit(new SQLQueueCallable<List<BasicDocumentRevision>>() {
                @Override
                public List<BasicDocumentRevision> call(SQLDatabase db) throws Exception {
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

    private List<BasicDocumentRevision> getDocumentsWithInternalIdsInQueue(SQLDatabase db,
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
        List<BasicDocumentRevision> result = new ArrayList<BasicDocumentRevision>(docIds.size());

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
        Collections.sort(result, new Comparator<BasicDocumentRevision>() {
            @Override
            public int compare(BasicDocumentRevision documentRevision, BasicDocumentRevision documentRevision2) {
                long a = documentRevision.getSequence();
                long b = documentRevision2.getSequence();
                return (int)(a - b);
            }
        });

        return result;
    }

    @Override
    public List<BasicDocumentRevision> getAllDocuments(final int offset,final  int limit,final boolean descending) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be >= 0");
        }
        try {
            return queue.submit(new SQLQueueCallable<List<BasicDocumentRevision>>(){
                @Override
                public List<BasicDocumentRevision> call(SQLDatabase db) throws Exception {
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
    public List<BasicDocumentRevision> getDocumentsWithIds(final List<String> docIds) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkNotNull(docIds, "Input document id list can not be null");
        try {
            return queue.submit(new SQLQueueCallable<List<BasicDocumentRevision>>(){
                @Override
                public List<BasicDocumentRevision> call(SQLDatabase db) throws Exception {
                    String sql = String.format("SELECT " + FULL_DOCUMENT_COLS + " FROM revs, docs" +
                            " WHERE docid IN ( %1$s ) AND current = 1 AND docs.doc_id = revs.doc_id " +
                            " ORDER BY docs.doc_id ", DatabaseUtils.makePlaceholders(docIds.size()));
                    String[] args = docIds.toArray(new String[docIds.size()]);
                    List<BasicDocumentRevision> docs = getRevisionsFromRawQuery(db,sql, args);
                    // Sort in memory since seems not able to sort them using SQL
                    return sortDocumentsAccordingToIdList(docIds, docs);
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get documents with ids",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get documents with ids", e);
        }

        return null;

    }

    @Override
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

    private List<BasicDocumentRevision> sortDocumentsAccordingToIdList(List<String> docIds,
                                                                       List<BasicDocumentRevision> docs) {
        Map<String, BasicDocumentRevision> idToDocs = putDocsIntoMap(docs);
        List<BasicDocumentRevision> results = new ArrayList<BasicDocumentRevision>();
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

    private Map<String, BasicDocumentRevision> putDocsIntoMap(List<BasicDocumentRevision> docs) {
        Map<String, BasicDocumentRevision> map = new HashMap<String, BasicDocumentRevision>();
        for (BasicDocumentRevision doc : docs) {
            // id should be unique cross all docs
            assert !map.containsKey(doc.getId());
            map.put(doc.getId(), doc);
        }
        return map;
    }

    @Override
    public LocalDocument getLocalDocument(final String docId) throws DocumentNotFoundException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        try {
            return queue.submit(new SQLQueueCallable<LocalDocument>(){
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

    private BasicDocumentRevision createDocument(SQLDatabase db,String docId, final DocumentBody body)
            throws AttachmentException, ConflictException, DatastoreException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        CouchUtils.validateDocumentId(docId);
        Preconditions.checkNotNull(body, "Input document body can not be null");
        this.validateDBBody(body);

        // check if the docid exists first:

        // if it does exist:
        // * if winning leaf deleted, root the 'created' document there
        // * else raise error
        // if it does not exist:
        // * normal insert logic for a new document

        InsertRevisionOptions options = new InsertRevisionOptions();
        BasicDocumentRevision potentialParent = null;

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
            this.setCurrent(db, potentialParent, false);
            options.revId = CouchUtils.generateNextRevisionId(potentialParent.getRevision());
            options.docNumericId = potentialParent.getInternalNumericId();
            options.parentSequence = potentialParent.getSequence();
        } else {
            // otherwise we are doing a normal create document
            long docNumericId = insertDocumentID(db,docId);
            options.revId = CouchUtils.getFirstRevisionId();
            options.docNumericId = docNumericId;
            options.parentSequence = -1l;
        }
        options.deleted = false;
        options.current = true;
        options.data = body.asBytes();
        options.available = true;
        insertRevision(db, options);

        try {
            BasicDocumentRevision doc =  getDocumentInQueue(db, docId, options.revId);
            logger.finer("New document created: " + doc.toString());
            return doc;
        } catch (DocumentNotFoundException e){
            throw new RuntimeException(String.format("Couldn't get document we just inserted " +
                    "(id: %s); this shouldn't happen, please file an issue with as much detail " +
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

    @Override
    public LocalDocument insertLocalDocument(final String docId, final DocumentBody body) throws DocumentException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        CouchUtils.validateDocumentId(docId);
        Preconditions.checkNotNull(body, "Input document body can not be null");
        try {
            return queue.submitTransaction(new SQLQueueCallable<LocalDocument>() {
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

    private BasicDocumentRevision updateDocument(SQLDatabase db,String docId,
                                                 String prevRevId,
                                                 final DocumentBody body,
                                                 boolean validateBody,
                                                 boolean copyAttachments)
            throws ConflictException, AttachmentException, DocumentNotFoundException, DatastoreException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(docId),
                "Input document id can not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(prevRevId),
                "Input previous revision id can not be empty");
        Preconditions.checkNotNull(body, "Input document body can not be null");
        if (validateBody) {
            this.validateDBBody(body);
        }
        CouchUtils.validateRevisionId(prevRevId);

        BasicDocumentRevision preRevision = this.getDocumentInQueue(db, docId, prevRevId);

        if (!preRevision.isCurrent()) {
            throw new ConflictException("Revision to be updated is not current revision.");
        }

        this.setCurrent(db, preRevision, false);
        String newRevisionId = this.insertNewWinnerRevision(db, body, preRevision, copyAttachments);
        return this.getDocumentInQueue(db, preRevision.getId(), newRevisionId);
    }

    private BasicDocumentRevision deleteDocumentInQueue(SQLDatabase db, final String docId,
                                                        final String prevRevId) throws ConflictException, DocumentNotFoundException, DatastoreException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(docId),
                "Input document id can not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(prevRevId),
                "Input previous revision id can not be empty");

        CouchUtils.validateRevisionId(prevRevId);

        BasicDocumentRevision prevRevision;
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
        setCurrent(db, prevRevision, false);
        String newRevisionId = CouchUtils.generateNextRevisionId(prevRevision.getRevision());
        // Previous revision to be deleted could be winner revision ("current" == true),
        // or a non-winner leaf revision ("current" == false), the new inserted
        // revision must have the same flag as it previous revision.
        // Deletion of non-winner leaf revision is mainly used when resolving
        // conflicts.
        InsertRevisionOptions options = new InsertRevisionOptions();
        options.docNumericId = prevRevision.getInternalNumericId();
        options.revId = newRevisionId;
        options.parentSequence = prevRevision.getSequence();
        options.deleted = true;
        options.current = prevRevision.isCurrent();
        options.data = JSONUtils.EMPTY_JSON;
        options.available = false;
        insertRevision(db, options);


        try {
            //get the deleted document revision to return to the user
            return getDocumentInQueue(db, prevRevision.getId(), newRevisionId);
        } catch (AttachmentException e){
            //throw document not found since we failed to load the document
            throw new DocumentNotFoundException(e);
        }
    }

    @Override
    public void deleteLocalDocument(final String docId) throws DocumentNotFoundException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(docId),
                "Input document id can not be empty");

        try {
            queue.submit(new SQLQueueCallable<Object>() {
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

    private  class InsertRevisionOptions {
        // doc_id in revs table
        public long docNumericId;
        public String revId;
        public long parentSequence;
        // is revision deleted?
        public boolean deleted;
        // is revision current? ("winning")
        public boolean current;
        public byte[] data;
        public boolean available;


        @Override
        public String toString() {
            return "InsertRevisionOptions{" +
                    ", docNumericId=" + docNumericId +
                    ", revId='" + revId + '\'' +
                    ", parentSequence=" + parentSequence +
                    ", deleted=" + deleted +
                    ", current=" + current +
                    ", available=" + available +
                    '}';
        }
    }

    private long insertRevisionAndCopyAttachments(SQLDatabase db,InsertRevisionOptions options) throws AttachmentException, DatastoreException {
        long newSequence = insertRevision(db,options);
        //always copy attachments
        this.attachmentManager.copyAttachments(db,options.parentSequence, newSequence);
        // inserted revision and copied attachments, so we are done
        return newSequence;
    }

    private long insertRevision(SQLDatabase db,InsertRevisionOptions options) {

        long newSequence;
            ContentValues args = new ContentValues();
            args.put("doc_id", options.docNumericId);
            args.put("revid", options.revId);
            // parent field is a foreign key
            if (options.parentSequence > 0) {
                args.put("parent", options.parentSequence);
            }
            args.put("current", options.current);
            args.put("deleted", options.deleted);
            args.put("available", options.available);
            args.put("json", options.data);
            logger.fine("New revision inserted: " + options.docNumericId + ", " + options.revId);
            newSequence = db.insert("revs", args);
            if (newSequence < 0) {
                throw new IllegalStateException("Unknown error inserting new updated doc, please check log");
            }


        return newSequence;
    }

    private long insertStubRevision(SQLDatabase db, long docNumericId, String revId, long parentSequence) throws AttachmentException {
        // don't copy attachments
        InsertRevisionOptions options = new InsertRevisionOptions();
        options.docNumericId = docNumericId;
        options.revId = revId;
        options.parentSequence = parentSequence;
        options.deleted = false;
        options.current = false;
        options.data = JSONUtils.EMPTY_JSON;
        options.available = false;
        return insertRevision(db, options);
    }

    private LocalDocument doGetLocalDocument(SQLDatabase db, String docId)
            throws  DocumentNotFoundException, DocumentException, DatastoreException {
        assert !Strings.isNullOrEmpty(docId);
        Cursor cursor = null;
        try {
            String[] args = {docId};
            cursor = db.rawQuery("SELECT json FROM localdocs WHERE docid=?", args);
            if (cursor.moveToFirst()) {
                byte[] json = cursor.getBlob(0);

                return new LocalDocument(docId,BasicDocumentBody.bodyWith(json));
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

    private List<BasicDocumentRevision> getRevisionsFromRawQuery(SQLDatabase db, String sql, String[] args)
            throws DocumentNotFoundException, AttachmentException, DocumentException, DatastoreException {
        List<BasicDocumentRevision> result = new ArrayList<BasicDocumentRevision>();
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, args);
            while (cursor.moveToNext()) {
                long sequence = cursor.getLong(3);
                List<? extends Attachment> atts = attachmentManager.attachmentsForRevision(db,sequence);
                BasicDocumentRevision row = getFullRevisionFromCurrentCursor(cursor, atts);
                result.add(row);
            }
        } catch (SQLException e) {
           throw new DatastoreException(e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
        return result;
    }

    @Override
    public String getPublicIdentifier() throws DatastoreException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        try {
            return queue.submit(new SQLQueueCallable < String > () {
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


    @Override
    public void forceInsert(final BasicDocumentRevision rev,
                            final List<String> revisionHistory,
                            final Map<String, Object> attachments,
                            final Map<String[],List<PreparedAttachment>>preparedAttachments,
                            final boolean pullAttachmentsInline) throws DocumentException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkNotNull(rev, "Input document revision can not be null");
        Preconditions.checkNotNull(revisionHistory, "Input revision history must not be null");
        Preconditions.checkArgument(revisionHistory.size() > 0, "Input revision history must not be empty");
        Preconditions.checkArgument(checkCurrentRevisionIsInRevisionHistory(rev, revisionHistory),
                "Current revision must exist in revision history.");
        Preconditions.checkArgument(checkRevisionIsInCorrectOrder(revisionHistory),
                "Revision history must be in right order.");
        CouchUtils.validateDocumentId(rev.getId());
        CouchUtils.validateRevisionId(rev.getRevision());

        logger.finer("forceInsert(): " + rev.toString() + ",\n" + JSONUtils.toPrettyJson
                (revisionHistory));

        try {
            Object event = queue.submitTransaction(new SQLQueueCallable<Object>(){
                @Override
                public Object call(SQLDatabase db) throws Exception{
                    DocumentCreated documentCreated = null;
                    DocumentUpdated documentUpdated = null;

                    boolean ok = true;

                    long seq = 0;


                    // sequence here is -1, but we need it to insert the attachment - also might
                    // be wanted by subscribers
                    BasicDocumentRevision revisionFromDB = null;
                    try {
                        revisionFromDB = getDocumentInQueue(db,rev.getId(),null);
                    } catch (DocumentNotFoundException e){
                        // this is expected since this method is normally used by replication
                        // we may be missing the document from our copy
                    }

                    if (revisionFromDB != null) {
                        seq = doForceInsertExistingDocumentWithHistory(db, rev, revisionHistory,
                                attachments);
                        rev.initialiseSequence(seq);
                        // TODO fetch the parent doc?
                        documentUpdated = new DocumentUpdated(null, rev);
                    } else {
                        seq = doForceInsertNewDocumentWithHistory(db, rev, revisionHistory);
                        rev.initialiseSequence(seq);
                        documentCreated = new DocumentCreated(rev);
                    }

                    // now deal with any attachments
                    if (pullAttachmentsInline) {
                        if (attachments != null) {
                            for (String att : attachments.keySet()) {
                                Map attachmentMetadata = (Map)attachments.get(att);
                                Boolean stub = (Boolean) attachmentMetadata.get("stub");

                                if (stub != null && stub) {
                                    // stubs get copied forward at the end of
                                    // insertDocumentHistoryIntoExistingTree - nothing to do here
                                    continue;
                                }
                                String data = (String) attachmentMetadata.get("data");
                                String type = (String) attachmentMetadata.get("content_type");
                                InputStream is = Base64InputStreamFactory.get(new
                                        ByteArrayInputStream(data.getBytes()));
                                // inline attachments are automatically decompressed,
                                // so we don't have to worry about that
                                UnsavedStreamAttachment usa = new UnsavedStreamAttachment(is,
                                        att, type);
                                try {
                                    PreparedAttachment pa = attachmentManager.prepareAttachment(usa);
                                    attachmentManager.addAttachment(db, pa, rev);
                                } catch (Exception e) {
                                    logger.log(Level.SEVERE, "There was a problem adding the " +
                                                    "attachment "
                                                    + usa + "to the datastore for document " + rev,
                                            e);
                                    throw e;
                                }
                            }
                        }
                    } else {

                        try {
                            if (preparedAttachments != null) {
                                for (String[] key : preparedAttachments.keySet()) {
                                    String id = key[0];
                                    String rev = key[1];
                                    try {
                                        BasicDocumentRevision doc = getDocumentInQueue(db, id, rev);
                                        if (doc != null) {
                                            for (PreparedAttachment att : preparedAttachments.get
                                                    (key)) {
                                                attachmentManager.addAttachment(db, att, doc);
                                            }
                                        }
                                    } catch (DocumentNotFoundException e){
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
                        logger.log(Level.FINER, "Inserted revision: %s", rev);
                        if (documentCreated != null) {
                            return documentCreated;
                        } else if (documentUpdated != null) {
                            return documentUpdated;
                        }
                    }
                    return null;
                }
            }).get();

            if(event != null) {
                eventBus.post(event);
            }


        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new DocumentException(e);
        }

    }

    @Override
    public void forceInsert(BasicDocumentRevision rev, String... revisionHistory) throws DocumentException {
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

    private boolean checkCurrentRevisionIsInRevisionHistory(BasicDocumentRevision rev, List<String> revisionHistory) {
        return revisionHistory.get(revisionHistory.size() - 1).equals(rev.getRevision());
    }

    /**
     *
     * @param newRevision DocumentRevision to insert
     * @param revisions   revision history to insert, it includes all revisions (include the revision of the DocumentRevision
     *                    as well) sorted in ascending order.
     */
    private long doForceInsertExistingDocumentWithHistory(SQLDatabase db,BasicDocumentRevision newRevision,
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

        // First look up all locally-known revisions of this document:

        DocumentRevisionTree localRevs;
        try {
            localRevs = getAllRevisionsOfDocumentInQueue(db, newRevision.getId());
        } catch (DocumentNotFoundException e){
            //this shouldn't be thrown since from the checkArugment above call we know the document
            //exists so it should have a revision history
            throw new RuntimeException(String.format("Error getting all revisions of document" +
                    " with id %s even though revision exists",newRevision.getId()), e);
        }

        assert localRevs != null;

        long sequence;

        BasicDocumentRevision parent = localRevs.lookup(newRevision.getId(), revisions.get(0));
        if(parent == null) {
            sequence = insertDocumentHistoryToNewTree(db,newRevision, revisions, localRevs.getDocumentNumericId(), localRevs);
        } else {
            sequence = insertDocumentHistoryIntoExistingTree(db,newRevision, revisions, localRevs.getDocumentNumericId(), localRevs, attachments);
        }
        return sequence;
    }

    private long insertDocumentHistoryIntoExistingTree(SQLDatabase db, BasicDocumentRevision newRevision, List<String> revisions,
                                                       Long docNumericID, DocumentRevisionTree localRevs,
                                                       Map<String, Object> attachments)
            throws AttachmentException, DocumentNotFoundException, DatastoreException {
        BasicDocumentRevision parent = localRevs.lookup(newRevision.getId(), revisions.get(0));
        Preconditions.checkNotNull(parent, "Parent must not be null");
        BasicDocumentRevision previousLeaf = localRevs.getCurrentRevision();


        // Walk through the remote history in chronological order, matching each revision ID to
        // a local revision. When the list diverges, start creating blank local revisions to fill
        // in the local history
        int i ;
        for (i = 1; i < revisions.size(); i++) {
            BasicDocumentRevision nextNode = localRevs.lookupChildByRevId(parent, revisions.get(i));
            if (nextNode == null) {
                break;
            } else {
                parent = nextNode;
            }
        }

        if (i >= revisions.size()) {
            logger.finer("All revision are in local sqlDatabase already, no new revision inserted.");
            return -1;
        }

        // Insert the new stub revisions
        for (; i < revisions.size() - 1; i++) {
            logger.finer("Inserting new stub revision, id: " + docNumericID + ", rev: " + revisions.get(i));
            this.changeDocumentToBeNotCurrent(db,parent.getSequence());
            insertStubRevision(db,docNumericID, revisions.get(i), parent.getSequence());
            parent = getDocumentInQueue(db, newRevision.getId(), revisions.get(i));
            localRevs.add(parent);
        }

        // Insert the new leaf revision
        logger.finer("Inserting new revision, id: " + docNumericID + ", rev: " + revisions.get(i));
        String newRevisionId = revisions.get(revisions.size() - 1);
        this.changeDocumentToBeNotCurrent(db,parent.getSequence());
        // don't copy over attachments
        InsertRevisionOptions options = new InsertRevisionOptions();
        options.docNumericId = docNumericID;
        options.revId = newRevisionId;
        options.parentSequence = parent.getSequence();
        options.deleted = newRevision.isDeleted();
        options.current = false; // we'll call pickWinnerOfConflicts to set this if it needs it
        options.data = newRevision.asBytes();
        options.available = true;
        long sequence =  insertRevision(db, options);

        BasicDocumentRevision newLeaf = getDocumentInQueue(db, newRevision.getId(), newRevisionId);
        localRevs.add(newLeaf);

        // Refresh previous leaf in case it is changed in sqlDb but not in memory
        previousLeaf = getDocumentInQueue(db, previousLeaf.getId(), previousLeaf.getRevision());

        pickWinnerOfConflicts(db,previousLeaf,localRevs);

        // copy stubbed attachments forward from last real revision to this revision
        if (attachments != null) {
            for (String att : attachments.keySet()) {
                Boolean stub = ((Map<String, Boolean>) attachments.get(att)).get("stub");
                if (stub != null && stub.booleanValue()) {
                    try {
                        this.attachmentManager.copyAttachment(db,previousLeaf.getSequence(), sequence, att);
                    } catch (SQLException sqe) {
                        logger.log(Level.SEVERE, "Error copying stubbed attachments", sqe);
                        throw new DatastoreException("Error copying stubbed attachments",sqe);
                    }
                }
            }
        }

        return sequence;
    }

    private long insertDocumentHistoryToNewTree(SQLDatabase db, BasicDocumentRevision newRevision,
                                                List<String> revisions,
                                                Long docNumericID,
                                                DocumentRevisionTree localRevs)
            throws AttachmentException, DocumentNotFoundException, DatastoreException {
        Preconditions.checkArgument(checkCurrentRevisionIsInRevisionHistory(newRevision, revisions),
                "Current revision must exist in revision history.");

        BasicDocumentRevision previousWinner = localRevs.getCurrentRevision();

        // Adding a brand new tree
        logger.finer("Inserting a brand new tree for an existing document.");
        long parentSequence = 0L;
        for(int i = 0 ; i < revisions.size() - 1 ; i ++) {
            //we copy attachments here so allow the exception to propagate
            parentSequence = insertStubRevision(db,docNumericID, revisions.get(i), parentSequence);
            BasicDocumentRevision newNode = this.getDocumentInQueue(db, newRevision.getId(),
                    revisions.get(i));
            localRevs.add(newNode);
        }
        // don't copy attachments
        InsertRevisionOptions options = new InsertRevisionOptions();
        options.docNumericId = docNumericID;
        options.revId = newRevision.getRevision();
        options.parentSequence = parentSequence;
        options.deleted = newRevision.isDeleted();
        options.current = false; // we'll call pickWinnerOfConflicts to set this if it needs it
        options.data = newRevision.asBytes();
        options.available = !newRevision.isDeleted();
        long sequence = insertRevision(db, options);
        BasicDocumentRevision newLeaf = getDocumentInQueue(db, newRevision.getId(), newRevision.getRevision());
        localRevs.add(newLeaf);

        // No need to refresh the previousWinner since we are inserting a new tree,
        // and nothing on the old tree should be touched.
        pickWinnerOfConflicts(db,previousWinner,localRevs);
        return sequence;
    }


    private void pickWinnerOfConflicts(SQLDatabase db, BasicDocumentRevision previousWinner, DocumentRevisionTree objectTree) {

        /*
         Pick winner and mark the appropriate revision with the 'current' flag set
         - There can only be one winner in a tree (or set of trees - if there is no common root)
           at any one time, so if there is a new winner, we only have to mark the old winner as
           no longer 'current'. This is the 'previousWinner' object
         - The new winner is determined by:
           * consider only non-deleted leafs
           * sort according to the CouchDB sorting algorithm: highest rev wins, if there is a tie
             then do a lexicographical compare of the revision id strings
           * we do a reverse sort (highest first) and pick the 1st and mark it 'current'
           * special case: if all leafs are deleted, then apply sorting and selection criteria
             above to all leafs
         */

        // first get all non-deleted leafs
        List<BasicDocumentRevision> leafs = objectTree.leafRevisions(true);
        if (leafs.size() == 0) {
            // all deleted, apply the normal rules to all the leafs
            leafs = objectTree.leafRevisions();
        }

        Collections.sort(leafs, new Comparator<BasicDocumentRevision>() {
            @Override
            public int compare(BasicDocumentRevision r1, BasicDocumentRevision r2) {
                int generationCompare = r1.getGeneration() - r2.getGeneration();
                // note that the return statements have a unary minus since we are reverse sorting
                if (generationCompare != 0) {
                    return -generationCompare;
                } else {
                    return -r1.getRevision().compareTo(r2.getRevision());
                }
            }
        });
        // new winner will be at the top of the list
        BasicDocumentRevision leaf = leafs.get(0);
        if (previousWinner.getSequence() != leaf.getSequence()) {
            this.changeDocumentToBeNotCurrent(db, previousWinner.getSequence());
            this.changeDocumentToBeCurrent(db, leaf.getSequence());
        }
    }

    /**
     * @param rev        DocumentRevision to insert
     * @param revHistory revision history to insert, it includes all revisions (include the revision of the DocumentRevision
     *                   as well) sorted in ascending order.
     */
    private long doForceInsertNewDocumentWithHistory(SQLDatabase db,
                                                     BasicDocumentRevision rev,
                                                     List<String> revHistory)
            throws AttachmentException{
        logger.entering("BasicDocumentRevision",
                "doForceInsertNewDocumentWithHistory()",
                new Object[]{rev, revHistory});

        long docNumericID = insertDocumentID(db,rev.getId());
        long parentSequence = 0L;
        for (int i = 0; i < revHistory.size() - 1; i++) {
            // Insert stub node
            parentSequence = insertStubRevision(db,docNumericID, revHistory.get(i), parentSequence);
        }
        // Insert the leaf node (don't copy attachments)
        InsertRevisionOptions options = new InsertRevisionOptions();
        options.docNumericId = docNumericID;
        options.revId = revHistory.get(revHistory.size() - 1);
        options.parentSequence = parentSequence;
        options.deleted = rev.isDeleted();
        options.current = true;
        options.data = rev.getBody().asBytes();
        options.available = true;
        long sequence = insertRevision(db,options);
        return sequence;
    }

    private void changeDocumentToBeCurrent(SQLDatabase db, long sequence) {
        ContentValues args = new ContentValues();
        args.put("current", 1);
        String[] whereArgs = {Long.toString(sequence)};
        db.update("revs", args, "sequence=?", whereArgs);
    }

    private void changeDocumentToBeNotCurrent(SQLDatabase db, long sequence) {
        ContentValues args = new ContentValues();
        args.put("current", 0);
        String[] whereArgs = {Long.toString(sequence)};
        db.update("revs", args, "sequence=?", whereArgs);
    }

    @Override
    public void compact() {
        try {
            queue.submit(new SQLQueueCallable<Object>() {
                @Override
                public Object call(SQLDatabase db) {
                    logger.finer("Deleting JSON of old revisions...");
                    ContentValues args = new ContentValues();
                    args.put("json", (String) null);
                    int i = db.update("revs", args, "current=0", null);

                    logger.finer("Deleting old attachments...");
                    attachmentManager.purgeAttachments(db);
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

    @Override
    public Map<String, Collection<String>> revsDiff(final Multimap<String, String> revisions) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkNotNull(revisions, "Input revisions must not be null");

        try {
            return queue.submit(new SQLQueueCallable<Map<String,Collection<String>>>(){
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

    @Override
    public String extensionDataFolder(String extensionName) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(extensionName),
                "extension name can not be null or empty");
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
            return queue.submit(new SQLQueueCallable<Iterator<String>>() {
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
            queue.submitTransaction(new SQLQueueCallable<Object>() {
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

                    AttachmentManager.PreparedAndSavedAttachments preparedAndSavedAttachments = null;
                    if (newWinner.getClass() == MutableDocumentRevision.class) {
                        preparedAndSavedAttachments = attachmentManager.prepareAttachments(
                                newWinner.getAttachments() != null ? newWinner.getAttachments().values() : null);
                    }

                        // if it's BasicDocumentRev:
                        // - keep the winner, delete the rest
                        // if it's MutableDocumentRev:
                        // - delete all except the sourceRevId and graft the new revision on later

                        // the revid to keep:
                        // - this will be the source rev id if it's a MutableDocumentRev
                        // - this will be rev id otherwise
                        String revIdKeep;
                        if (newWinner.getClass() == MutableDocumentRevision.class) {
                            revIdKeep = ((MutableDocumentRevision)newWinner).sourceRevisionId;
                        } else {
                            revIdKeep = newWinner.getRevision();
                        }

                        for(BasicDocumentRevision revision : docTree.leafRevisions()) {
                            if(revision.getRevision().equals(revIdKeep)) {
                                // this is the one we want to keep, set it to current
                                setCurrent(db, revision, true);
                            } else {
                                if (revision.isDeleted()) {
                                    // if it is deleted, just make it non-current
                                    setCurrent(db, revision, false);
                                } else {
                                    // if it's not deleted, deleted and make it non-current
                                    BasicDocumentRevision deleted = deleteDocumentInQueue(db,
                                            revision.getId(), revision.getRevision());
                                    setCurrent(db, deleted, false);
                                }
                            }
                        }

                        // if it's MutableDocumentRev: graft the new revision on
                        if (newWinner.getClass() == MutableDocumentRevision.class) {
                            updateDocumentFromRevision(db,(MutableDocumentRevision) newWinner,
                                    preparedAndSavedAttachments);
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
                                           BasicDocumentRevision oldWinner,
                                           boolean copyAttachments)
            throws AttachmentException, DatastoreException {
        String newRevisionId = CouchUtils.generateNextRevisionId(oldWinner.getRevision());

        InsertRevisionOptions options = new InsertRevisionOptions();
        options.docNumericId = oldWinner.getInternalNumericId();
        options.revId = newRevisionId;
        options.parentSequence = oldWinner.getSequence();
        options.deleted = false;
        options.current = true;
        options.data = newWinner.asBytes();
        options.available = true;

        if( copyAttachments){
            this.insertRevisionAndCopyAttachments(db,options);
        } else {
            this.insertRevision(db, options);
        }

        return newRevisionId;
    }

    private void setCurrent(SQLDatabase db,BasicDocumentRevision winner, boolean currentValue) {
        ContentValues updateContent = new ContentValues();
        updateContent.put("current", currentValue ? 1 : 0);
        String[] whereArgs = new String[]{String.valueOf(winner.getSequence())};
        db.update("revs", updateContent, "sequence=?", whereArgs);
    }

    private static BasicDocumentRevision getFullRevisionFromCurrentCursor(Cursor cursor,
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
                .setBody(BasicDocumentBody.bodyWith(json))
                .setDeleted(deleted)
                .setSequence(sequence)
                .setInternalId(internalId)
                .setCurrent(current)
                .setParent(parent)
                .setAttachments(attachments);

        return builder.build();
    }


    // this is just a facade into attachmentManager.PrepareAttachment for the sake of DatastoreWrapper
    @Override
    public PreparedAttachment prepareAttachment(Attachment att, long length, long encodedLength) throws AttachmentException {
        PreparedAttachment pa = attachmentManager.prepareAttachment(att, length, encodedLength);
        return pa;
    }
    
    @Override
    public Attachment getAttachment(final BasicDocumentRevision rev, final String attachmentName) {
        try {
            return queue.submit(new SQLQueueCallable<Attachment>() {
                @Override
                public Attachment call(SQLDatabase db) throws Exception {
                    return attachmentManager.getAttachment(db,rev, attachmentName);
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get attachment",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE,"Failed to get attachment",e);
        }

        return null;
    }

    @Override
    public List<? extends Attachment> attachmentsForRevision(final BasicDocumentRevision rev) throws AttachmentException {
        try {
            return queue.submit(new SQLQueueCallable<List<? extends Attachment>>(){

                @Override
                public List<? extends Attachment> call(SQLDatabase db) throws Exception {
                    return attachmentManager.attachmentsForRevision(db,rev.getSequence());
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
    public BasicDocumentRevision createDocumentFromRevision(final MutableDocumentRevision rev)
            throws DocumentException {
        Preconditions.checkNotNull(rev, "DocumentRevision can not be null");
        Preconditions.checkState(isOpen(), "Datastore is closed");
        final String docId;
        // create docid if docid is null
        if (rev.docId == null) {
            docId = CouchUtils.generateDocumentId();
        } else {
            docId = rev.docId;
        }
        final AttachmentManager.PreparedAndSavedAttachments preparedAndSavedAttachments =
                attachmentManager.prepareAttachments(rev.attachments != null ? rev.attachments.values() : null);
        BasicDocumentRevision created = null;
        try {
            created = queue.submitTransaction(new SQLQueueCallable<BasicDocumentRevision>(){
                @Override
                public BasicDocumentRevision call(SQLDatabase db) throws Exception {
                        // save document with body
                        BasicDocumentRevision saved = createDocument(db,docId, rev.body);
                        // set attachments
                        attachmentManager.setAttachments(db,saved, preparedAndSavedAttachments);
                        // now re-fetch the revision with updated attachments
                        BasicDocumentRevision updatedWithAttachments = getDocumentInQueue(db,
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
    public BasicDocumentRevision updateDocumentFromRevision(final MutableDocumentRevision rev)
            throws DocumentException {
        final AttachmentManager.PreparedAndSavedAttachments preparedAndSavedAttachments =
                this.attachmentManager.prepareAttachments(rev.attachments != null ? rev.attachments.values() : null);

        try {
            BasicDocumentRevision revision = queue.submitTransaction(new SQLQueueCallable<BasicDocumentRevision>(){
                @Override
                public BasicDocumentRevision call(SQLDatabase db) throws Exception {
                    return updateDocumentFromRevision(db,rev, preparedAndSavedAttachments);
                }
            }).get();

            if (revision != null) {
                eventBus.post(new DocumentUpdated(getDocument(rev.docId,rev.sourceRevisionId),revision));
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

    private BasicDocumentRevision updateDocumentFromRevision(SQLDatabase db,MutableDocumentRevision rev,
                                                             AttachmentManager.PreparedAndSavedAttachments preparedAndSavedAttachments)
            throws ConflictException, AttachmentException, DocumentNotFoundException, DatastoreException {
        Preconditions.checkNotNull(rev, "DocumentRevision can not be null");

            // update document with new body
            BasicDocumentRevision updated = updateDocument(db,rev.docId, rev.sourceRevisionId, rev.body, true, false);
            // set attachments
            this.attachmentManager.setAttachments(db,updated, preparedAndSavedAttachments);
            // now re-fetch the revision with updated attachments
            BasicDocumentRevision updatedWithAttachments = this.getDocumentInQueue(db, updated.getId(), updated.getRevision());
            return updatedWithAttachments;
    }

    @Override
    public BasicDocumentRevision deleteDocumentFromRevision(final BasicDocumentRevision rev) throws ConflictException {
        Preconditions.checkNotNull(rev, "DocumentRevision can not be null");
        Preconditions.checkState(isOpen(),"Datastore is closed");

        try {
            BasicDocumentRevision deletedRevision = queue.submitTransaction(new SQLQueueCallable<BasicDocumentRevision>() {
                @Override
                public BasicDocumentRevision call(SQLDatabase db) throws Exception {
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
    public List<BasicDocumentRevision> deleteDocument(final String id)
            throws DocumentException  {
        Preconditions.checkNotNull(id, "id can not be null");
        // to return

        try {
            return queue.submitTransaction(new SQLQueueCallable<List<BasicDocumentRevision>>(){

                @Override
                public List<BasicDocumentRevision> call(SQLDatabase db) throws Exception {
                    ArrayList<BasicDocumentRevision> deleted = new ArrayList<BasicDocumentRevision>();
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

    <T> Future<T> runOnDbQueue(SQLQueueCallable<T> callable){
        return queue.submit(callable);
    }
}
