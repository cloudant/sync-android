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

import com.cloudant.common.Log;
import com.cloudant.sync.notifications.DatabaseClosed;
import com.cloudant.sync.notifications.DocumentCreated;
import com.cloudant.sync.notifications.DocumentDeleted;
import com.cloudant.sync.notifications.DocumentUpdated;
import com.cloudant.sync.sqlite.ContentValues;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLDatabaseFactory;
import com.cloudant.sync.util.CouchUtils;
import com.cloudant.sync.util.JSONUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.eventbus.EventBus;

import org.apache.commons.codec.binary.Base64InputStream;
import org.apache.commons.io.FilenameUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
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

class BasicDatastore implements Datastore, DatastoreExtended {

    private static final String LOG_TAG = "BasicDatastore";

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

    private final SQLDatabase sqlDb;
    private final String datastoreName;
    private final EventBus eventBus;
    private final AttachmentManager attachmentManager;

    final String datastoreDir;
    final String extensionsDir;

    private static final String DB_FILE_NAME = "db.sync";

    public BasicDatastore(String dir, String name) throws SQLException, IOException {
        Preconditions.checkNotNull(dir);
        Preconditions.checkNotNull(name);

        this.datastoreDir = dir;
        this.datastoreName = name;
        this.extensionsDir = FilenameUtils.concat(this.datastoreDir, "extensions");
        String dbFilename = FilenameUtils.concat(this.datastoreDir, DB_FILE_NAME);
        this.sqlDb = SQLDatabaseFactory.openSqlDatabase(dbFilename);
        this.updateSchema();
        this.eventBus = new EventBus();
        this.attachmentManager = new AttachmentManager(this);
    }

    private void updateSchema() throws SQLException {
        SQLDatabaseFactory.updateSchema(this.sqlDb, DatastoreConstants.SCHEMA_VERSION_3, 3);
        SQLDatabaseFactory.updateSchema(this.sqlDb, DatastoreConstants.SCHEMA_VERSION_4, 4);
        SQLDatabaseFactory.updateSchema(this.sqlDb, DatastoreConstants.SCHEMA_VERSION_5, 5);
    }

    @Override
    public String getDatastoreName() {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        return this.datastoreName;
    }

    @Override
    public long getLastSequence() {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        String sql = "SELECT MAX(sequence) FROM revs";
        Cursor cursor = null;
        long result = 0;
        try {
            cursor = this.sqlDb.rawQuery(sql, null);
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
            Log.e(LOG_TAG, "Error getting last sequence", e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
        return result;
    }

    @Override
    public int getDocumentCount() {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        String sql = "SELECT COUNT(DISTINCT doc_id) FROM revs WHERE current=1 AND deleted=0";
        Cursor cursor = null;
        int result = 0;
        try {
            cursor = this.sqlDb.rawQuery(sql, null);
            if (cursor.moveToFirst()) {
                result = cursor.getInt(0);
            }
        } catch (SQLException e) {
            Log.e(LOG_TAG, "Error getting document count", e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
        return result;
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
    public BasicDocumentRevision getDocument(String id) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        return getDocument(id, null);
    }

    @Override
    public BasicDocumentRevision getDocument(String id, String rev) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "DocumentRevisionTree id can not be empty");
        Cursor cursor = null;
        try {
            String[] args = (rev == null) ? new String[]{id} : new String[]{id, rev};
            String sql = (rev == null) ? GET_DOCUMENT_CURRENT_REVISION : GET_DOCUMENT_GIVEN_REVISION;
            cursor = this.sqlDb.rawQuery(sql, args);
            if (cursor.moveToFirst()) {
                return SQLDatabaseUtils.getFullRevisionFromCurrentCursor(cursor);
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new SQLRuntimeException("Error getting document with id: " + id + "and rev" + rev, e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    @Override
    public DocumentRevisionTree getAllRevisionsOfDocument(String docId) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(docId),
                "Input document id can not be empty");
        long docNumericId = getDocNumericId(docId);
        if (docNumericId < 0) {
            throw new DocumentNotFoundException("DocumentRevisionTree not found with id: " + docId);
        } else {
            return getAllRevisionsOfDocument(docNumericId);
        }
    }

    private DocumentRevisionTree getAllRevisionsOfDocument(long docNumericID) {
        String sql = "SELECT " + FULL_DOCUMENT_COLS + " FROM revs, docs " +
                "WHERE revs.doc_id=? AND revs.doc_id = docs.doc_id ORDER BY sequence ASC";

        String[] args = {Long.toString(docNumericID)};
        Cursor cursor = null;

        try {
            DocumentRevisionTree tree = new DocumentRevisionTree();
            cursor = this.sqlDb.rawQuery(sql, args);
            while (cursor.moveToNext()) {
                DocumentRevision rev = SQLDatabaseUtils.getFullRevisionFromCurrentCursor(cursor);
                Log.v(LOG_TAG, "Rev: " + rev);
                tree.add(rev);
            }
            return tree;
        } catch (SQLException e) {
            Log.e(LOG_TAG, "Error getting all revisions of document", e);
            return null;
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    @Override
    public long getDocNumericId(String docId) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(docId),
                "Input document id can not be empty");
        Cursor cursor = null;
        try {
            cursor = this.sqlDb.rawQuery("SELECT doc_id FROM docs WHERE docid = ?", new String[]{docId});
            if (cursor.moveToFirst()) {
                return cursor.getLong(0);
            }
        } catch (SQLException e) {
            Log.e(LOG_TAG, "Error getting doc numeric id", e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
        return -1;
    }

    @Override
    public Changes changes(long since, int limit) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(limit > 0, "Limit must be positive number");
        since = since >= 0 ? since : 0;

        String[] args = {Long.toString(since), Long.toString(since + limit)};
        Cursor cursor = null;
        try {
            Long lastSequence = since;
            List<Long> ids = new ArrayList<Long>();
            cursor = this.sqlDb.rawQuery(SQL_CHANGE_IDS_SINCE_LIMIT, args);
            while (cursor.moveToNext()) {
                ids.add(cursor.getLong(0));
                lastSequence = Math.max(lastSequence, cursor.getLong(1));
            }
            List<DocumentRevision> results = this.getDocumentsWithInternalIds(ids);
            if(results.size() != ids.size()) {
                throw new IllegalStateException("The number of document does not match number of ids, " +
                        "something must be wrong here.");
            }

            return new Changes(lastSequence, results);
        } catch (SQLException e) {
            throw new IllegalStateException("Error querying all changes since: " + since + ", limit: " + limit, e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    /**
     * Get list of documents for given list of numeric ids. The result list is ordered by sequence number,
     * and only the current revisions are returned.
     *
     * @param docIds given list of internal ids
     * @return list of documents ordered by sequence number
     */
    List<DocumentRevision> getDocumentsWithInternalIds(List<Long> docIds) {
        Preconditions.checkNotNull(docIds, "Input document internal id list can not be null");
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
                    makePlaceholders(batch.size())
            );
            String[] args = new String[batch.size()];
            for(int i = 0 ; i < batch.size() ; i ++) {
                args[i] = Long.toString(batch.get(i));
            }
            result.addAll(getRevisionsFromRawQuery(sql, args));
        }

        // Contract is to sort by sequence number, which we need to do
        // outside the sqlDb as we're batching requests.
        Collections.sort(result, new Comparator<DocumentRevision>() {
            @Override
            public int compare(DocumentRevision documentRevision, DocumentRevision documentRevision2) {
                long a = documentRevision.getSequence();
                long b = documentRevision2.getSequence();
                return (int)(a - b);
            }
        });

        return result;
    }

    @Override
    public List<DocumentRevision> getAllDocuments(int offset, int limit, boolean descending) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be >= 0");
        }

        // Generate the SELECT statement, based on the options:
        String sql = String.format("SELECT " + FULL_DOCUMENT_COLS + " FROM revs, docs " +
                "WHERE deleted = 0 AND current = 1 AND docs.doc_id = revs.doc_id " +
                "ORDER BY docs.doc_id %1$s, revid DESC LIMIT %2$s OFFSET %3$s ",
                (descending ? "DESC" : "ASC"), limit, offset);
        return getRevisionsFromRawQuery(sql, new String[]{});
    }

    @Override
    public List<DocumentRevision> getDocumentsWithIds(List<String> docIds) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkNotNull(docIds, "Input document id list can not be null");
        String sql = String.format("SELECT " + FULL_DOCUMENT_COLS + " FROM revs, docs" +
                " WHERE docid IN ( %1$s ) AND current = 1 AND docs.doc_id = revs.doc_id " +
                " ORDER BY docs.doc_id ", makePlaceholders(docIds.size()));
        String[] args = docIds.toArray(new String[docIds.size()]);
        List<DocumentRevision> docs = getRevisionsFromRawQuery(sql, args);
        // Sort in memory since seems not able to sort them using SQL
        return sortDocumentsAccordingToIdList(docIds, docs);
    }

    private List<DocumentRevision> sortDocumentsAccordingToIdList(List<String> docIds, List<DocumentRevision> docs) {
        Map<String, DocumentRevision> idToDocs = putDocsIntoMap(docs);
        List<DocumentRevision> results = new ArrayList<DocumentRevision>();
        for (String id : docIds) {
            if (idToDocs.containsKey(id)) {
                results.add(idToDocs.remove(id));
            } else {
                Log.d(LOG_TAG, "No document found for id: " + id);
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

    String makePlaceholders(int len) {
        if (len < 1) {
            // It will lead to an invalid query anyway ..
            throw new RuntimeException("No placeholders");
        } else {
            StringBuilder sb = new StringBuilder(len * 2 - 1);
            sb.append("?");
            for (int i = 1; i < len; i++) {
                sb.append(",?");
            }
            return sb.toString();
        }
    }

    @Override
    public BasicDocumentRevision getLocalDocument(String docId) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        return doGetLocalDocument(docId, null);
    }

    @Override
    public BasicDocumentRevision getLocalDocument(String docId, String revId) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        assert !Strings.isNullOrEmpty(revId);
        return doGetLocalDocument(docId, revId);
    }

    @Override
    public BasicDocumentRevision createDocument(final DocumentBody body) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        String documentId = CouchUtils.generateDocumentId();
        return createDocument(documentId, body);
    }

    @Override
    public BasicDocumentRevision createDocument(String docId, final DocumentBody body) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        CouchUtils.validateDocumentId(docId);
        Preconditions.checkNotNull(body, "Input document body can not be null");
        this.validateDBBody(body);

        DocumentCreated documentCreated = null;
        this.sqlDb.beginTransaction();
        try {
            long docNumericID = insertDocumentID(docId);
            if (docNumericID < 0) {
                throw new IllegalArgumentException("Can not insert new doc, likely the docId exists already: "
                        + docId);
            }

            String revisionId = CouchUtils.getFirstRevisionId();
            long newSequence = insertRevision(docNumericID, revisionId, -1l, false, true, body.asBytes(), true);
            if (newSequence < 0) {
                throw new IllegalStateException("Error inserting data, please checking data.");
            }

            BasicDocumentRevision doc = getDocument(docId, revisionId);
            documentCreated = new DocumentCreated(doc);
            
            Log.d(LOG_TAG, "New document created: " + doc.toString());

            this.sqlDb.setTransactionSuccessful();
            return doc;
        } finally {
            this.sqlDb.endTransaction();
            if (documentCreated != null) {
                eventBus.post(documentCreated);
            }
        }
    }

    private void validateDBBody(DocumentBody body) {
        for(String name : body.asMap().keySet()) {
            if(name.startsWith("_")) {
                throw new IllegalArgumentException("Field name start with '_' is not allowed. ");
            }
        }
    }

    @Override
    public BasicDocumentRevision createLocalDocument(final DocumentBody body) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        String documentId = CouchUtils.generateDocumentId();
        return createLocalDocument(documentId, body);
    }

    @Override
    public BasicDocumentRevision createLocalDocument(String docId, final DocumentBody body) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        CouchUtils.validateDocumentId(docId);
        Preconditions.checkNotNull(body, "Input document body can not be null");
        this.sqlDb.beginTransaction();
        try {
            String firstRevId = CouchUtils.getFirstLocalDocRevisionId();
            ContentValues values = new ContentValues();
            values.put("docid", docId);
            values.put("revid", firstRevId);
            values.put("json", body.asBytes());

            long lineId = this.sqlDb.insert("localdocs", values);
            if (lineId < 0) {
                throw new IllegalArgumentException("Can not insert new local doc, likely the docId exists already: "
                        + docId);
            } else {
                Log.d(LOG_TAG, "New local doc inserted: " + lineId + ", " + docId);
            }

            this.sqlDb.setTransactionSuccessful();
            return getLocalDocument(docId, firstRevId);
        } finally {
            this.sqlDb.endTransaction();
        }
    }

    @Override
    public BasicDocumentRevision updateDocument(String docId, String prevRevId, final DocumentBody body)
            throws ConflictException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(docId),
                "Input document id can not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(prevRevId),
                "Input previous revision id can not be empty");
        Preconditions.checkNotNull(body, "Input document body can not be null");
        this.validateDBBody(body);

        CouchUtils.validateRevisionId(prevRevId);

        DocumentUpdated documentUpdated = null;
        this.sqlDb.beginTransaction();
        try {
            BasicDocumentRevision preRevision = this.getDocument(docId, prevRevId);
            if (preRevision == null) {
                throw new IllegalArgumentException("The document trying to update does not exist.");
            }

            if (!preRevision.isCurrent()) {
                throw new ConflictException("Revision to be updated is not current revision.");
            }

            this.checkOffPreviousWinnerRevisionStatus(preRevision);
            String newRevisionId = this.insertNewWinnerRevision(body, preRevision);
            BasicDocumentRevision newRevision = this.getDocument(preRevision.getId(), newRevisionId);

            this.sqlDb.setTransactionSuccessful();
            documentUpdated = new DocumentUpdated(preRevision, newRevision);
            return newRevision;
        } finally {
            this.sqlDb.endTransaction();
            if (documentUpdated != null) {
                eventBus.post(documentUpdated);
            }
        }
    }

    @Override
    public BasicDocumentRevision updateLocalDocument(String docId, String prevRevId, final DocumentBody body) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(docId),
                "Input document id can not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(prevRevId),
                "Input previous revision id can not be empty");
        Preconditions.checkNotNull(body, "Input document body can not be null");

        CouchUtils.validateRevisionId(prevRevId);

        DocumentRevision preRevision = this.getLocalDocument(docId, prevRevId);
        this.sqlDb.beginTransaction();
        try {
            String newRevId = CouchUtils.generateNextLocalRevisionId(prevRevId);
            ContentValues values = new ContentValues();
            values.put("revid", newRevId);
            values.put("json", body.asBytes());
            String[] whereArgs = new String[]{docId, prevRevId};
            int rowsUpdated = this.sqlDb.update("localdocs", values, "docid=? AND revid=?", whereArgs);
            if (rowsUpdated == 1) {
                this.sqlDb.setTransactionSuccessful();
                return this.getLocalDocument(docId, newRevId);
            } else {
                throw new IllegalStateException("Error updating local docs: " + preRevision);
            }
        } finally {
            this.sqlDb.endTransaction();
        }
    }

    @Override
    public void deleteDocument(String docId, String prevRevId) throws ConflictException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(docId),
                "Input document id can not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(prevRevId),
                "Input previous revision id can not be empty");

        CouchUtils.validateRevisionId(prevRevId);

        DocumentDeleted documentDeleted = null;
        this.sqlDb.beginTransaction();
        try {
            BasicDocumentRevision preRevision = this.getDocument(docId, prevRevId);
            if (preRevision == null) {
                throw new IllegalArgumentException("The document trying to update does not exist.");
            }

            DocumentRevisionTree revisionTree = this.getAllRevisionsOfDocument(docId);
            if(revisionTree == null) {
                throw new IllegalArgumentException("Document does not exist for id: " + docId);
            } else if (!revisionTree.leafRevisionIds().contains(prevRevId)) {
                throw new ConflictException("Revision to be deleted is not a leaf node:"
                        + prevRevId);
            }

            if(!preRevision.isDeleted()) {
                this.checkOffPreviousWinnerRevisionStatus(preRevision);
                String newRevisionId = CouchUtils.generateNextRevisionId(preRevision.getRevision());
                // Previous revision to be deleted could be winner revision ("current" == true),
                // or a non-winner leaf revision ("current" == false), the new inserted
                // revision must have the same flag as it previous revision.
                // Deletion of non-winner leaf revision is mainly used when resolving
                // conflicts.
                this.insertRevision(preRevision.getInternalNumericId(), newRevisionId,
                        preRevision.getSequence(), true, preRevision.isCurrent(), JSONUtils.EMPTY_JSON, false);
                BasicDocumentRevision newRevision = this.getDocument(preRevision.getId(), newRevisionId);
                documentDeleted = new DocumentDeleted(preRevision, newRevision);
            }

            // Very tricky! Must call setTransactionSuccessful() even no change
            // to the db within this method. This is to allow this method to be
            // nested to other outer transaction, otherwise, the outer transaction
            // will rollback.
            this.sqlDb.setTransactionSuccessful();
        } finally {
            this.sqlDb.endTransaction();
            if (documentDeleted != null) {
                eventBus.post(documentDeleted);
            }
        }
    }

    @Override
    public void deleteLocalDocument(String docId) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(docId),
                "Input document id can not be empty");
        String[] whereArgs = {docId};
        int rowsDeleted = this.sqlDb.delete("localdocs", "docid=? ", whereArgs);
        if (rowsDeleted == 0) {
            throw new DocumentNotFoundException("No local document with doc id: " + docId);
        }
    }

    private long insertDocumentID(String docId) {
        ContentValues args = new ContentValues();
        args.put("docid", docId);
        return this.sqlDb.insert("docs", args);
    }

    private long insertRevision(long docNumericId, String revId, long parentSequence, boolean deleted, boolean current, byte[] data, boolean available) {
        ContentValues args = new ContentValues();
        args.put("doc_id", docNumericId);
        args.put("revid", revId);
        // parent field is a foreign key
        if (parentSequence > 0) {
            args.put("parent", parentSequence);
        }
        args.put("current", current);
        args.put("deleted", deleted);
        args.put("available", available);
        args.put("json", data);
        Log.d(LOG_TAG, "New revision inserted: " + docNumericId + ", " + revId);
        long newSequence = this.getSQLDatabase().insert("revs", args);
        if (newSequence < 0) {
            throw new IllegalStateException("Unknown error inserting new updated doc, please checking log");
        }
        // by default all the attachments from the previous rev will be carried over

        String attsSql = "INSERT INTO attachments "+
        "(sequence, filename, key, type, length, revpos) "+
        "SELECT "+newSequence+", filename, key, type, length, revpos "+
        "FROM attachments WHERE sequence="+parentSequence;

//        Object[] attsArgs = {parentSequence, newSequence};
        try {
            this.getSQLDatabase().execSQL(attsSql);
        } catch (SQLException e) {
            throw new IllegalStateException("Error copying attachments to new revision "+e);
        }

        return newSequence;
    }

    private long insertStubRevision(long docNumricId, String revId, long parentSequence) {
        return insertRevision(docNumricId, revId, parentSequence, false, false, JSONUtils.EMPTY_JSON, false);
    }

    // Keep in mind we do not keep local document revision history
    private BasicDocumentRevision doGetLocalDocument(String docId, String revId) {
        assert !Strings.isNullOrEmpty(docId);
        Cursor cursor = null;
        try {
            String[] args = {docId};
            cursor = this.sqlDb.rawQuery("SELECT revid, json FROM localdocs WHERE docid=?", args);
            if (cursor.moveToFirst()) {
                String gotRevID = cursor.getString(0);

                if (revId != null && !revId.equals(gotRevID)) {
//                    throw new DocumentNotFoundException("No local document found with id: " + docId + ", revId: " + revId);
                    return null;
                }

                byte[] json = cursor.getBlob(1);

                DocumentRevisionBuilder builder = new DocumentRevisionBuilder()
                        .setDocId(docId)
                        .setRevId(gotRevID)
                        .setBody(BasicDocumentBody.bodyWith(json));

                return builder.buildBasicDBObjectLocalDocument();
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new SQLRuntimeException("Error getting local document with id: " + docId, e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    private List<DocumentRevision> getRevisionsFromRawQuery(String sql, String[] args) {
        List<DocumentRevision> result = new ArrayList<DocumentRevision>();
        Cursor cursor = null;
        try {
            cursor = this.sqlDb.rawQuery(sql, args);
            while (cursor.moveToNext()) {
                DocumentRevision row = SQLDatabaseUtils.getFullRevisionFromCurrentCursor(cursor);
                result.add(row);
            }
        } catch (SQLException e) {
            e.printStackTrace();  //To change bodyOne of catch statement use File | Settings | File Templates.
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
        return result;
    }

    @Override
    public SQLDatabase getSQLDatabase() {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        return this.sqlDb;
    }

    @Override
    public String getPublicIdentifier() {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Cursor cursor = null;
        try {
            cursor = this.sqlDb.rawQuery("SELECT value FROM info WHERE key='publicUUID'", null);
            if (cursor.moveToFirst()) {
                return "touchdb_" + cursor.getString(0);
            } else {
                throw new IllegalStateException("Error querying PublicUUID, " +
                        "it is probably because the sqlDatabase is not probably initialized.");
            }
        } catch (SQLException e) {
            throw new SQLRuntimeException("Error querying publicUUID: ", e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }


    @Override
    public void forceInsert(DocumentRevision rev, List<String> revisionHistory, Map<String, Object> attachments) {
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

        Log.v(LOG_TAG, "forceInsert(): " + rev.toString() + ",\n" + JSONUtils.toPrettyJson(revisionHistory));

        DocumentCreated documentCreated = null;
        DocumentUpdated documentUpdated = null;

        this.sqlDb.beginTransaction();
        try {
            long seq = 0;
            // sequence here is -1, but we need it to insert the attachment - also might be wanted by subscribers
            if (!this.containsDocument(rev.getId())) {
                seq = doForceInsertNewDocumentWithHistory(rev, revisionHistory);
                rev.setSequence(seq);
                documentCreated = new DocumentCreated(rev);
            } else {
                seq = doForceInsertExistingDocumentWithHistory(rev, revisionHistory);
                rev.setSequence(seq);
                // TODO fetch the parent doc?
                documentUpdated = new DocumentUpdated(null, rev);
            }
            // now deal with any attachments
            if (attachments != null) {
                for (String att : attachments.keySet()) {
                    String data = (String)((Map<String,Object>)attachments.get(att)).get("data");
                    InputStream is = new Base64InputStream(new ByteArrayInputStream(data.getBytes()));
                    String type = (String)((Map<String,Object>)attachments.get(att)).get("content_type");
                    UnsavedFileAttachment ufa = new UnsavedFileAttachment(is, att, type);
                    try {
                        this.attachmentManager.addAttachment(ufa, rev);
                    } catch (Exception e) {
                        System.out.println("Problem adding attachment! "+e);
                    }
                }
            }

            this.sqlDb.setTransactionSuccessful();
        } finally {
            this.sqlDb.endTransaction();
            if (documentCreated != null) {
                eventBus.post(documentCreated);
            }
            else if (documentUpdated != null) {
                eventBus.post(documentUpdated);                
            }
        }
    }

    @Override
    public void forceInsert(DocumentRevision rev, String... revisionHistory) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        this.forceInsert(rev, Arrays.asList(revisionHistory), null);
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
    private long doForceInsertExistingDocumentWithHistory(DocumentRevision newRevision, List<String> revisions) {
        Log.v(LOG_TAG, "doForceInsertExistingDocumentWithHistory(): Revisions: " + revisions);
        Preconditions.checkNotNull(newRevision, "New document revision must not be null.");
        Preconditions.checkArgument(this.containsDocument(newRevision.getId()), "DocumentRevisionTree must exist.");
        Preconditions.checkNotNull(revisions, "Revision history should not be null.");
        Preconditions.checkArgument(revisions.size() > 0, "Revision history should have at least one revision." );

        // First look up all locally-known revisions of this document:
        long docNumericID = this.getDocNumericId(newRevision.getId());
        DocumentRevisionTree localRevs = getAllRevisionsOfDocument(docNumericID);

        assert localRevs != null;

        long sequence;

        DocumentRevision parent = localRevs.lookup(newRevision.getId(), revisions.get(0));
        if(parent == null) {
            sequence = insertDocumentHistoryToNewTree(newRevision, revisions, docNumericID, localRevs);
        } else {
            sequence = insertDocumentHistoryIntoExistingTree(newRevision, revisions, docNumericID, localRevs);
        }
        return sequence;
    }

    private long insertDocumentHistoryIntoExistingTree(DocumentRevision newRevision, List<String> revisions,
                                                       Long docNumericID, DocumentRevisionTree localRevs) {
        DocumentRevision parent = localRevs.lookup(newRevision.getId(), revisions.get(0));
        Preconditions.checkNotNull(parent, "Parent must not be null");
        BasicDocumentRevision previousLeaf = (BasicDocumentRevision) localRevs.getCurrentRevision();

        // Walk through the remote history in chronological order, matching each revision ID to
        // a local revision. When the list diverges, start creating blank local revisions to fill
        // in the local history
        int i ;
        for (i = 1; i < revisions.size(); i++) {
            DocumentRevision nextNode = localRevs.lookupChildByRevId(parent, revisions.get(i));
            if (nextNode == null) {
                break;
            } else {
                parent = nextNode;
            }
        }

        if (i >= revisions.size()) {
            Log.v(LOG_TAG, "All revision are in local sqlDatabase already, no new revision inserted.");
            return -1;
        }

        // Insert the new stub revisions
        for (; i < revisions.size() - 1; i++) {
            Log.v(LOG_TAG, "Inserting new stub revision, id: " + docNumericID + ", rev: " + revisions.get(i));
            this.changeDocumentToBeNotCurrent(parent.getSequence());
            insertStubRevision(docNumericID, revisions.get(i), parent.getSequence());
            parent = getDocument(newRevision.getId(), revisions.get(i));
            localRevs.add(parent);
        }

        // Insert the new leaf revision
        Log.v(LOG_TAG, "Inserting new revision, id: " + docNumericID + ", rev: " + revisions.get(i));
        String newRevisionId = revisions.get(revisions.size() - 1);
        this.changeDocumentToBeNotCurrent(parent.getSequence());
        long sequence = insertRevision(docNumericID, newRevisionId, parent.getSequence(), newRevision.isDeleted(), true, newRevision.asBytes(), true);
        BasicDocumentRevision newLeaf = getDocument(newRevision.getId(), newRevisionId);
        localRevs.add(newLeaf);

        // Refresh previous leaf in case it is changed in sqlDb but not in memory
        previousLeaf = getDocument(previousLeaf.getId(), previousLeaf.getRevision());

        if (previousLeaf.isCurrent()) {
            // we have a conflicts, and we need to resolve it.
            pickWinnerOfConflicts(localRevs, newLeaf, previousLeaf);
        }

        return sequence;
    }

    private long insertDocumentHistoryToNewTree(DocumentRevision newRevision, List<String> revisions,
                                                Long docNumericID, DocumentRevisionTree localRevs) {
        Preconditions.checkArgument(checkCurrentRevisionIsInRevisionHistory(newRevision, revisions),
                "Current revision must exist in revision history.");

        BasicDocumentRevision previousWinner = (BasicDocumentRevision) localRevs.getCurrentRevision();

        // Adding a brand new tree
        Log.v(LOG_TAG, "Inserting a brand new tree for an existing document.");
        long parentSequence = 0L;
        for(int i = 0 ; i < revisions.size() - 1 ; i ++) {
            parentSequence = insertStubRevision(docNumericID, revisions.get(i), parentSequence);
            DocumentRevision newNode = this.getDocument(newRevision.getId(), revisions.get(i));
            localRevs.add(newNode);
        }

        long sequence = insertRevision(docNumericID, newRevision.getRevision(), parentSequence, newRevision.isDeleted(), true,
                newRevision.asBytes(), !newRevision.isDeleted());
        BasicDocumentRevision newLeaf = getDocument(newRevision.getId(), newRevision.getRevision());
        localRevs.add(newLeaf);

        // No need to refresh the previousWinner since we are inserting a new tree,
        // and nothing on the old tree should be touched.
        pickWinnerOfConflicts(localRevs, newLeaf, previousWinner);
        return sequence;
    }


    private void pickWinnerOfConflicts(DocumentRevisionTree objectTree, BasicDocumentRevision newLeaf, BasicDocumentRevision previousLeaf) {
        // We are having a conflict, and we are resolving it
        if (newLeaf.isDeleted() == previousLeaf.isDeleted()) {
            // If both leafs are deleted or not
            int previousLeafDepth = objectTree.depth(previousLeaf.getSequence());
            int newLeafDepth = objectTree.depth(newLeaf.getSequence());
            if (previousLeafDepth > newLeafDepth) {
                this.changeDocumentToBeNotCurrent(newLeaf.getSequence());
            } else if (previousLeafDepth < newLeafDepth) {
                this.changeDocumentToBeNotCurrent(previousLeaf.getSequence());
            } else {
                // Compare revision hash if both leafs has same depth
                String previousRevisionHash = previousLeaf.getRevision().substring(2);
                String newRevisionHash = newLeaf.getRevision().substring(2);
                if (previousRevisionHash.compareTo(newRevisionHash) > 0) {
                    this.changeDocumentToBeNotCurrent(newLeaf.getSequence());
                } else {
                    this.changeDocumentToBeNotCurrent(previousLeaf.getSequence());
                }
            }
        } else {
            // If only one of the leaf is not deleted, that is the winner
            if (newLeaf.isDeleted()) {
                this.changeDocumentToBeNotCurrent(newLeaf.getSequence());
            } else {
                this.changeDocumentToBeNotCurrent(previousLeaf.getSequence());
            }
        }
    }

    /**
     * @param rev        DocumentRevision to insert
     * @param revHistory revision history to insert, it includes all revisions (include the revision of the DocumentRevision
     *                   as well) sorted in ascending order.
     */
    private long doForceInsertNewDocumentWithHistory(DocumentRevision rev, List<String> revHistory) {
        Log.v(LOG_TAG, "doForceInsertNewDocumentWithHistory()");
        assert !this.containsDocument(rev.getId());

        long docNumericID = insertDocumentID(rev.getId());
        long parentSequence = 0L;
        for (int i = 0; i < revHistory.size() - 1; i++) {
            // Insert stub node
            parentSequence = insertStubRevision(docNumericID, revHistory.get(i), parentSequence);
        }
        // Insert the leaf node
        long seq = insertRevision(docNumericID, revHistory.get(revHistory.size() - 1), parentSequence, rev.isDeleted(), true, rev.getBody().asBytes(), true);
        return seq;
    }

    private void changeDocumentToBeNotCurrent(long sequence) {
        ContentValues args = new ContentValues();
        args.put("current", 0);
        String[] whereArgs = {Long.toString(sequence)};
        this.sqlDb.update("revs", args, "sequence=?", whereArgs);
    }

    /**
     * Compacts the sqlDatabase storage by removing the bodies and attachments of obsolete revisions.
     */
    public void compact() {
        Log.v(LOG_TAG, "Deleting JSON of old revisions...");
        ContentValues args = new ContentValues();
        args.put("json", (String) null);
        this.sqlDb.update("revs", args, "current=0", null);

        Log.v(LOG_TAG, "Deleting old attachments...");

        Log.v(LOG_TAG, "Vacuuming SQLite database...");
        this.sqlDb.compactDatabase();
    }

    @Override
    public void close() {
        try {
            if (this.sqlDb != null && this.sqlDb.isOpen()) {
                this.sqlDb.close();
            }
        } finally {
            this.eventBus.post(new DatabaseClosed(this.datastoreName));
        }
    }

    boolean isOpen() {
        return this.sqlDb.isOpen();
    }

    @Override
    public Map<String, Collection<String>> revsDiff(Multimap<String, String> revisions) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkNotNull(revisions, "Input revisions must not be null");
        Multimap<String, String> missingRevs = ArrayListMultimap.create();
        // Break the potentially big multimap into small ones so for each map,
        // a single query can be use to check if the <id, revision> pairs in sqlDb or not
        List<Multimap<String, String>> batches =
                this.multiMapPartitions(revisions, SQLITE_QUERY_PLACEHOLDERS_LIMIT);
        for(Multimap<String, String> batch : batches) {
            this.revsDiffBatch(batch);
            missingRevs.putAll(batch);
        }
        return missingRevs.asMap();
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
    void revsDiffBatch(Multimap<String, String> revisions) {

        final String sql = String.format(
                "SELECT docs.docid, revs.revid FROM docs, revs " +
                "WHERE docs.doc_id = revs.doc_id AND docs.docid IN (%s) AND revs.revid IN (%s) " +
                "ORDER BY docs.docid",
                makePlaceholders(revisions.keySet().size()),
                makePlaceholders(revisions.size()));

        String[] args = new String[revisions.keySet().size() + revisions.size()];
        String[] keys = revisions.keySet().toArray(new String[revisions.keySet().size()]);
        String[] values = revisions.values().toArray(new String[revisions.size()]);
        System.arraycopy(keys, 0, args, 0, revisions.keySet().size());
        System.arraycopy(values, 0,args, revisions.keySet().size(), revisions.size());

        Cursor cursor = null;
        try {
            cursor = this.sqlDb.rawQuery(sql, args);
            while (cursor.moveToNext()) {
                String docId = cursor.getString(0);
                String revId = cursor.getString(1);
                revisions.remove(docId, revId);
            }
        } catch (SQLException e) {
            e.printStackTrace();
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

        List<String> conflicts = new ArrayList<String>();
        Cursor cursor = null;
        try {
            cursor = this.sqlDb.rawQuery(sql, new String[]{});
            while (cursor.moveToNext()) {
                String docId = cursor.getString(0);
                conflicts.add(docId);
            }
        }  catch (SQLException e) {
            Log.e(LOG_TAG, "Error getting conflicted document: ", e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
        return conflicts.iterator();
    }

    @Override
    public void resolveConflictsForDocument(String docId, ConflictResolver resolver)
            throws ConflictException {
        this.sqlDb.beginTransaction();
        try {

            DocumentRevisionTree doc = this.getAllRevisionsOfDocument(docId);
            if(!doc.hasConflicts()) {
                return;
            }

            DocumentRevision newWinner = null;
            try {
                newWinner = resolver.resolve(docId, doc.leafRevisions());
            } catch (Exception e) {
                Log.e(LOG_TAG, "Exception when calling ConflictResolver", e);
            }
            if(newWinner == null) {
                return;
            }

            DocumentRevision winner = doc.getCurrentRevision();
            if(!newWinner.isDeleted()) {
                this.updateDocument(winner.getId(), winner.getRevision(), newWinner.getBody());
            } else {
                this.deleteDocument(winner.getId(), winner.getRevision());
            }

            for(DocumentRevision revision : doc.leafRevisions()) {
                if(!revision.isCurrent() && !revision.isDeleted()) {
                    this.deleteDocument(revision.getId(), revision.getRevision());
                }
            }
            this.sqlDb.setTransactionSuccessful();
        } finally {
            this.sqlDb.endTransaction();
        }
    }

    private String insertNewWinnerRevision(DocumentBody newWinner, DocumentRevision oldWinner) {
        String newRevisionId = CouchUtils.generateNextRevisionId(oldWinner.getRevision());
        this.insertRevision(oldWinner.getInternalNumericId(),
                newRevisionId,
                oldWinner.getSequence(),
                false,
                true,
                newWinner.asBytes(),
                true);
        return newRevisionId;
    }

    private void checkOffPreviousWinnerRevisionStatus(DocumentRevision winner) {
        ContentValues updateContent = new ContentValues();
        updateContent.put("current", 0);
        String[] whereArgs = new String[]{String.valueOf(winner.getSequence())};
        this.getSQLDatabase().update("revs", updateContent, "sequence=?", whereArgs);
    }

    @Override
    public DocumentRevision updateAttachments(DocumentRevision rev, List<? extends Attachment> attachments) throws ConflictException, IOException {
        // facade to attachmentmanager
        return this.attachmentManager.updateAttachments(rev, attachments);
    }

    @Override
    public Attachment getAttachment(DocumentRevision rev, String attachmentName) {
        // facade to attachmentmanager
        return this.attachmentManager.getAttachment(rev, attachmentName);
    }

    @Override
    public List<? extends Attachment> attachmentsForRevision(DocumentRevision rev) {
        // facade to attachmentmanager
        return this.attachmentManager.attachmentsForRevision(rev);
    }

    @Override
    public DocumentRevision removeAttachments(DocumentRevision rev, String[] attachmentNames) throws ConflictException {
        return this.attachmentManager.removeAttachments(rev, attachmentNames);
    }

    @Override
    public EventBus getEventBus() {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        return eventBus;
    }
}
