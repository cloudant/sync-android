/*
 * Copyright © 2014 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.internal.query;

import com.cloudant.sync.internal.android.ContentValues;
import com.cloudant.sync.documentstore.Changes;
import com.cloudant.sync.documentstore.Database;
import com.cloudant.sync.internal.documentstore.InternalDocumentRevision;
import com.cloudant.sync.query.FieldSort;
import com.cloudant.sync.query.Index;
import com.cloudant.sync.query.QueryException;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.sqlite.SQLDatabaseQueue;
import com.cloudant.sync.internal.util.DatabaseUtils;
import com.cloudant.sync.internal.util.Misc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  Handles updating indexes for a given datastore.
 */
class IndexUpdater {

    private final Database database;

    private final SQLDatabaseQueue queue;

    private static final Logger logger = Logger.getLogger(IndexUpdater.class.getName());

    /**
     *  Constructs a new CDTQQueryExecutor using the indexes in 'database' to index documents from
     *  'datastore'.
     */
    public IndexUpdater(Database database, SQLDatabaseQueue queue) {
        this.database = database;
        this.queue = queue;
    }

    /**
     *  Update all indexes in a set.
     *
     *  These indexes are assumed to already exist.
     *
     *  @param indexes Map of indexes and their definitions.
     *  @param database The local datastore
     *  @param queue The executor service queue
     *  @return index update success status (true/false)
     */
    public static void updateAllIndexes(List<Index> indexes,
                                           Database database,
                                           SQLDatabaseQueue queue) throws QueryException {
        IndexUpdater updater = new IndexUpdater(database, queue);

        updater.updateAllIndexes(indexes);
    }

    /**
     *  Update a single index.
     *
     *  This index is assumed to already exist.
     *
     *  @param indexName Name of index to update
     *  @param fieldNames List of field names in the sort format
     *  @param database The local datastore
     *  @param queue The executor service queue
     *  @return index update success status (true/false)
     */
    public static void updateIndex(String indexName,
                                      List<FieldSort> fieldNames,
                                      Database database,
                                      SQLDatabaseQueue queue) throws QueryException {
        IndexUpdater updater = new IndexUpdater(database, queue);

        updater.updateIndex(indexName, fieldNames);
    }

    private void updateAllIndexes(List<Index> indexes) throws QueryException {

        for (Index index : indexes) {
            updateIndex(index.indexName, index.fieldNames);
        }
    }

    private void updateIndex(String indexName, List<FieldSort> fieldNames) throws QueryException {

        Misc.checkNotNullOrEmpty(indexName, "indexName");

        Changes changes;
        long lastSequence = sequenceNumberForIndex(indexName);

        do {
            changes = database.changes(lastSequence, 10000);
            updateIndex(indexName, fieldNames, changes, lastSequence);
            lastSequence = changes.getLastSequence();
        } while (changes.size() > 0);
    }

    private void updateIndex(final String indexName,
                                final List<FieldSort> fieldNames,
                                final Changes changes,
                                long lastSequence) throws QueryException {

        Future<Void> result = queue.submitTransaction(new UpdateIndexCallable(changes, indexName,
                fieldNames));

        try {
            result.get();
        } catch (ExecutionException e) {
            String message = String.format("Execution error encountered whilst updating index %s", indexName);
            logger.log(Level.SEVERE, message, e);
            throw new QueryException(message, e);
        } catch (InterruptedException e) {
            String message = String.format("Execution interrupted error encountered whilst updating index %s", indexName);
            logger.log(Level.SEVERE, message, e);
            throw new QueryException(message, e);
        }

        // if there was a problem, we rolled back, and threw an exception, so the sequence won't be
        // updated. otherwise if we got here we can update the sequence.
        updateMetadataForIndex(indexName, lastSequence);


    }

    /**
     *  Returns a List of DBParameters containing table name and ContentValues to index
     *  a document in an index.
     *
     *  For most revisions, a single entry will be returned. If a field
     *  is an array, however, multiple entries are required.
     */
    @SuppressWarnings("unchecked")
    private List<DBParameter> parametersToIndexRevision (InternalDocumentRevision rev,
                                                         String indexName,
                                                         List<FieldSort> fieldNames) {
        Misc.checkNotNull(rev, "rev");
        Misc.checkNotNull(indexName, "indexName");
        Misc.checkNotNull(fieldNames, "fieldNames");

        int arrayCount = 0;
        String arrayFieldName = null; // only record the last, as error if more than one
        for (FieldSort fieldName: fieldNames) {
            Object value = ValueExtractor.extractValueForFieldName(fieldName.field, rev.getBody());
            if (value != null && value instanceof List) {
                arrayCount = arrayCount + 1;
                arrayFieldName = fieldName.field;
            }
        }

        if (arrayCount > 1) {
            String msg = String.format("Indexing %s in index %s includes > 1 array field; " +
                            "Only one array field per index allowed.",
                    rev.getId(),
                    indexName);
            logger.log(Level.SEVERE, msg);
            return null;
        }

        List<DBParameter> parameters = new ArrayList<DBParameter>();
        List<Object> arrayFieldValues = null;
        if (arrayCount == 1) {
            arrayFieldValues = (List) ValueExtractor.extractValueForFieldName(arrayFieldName,
                    rev.getBody());
        }

        if (arrayFieldValues != null && arrayFieldValues.size() > 0) {
            for (Object value: arrayFieldValues) {
                // For each value in the list we create a row. We put this value at the start
                // of the INSERT statement along with _id and _rev, followed by the other
                // fields. _id and _rev are special fields in that they don't appear in the
                // body, so they need special-casing to get the values.
                List<FieldSort> initialIncludedFields = Arrays.asList(new FieldSort("_id"),
                                                                   new FieldSort("_rev"),
                                                                   new FieldSort(arrayFieldName));
                List<Object> initialArgs = Arrays.asList(rev.getId(), rev.getRevision(), value);
                DBParameter parameter = populateDBParameter(fieldNames,
                                                            initialIncludedFields,
                                                            initialArgs,
                                                            indexName,
                                                            rev);
                if (parameter == null) {
                    return null;
                }
                parameters.add(parameter);
            }
        } else {
            // We know that there is no populated list in the values that we are indexing.
            // We just need to index the fields in the index now. _id and _rev are special
            // fields because they don't appear in the document body, so they need
            // special-casing to get the values.
            List<FieldSort> initialIncludedFields = Arrays.asList(new FieldSort("_id"), new FieldSort("_rev"));
            List<Object> initialArgs = Arrays.<Object>asList(rev.getId(), rev.getRevision());
            DBParameter parameter = populateDBParameter(fieldNames,
                                                        initialIncludedFields,
                                                        initialArgs,
                                                        indexName,
                                                        rev);
            if (parameter == null) {
                return null;
            }
            parameters.add(parameter);
        }

        return parameters;
    }

    private DBParameter populateDBParameter(List<FieldSort> fieldNames,
                                            List<FieldSort> initialIncludedFields,
                                            List<Object> initialArgs,
                                            String indexName,
                                            InternalDocumentRevision rev) {
        List<FieldSort> includeFieldNames = new ArrayList<FieldSort>();
        includeFieldNames.addAll(initialIncludedFields);
        List<Object> args = new ArrayList<Object>();
        args.addAll(initialArgs);

        for (FieldSort fieldName: fieldNames) {
            // Fields in initialIncludedFields already have values in the other initial* array,
            // so it need not be included again.
            if (initialIncludedFields.contains(fieldName)) {
                continue;
            }

            Object value = ValueExtractor.extractValueForFieldName(fieldName.field, rev.getBody());
            if (value != null && !(value instanceof List && ((List) value).size() == 0)) {
                // Only include a field with a value or a field with a populated list
                includeFieldNames.add(new FieldSort(fieldName.field));
                args.add(value);
            }
        }

        ContentValues contentValues = new ContentValues();
        int argIndex = 0;
        for (FieldSort f: includeFieldNames) {
            String fieldName = f.field;
            fieldName = String.format("\"%s\"", fieldName);
            Object argument = args.get(argIndex);
            if (argument instanceof Boolean) {
                contentValues.put(fieldName, (Boolean) argument);
            } else if (argument instanceof Byte) {
                contentValues.put(fieldName, (Byte) argument);
            } else if (argument instanceof byte[]) {
                contentValues.put(fieldName, (byte[]) argument);
            } else if (argument instanceof Double) {
                contentValues.put(fieldName, (Double) argument);
            } else if (argument instanceof Float) {
                contentValues.put(fieldName, (Float) argument);
            } else if (argument instanceof Integer) {
                contentValues.put(fieldName, (Integer) argument);
            } else if (argument instanceof Long) {
                contentValues.put(fieldName, (Long) argument);
            } else if (argument instanceof Short) {
                contentValues.put(fieldName, (Short) argument);
            } else if (argument instanceof String) {
                contentValues.put(fieldName, (String) argument);
            }
            // NB there is no default case - if the type isn't supported, it doesn't get indexed
            argIndex = argIndex + 1;
        }
        String tableName = QueryImpl.tableNameForIndex(indexName);

        return new DBParameter(tableName, contentValues);
    }

    private long sequenceNumberForIndex(final String indexName) throws QueryException {
        Future<Long> sequenceNumber = queue.submit(new SequenceNumberForIndexCallable(indexName));

        long lastSequenceNumber = 0;
        try {
            lastSequenceNumber = sequenceNumber.get();
        } catch (ExecutionException e) {
            throw new QueryException("Execution error encountered:", e);
        } catch (InterruptedException e) {
            throw new QueryException("Execution interrupted error encountered:", e);
        }

        return lastSequenceNumber;
    }

    private void updateMetadataForIndex(final String indexName, final long lastSequence) throws QueryException {
        Future<Void> result = queue.submit(new UpdateMetadataForIndexCallable(lastSequence,
                indexName));

        try {
            result.get();
        } catch (ExecutionException e) {
            String message = String.format("Execution error encountered whilst updating index metadata for index %s", indexName);
            logger.log(Level.SEVERE, message, e);
            throw new QueryException(message, e);
        } catch (InterruptedException e) {
            String message = String.format("Execution interrupted error encountered whilst updating index metadata for index %s", indexName);
            logger.log(Level.SEVERE, message, e);
            throw new QueryException(message, e);
        }
    }

    private static class DBParameter {
        private final String tableName;
        private final ContentValues contentValues;

        public DBParameter(String tableName, ContentValues contentValues) {
            this.tableName = tableName;
            this.contentValues = contentValues;
        }
    }

    private static class SequenceNumberForIndexCallable implements SQLCallable<Long> {
        private final String indexName;

        public SequenceNumberForIndexCallable(String indexName) {
            this.indexName = indexName;
        }

        @Override
        public Long call(SQLDatabase database) {
            long result = 0;
            String sql = String.format("SELECT last_sequence FROM %s WHERE index_name = ?",
                                       QueryImpl.INDEX_METADATA_TABLE_NAME);
            Cursor cursor = null;
            try {
                cursor = database.rawQuery(sql, new String[]{indexName});
                if (cursor.getCount() > 0) {
                    // All rows for a given index will have the same last_sequence
                    cursor.moveToNext();
                    result = cursor.getLong(0);
                }
            } catch (SQLException e) {
                logger.log(Level.SEVERE, "Error getting last sequence number. ", e);
            } finally {
                DatabaseUtils.closeCursorQuietly(cursor);
            }
            return result;
        }
    }

    private static class UpdateMetadataForIndexCallable implements SQLCallable<Void> {
        private final long lastSequence;
        private final String indexName;

        public UpdateMetadataForIndexCallable(long lastSequence, String indexName) {
            this.lastSequence = lastSequence;
            this.indexName = indexName;
        }

        @Override
        public Void call(SQLDatabase database) throws QueryException {
            ContentValues v = new ContentValues();
            v.put("last_sequence", lastSequence);
            int row = database.update(QueryImpl.INDEX_METADATA_TABLE_NAME,
                                      v,
                                      " index_name = ? ",
                                      new String[]{indexName});
            if (row <= 0) {
                throw new QueryException("Failed to update index metadata for index "+ indexName);
            }
            return null;
        }
    }

    private class UpdateIndexCallable implements SQLCallable<Void> {
        private final Changes changes;
        private final String indexName;
        private final List<FieldSort> fieldNames;

        public UpdateIndexCallable(Changes changes, String indexName, List<FieldSort> fieldNames) {
            this.changes = changes;
            this.indexName = indexName;
            this.fieldNames = fieldNames;
        }

        @Override
        public Void call(SQLDatabase database) throws QueryException {
            for (DocumentRevision rev: changes.getResults()) {
                // Delete existing values
                String tableName = QueryImpl.tableNameForIndex(indexName);
                database.delete(tableName, " _id = ? ", new String[]{rev.getId()});

                // Insert new values if the rev isn't deleted
                if (!rev.isDeleted()) {
                    // If we are indexing a document where one field is an array, we
                    // have multiple rows to insert into the index.
                    List<DBParameter> parameters = parametersToIndexRevision(rev,
                            indexName,
                            fieldNames);
                    if (parameters == null) {
                        // non-fatal error found with this rev, but we can carry on indexing
                        continue;
                    }
                    for (DBParameter parameter: parameters) {
                        long rowId = database.insert(parameter.tableName,
                                parameter.contentValues);
                        if (rowId < 0) {
                            String msg = String.format("Updating index %s failed.", indexName);
                            throw new QueryException(msg);
                        }
                    }
                }
            }

            return null;
        }
    }
}
