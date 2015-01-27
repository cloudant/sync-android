//  Copyright (c) 2014 Cloudant. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.cloudant.sync.query;

import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.Changes;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.sqlite.ContentValues;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  Handles updating indexes for a given datastore.
 */
class IndexUpdater {

    private final SQLDatabase database;
    private final Datastore datastore;

    private final ExecutorService queue;

    private static final Logger logger = Logger.getLogger(IndexUpdater.class.getName());

    /**
     *  Constructs a new CDTQQueryExecutor using the indexes in 'database' to index documents from
     *  'datastore'.
     */
    public IndexUpdater(SQLDatabase database, Datastore datastore, ExecutorService queue) {
        this.datastore = datastore;
        this.database = database;
        this.queue = queue;
    }

    /**
     *  Update all indexes in a set.
     *
     *  This indexes are assumed to already exist.
     *
     *  @param indexes Map of indexes and their definitions.
     *  @param database The local database
     *  @param datastore The local datastore
     *  @param queue The executor service queue
     *  @return index update success status (true/false)
     */
    public static boolean updateAllIndexes(Map<String, Object> indexes,
                                           SQLDatabase database,
                                           Datastore datastore,
                                           ExecutorService queue) {
        IndexUpdater updater = new IndexUpdater(database, datastore, queue);

        return updater.updateAllIndexes(indexes);
    }

    /**
     *  Update a single index.
     *
     *  This index is assumed to already exist.
     *
     *  @param indexName Name of index to update
     *  @param fieldNames List of field names in the sort format
     *  @param database The local database
     *  @param datastore The local datastore
     *  @param queue The executor service queue
     *  @return index update success status (true/false)
     */
    public static boolean updateIndex(String indexName,
                                      List<String> fieldNames,
                                      SQLDatabase database,
                                      Datastore datastore,
                                      ExecutorService queue) {
        IndexUpdater updater = new IndexUpdater(database, datastore, queue);

        return updater.updateIndex(indexName, fieldNames);
    }

    @SuppressWarnings("unchecked")
    private boolean updateAllIndexes(Map<String, Object> indexes) {
        boolean success = true;

        for (String indexName: indexes.keySet()) {
            Map<String, Object> index = (Map<String, Object>) indexes.get(indexName);
            List<String> fields = (ArrayList<String>) index.get("fields");
            success = updateIndex(indexName, fields);
            if (!success) {
                break;
            }
        }

        return success;
    }

    private boolean updateIndex(String indexName, List<String> fieldNames) {
        boolean success;
        Changes changes;
        long lastSequence = sequenceNumberForIndex(indexName);

        do {
            changes = datastore.changes(lastSequence, 10000);
            success = updateIndex(indexName, fieldNames, changes, lastSequence);
            lastSequence = changes.getLastSequence();
        } while (success && changes.size() > 0);

        // raise error
        if (!success) {
            logger.log(Level.SEVERE, String.format("Problem updating index %s", indexName));
        }

        return success;
    }

    private boolean updateIndex(final String indexName,
                                final List<String> fieldNames,
                                final Changes changes,
                                long lastSequence) {
        if (indexName == null || indexName.isEmpty()) {
            return false;
        }

        Future<Boolean> result = queue.submit( new Callable<Boolean>() {
            @Override
            public Boolean call() {
                boolean transactionSuccess = true;
                database.beginTransaction();
                for (BasicDocumentRevision rev: changes.getResults()) {
                    // Delete existing values
                    String tableName = IndexManager.tableNameForIndex(indexName);
                    database.delete(tableName, " _id = ? ", new String[]{rev.getId()});

                    // Insert new values if the rev isn't deleted
                    if (!rev.isDeleted()) {
                        // If we are indexing a document where one field is an array, we
                        // have multiple rows to insert into the index.
                        List<DBParameter> parameters = parametersToIndexRevision(rev,
                                                                                 indexName,
                                                                                 fieldNames);
                        if (parameters == null) {
                            continue;
                        }
                        for (DBParameter parameter: parameters) {
                            if (parameter != null) {
                                long rowId = database.insert(parameter.tableName,
                                                             parameter.contentValues);
                                if (rowId < 0) {
                                    transactionSuccess = false;
                                }
                            }
                            if (!transactionSuccess) {
                                String msg = String.format("Updating index %s failed.", indexName);
                                logger.log(Level.SEVERE, msg);
                                break;
                            }
                        }
                    }
                    if (!transactionSuccess) {
                        break;
                    }
                }
                if (transactionSuccess) {
                    database.setTransactionSuccessful();
                }
                database.endTransaction();

                return transactionSuccess;
            }
        });

        boolean success;
        try {
            success = result.get();
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Execution error encountered:", e);
            success = false;
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Execution interrupted error encountered:", e);
            success = false;
        }

        // if there was a problem, we rolled back, so the sequence won't be updated
        if (success) {
            success = updateMetadataForIndex(indexName, lastSequence);
        }

        return success;
    }

    /**
     *  Returns a List of DBParameters containing table name and ContentValues to index
     *  a document in an index.
     *
     *  For most revisions, a single entry will be returned. If a field
     *  is an array, however, multiple entries are required.
     */
    @SuppressWarnings("unchecked")
    private List<DBParameter> parametersToIndexRevision (BasicDocumentRevision rev,
                                                         String indexName,
                                                         List<String> fieldNames) {
        if (rev == null) {
            return null;
        }

        if(indexName == null) {
            return null;
        }

        if (fieldNames == null) {
            return null;
        }

        int arrayCount = 0;
        String arrayFieldName = null; // only record the last, as error if more than one
        for (String fieldName: fieldNames) {
            Object value = ValueExtractor.extractValueForFieldName(fieldName, rev.getBody());
            if (value != null && value instanceof ArrayList) {
                arrayCount = arrayCount + 1;
                arrayFieldName = fieldName;
            }
        }

        if (arrayCount > 1) {
            String msg = String.format("Indexing %s in index %s includes >1 array field;",
                                       rev.getId(),
                                       indexName);
            msg = String.format("%s only array field per index allowed", msg);
            logger.log(Level.SEVERE, msg);
            return null;
        }

        List<DBParameter> parameters = new ArrayList<DBParameter>();
        if (arrayCount == 0) {
            // The are no arrays in the values we are indexing. We just need to index the fields
            // in the index. _id and _rev are special fields in that they don't appear in the
            // body, so they need special-casing to get the values.

            List<String> initialIncludedFields = new ArrayList<String>();
            initialIncludedFields.add("_id");
            initialIncludedFields.add("_rev");
            List<Object> initialArgs = new ArrayList<Object>();
            initialArgs.add(rev.getId());
            initialArgs.add(rev.getRevision());
            DBParameter parameter = populateDBParameter(fieldNames,
                                                        initialIncludedFields,
                                                        initialArgs,
                                                        indexName,
                                                        rev);
            if (parameter == null) {
                return null;
            }
            parameters.add(parameter);
        } else if (arrayFieldName != null) {
            // We know the value is an array, we found this out in the check above
            List<Object> arrayFieldValues;
            arrayFieldValues = (ArrayList) ValueExtractor.extractValueForFieldName(arrayFieldName,
                                                                                   rev.getBody());
            for (Object value: arrayFieldValues) {
                List<String> initialIncludedFields = new ArrayList<String>();
                initialIncludedFields.add("_id");
                initialIncludedFields.add("_rev");
                initialIncludedFields.add(arrayFieldName);
                List<Object> initialArgs = new ArrayList<Object>();
                initialArgs.add(rev.getId());
                initialArgs.add(rev.getRevision());
                initialArgs.add(value);
                DBParameter parameter;
                parameter = populateDBParameter(fieldNames,
                                                initialIncludedFields,
                                                initialArgs,
                                                indexName,
                                                rev);
                if (parameter == null) {
                    return null;
                }
                parameters.add(parameter);
            }
        }

        return parameters;
    }

    private DBParameter populateDBParameter(List<String> fieldNames,
                                            List<String> initialIncludedFields,
                                            List<Object> initialArgs,
                                            String indexName,
                                            BasicDocumentRevision rev) {
        List<String> includeFieldNames = new ArrayList<String>();
        includeFieldNames.addAll(initialIncludedFields);
        List<Object> args = new ArrayList<Object>();
        args.addAll(initialArgs);

        for (String fieldName: fieldNames) {
            // Fields in initialIncludedFields already have values in the other initial* array,
            // so it need not be included again.
            if (initialIncludedFields.contains(fieldName)) {
                continue;
            }

            Object value = ValueExtractor.extractValueForFieldName(fieldName, rev.getBody());

            if (value != null) {
                includeFieldNames.add(fieldName);
                args.add(value);
            }
        }

        ContentValues contentValues = new ContentValues();
        int argIndex = 0;
        for (String fieldName: includeFieldNames) {
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
            } else {
                contentValues.put(fieldName, (String) null);
            }
            argIndex = argIndex + 1;
        }
        String tableName = IndexManager.tableNameForIndex(indexName);

        return new DBParameter(tableName, contentValues);
    }

    private long sequenceNumberForIndex(final String indexName) {
        Future<Long> sequenceNumber = queue.submit( new Callable<Long>() {
            @Override
            public Long call() {
                long result = 0;
                String sql = String.format("SELECT last_sequence FROM %s WHERE index_name = ?",
                                           IndexManager.INDEX_METADATA_TABLE_NAME);
                Cursor cursor = null;
                try {
                    cursor = database.rawQuery(sql, new String[]{ indexName });
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
        });

        long lastSequenceNumber = 0;
        try {
            lastSequenceNumber = sequenceNumber.get();
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Execution error encountered:", e);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Execution interrupted error encountered:", e);
        }

        return lastSequenceNumber;
    }

    private boolean updateMetadataForIndex(final String indexName, final long lastSequence) {
        Future<Boolean> result = queue.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                boolean updateSuccess = true;
                ContentValues v = new ContentValues();
                v.put("last_sequence", lastSequence);
                int row = database.update(IndexManager.INDEX_METADATA_TABLE_NAME,
                                          v,
                                          " index_name = ? ",
                                          new String[]{ indexName });
                if (row <= 0) {
                    updateSuccess = false;
                }
                return updateSuccess;
            }
        });

        boolean success;
        try {
            success = result.get();
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Execution error encountered:", e);
            success = false;
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Execution interrupted error encountered:", e);
            success = false;
        }

        return success;
    }

    private class DBParameter {
        private final String tableName;
        private final ContentValues contentValues;

        public DBParameter(String tableName, ContentValues contentValues) {
            this.tableName = tableName;
            this.contentValues = contentValues;
        }
    }

}