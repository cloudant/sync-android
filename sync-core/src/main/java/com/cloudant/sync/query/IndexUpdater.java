//  Copyright (c) 2014 Cloudant. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.cloudant.sync.query;

import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.Changes;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DocumentRevisionBuilder;
import com.cloudant.sync.sqlite.ContentValues;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.DatabaseUtils;

import java.sql.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

class IndexUpdater {

    private final SQLDatabase database;
    private final Datastore datastore;

    private final ExecutorService queue;

    private static final Logger logger = Logger.getLogger(IndexUpdater.class.getName());

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
                                      ArrayList<String> fieldNames,
                                      SQLDatabase database,
                                      Datastore datastore,
                                      ExecutorService queue) {
        IndexUpdater updater = new IndexUpdater(database, datastore, queue);
        return updater.updateIndex(indexName, fieldNames);
    }

    private boolean updateAllIndexes(Map<String, Object> indexes) {

        boolean success = true;

        for (String indexName: indexes.keySet()) {
            Map<String, Object> index = (Map<String, Object>) indexes.get(indexName);
            ArrayList<String> fields = (ArrayList<String>) index.get("fields");
            success = updateIndex(indexName, fields);
            if (!success) {
                break;
            }
        }

        return success;
    }

    private boolean updateIndex(String indexName, ArrayList<String> fieldNames) {

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
                                final ArrayList<String> fieldNames,
                                final Changes changes,
                                long lastSequence) {

        boolean success;

        Future<Boolean> result = queue.submit( new Callable<Boolean>() {
            @Override
            public Boolean call() {
                Boolean transactionSuccess = true;
                database.beginTransaction();
                for (BasicDocumentRevision rev: changes.getResults()) {
                    // Delete existing values
                    String tableName = IndexManager.INDEX_TABLE_PREFIX.concat(indexName);
                    database.delete(tableName, " _id = ? ", new String[]{rev.getId()});

                    // Insert new values if the rev isn't deleted
                    if (!rev.isDeleted()) {
                        // Ignoring the attachments seems reasonable right now
                        // as we don't index them.
                        DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
                        builder.setDocId(rev.getId());
                        builder.setRevId(rev.getRevision());
                        builder.setBody(rev.getBody());
                        builder.setDeleted(rev.isDeleted());
                        builder.setSequence(rev.getSequence());
                        BasicDocumentRevision revision = builder.build();

                        // If we are indexing a document where one field is an array, we
                        // have multiple rows to insert into the index.
                        List<DBParameter> parmList = parmsIndexRevision(revision,
                                                                        indexName,
                                                                        fieldNames);
                        for (DBParameter parm: parmList) {
                            if (parm != null) {
                                long rowId = database.insert(parm.tableName, parm.contentValues);
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
    private List<DBParameter> parmsIndexRevision (BasicDocumentRevision rev,
                                                  String indexName,
                                                  ArrayList<String> fieldNames) {
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
            if (value != null && value instanceof Array) {
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

        List<DBParameter> parmList = new ArrayList<DBParameter>();
        if (arrayCount == 0) {
            // The are no arrays in the values we are indexing. We just need to index the fields
            // in the index. _id and _rev are special fields in that they don't appear in the
            // body, so they need special-casing to get the values.

            DBParameter parm = populateDBParameter(fieldNames
                    , new String[]{ "_id", "_rev" }
                    , new String[]{ rev.getId(), rev.getRevision() }
                    , indexName
                    , rev);
            parmList.add(parm);
        } else if (arrayFieldName != null) {
            // We know the value is an array, we found this out in the check above
            String[] arrayFieldValues =
                    (String[]) ValueExtractor.extractValueForFieldName(arrayFieldName,
                                                                       rev.getBody());
            for (String value: arrayFieldValues) {
                DBParameter parm;
                parm = populateDBParameter(fieldNames,
                                           new String[]{ "_id", "_rev", arrayFieldName },
                                           new String[]{ rev.getId(), rev.getRevision(), value },
                                           indexName,
                                           rev);
                parmList.add(parm);
            }
        }
        return parmList;
    }

    private DBParameter populateDBParameter(ArrayList<String> fieldNames,
                                            String[] initialIncludedFields,
                                            String[] initialArgs,
                                            String indexName,
                                            BasicDocumentRevision rev) {
        List<String> includeFieldNames;
        includeFieldNames = new ArrayList<String>(Arrays.asList(initialIncludedFields));
        List<String> args;
        args = new ArrayList<String>(Arrays.asList(initialArgs));

        for (String fieldName: fieldNames) {
            // Fields in initialIncludedFields already have values in the other initial* array,
            // so it need not be included again.
            boolean skip = false;
            for (String initField: initialIncludedFields) {
                if (initField.equalsIgnoreCase(fieldName)) {
                    skip = true;
                    break;
                }
            }
            if (skip) {
                continue;
            }

            Object value = ValueExtractor.extractValueForFieldName(fieldName, rev.getBody());

            if (value != null) {
                includeFieldNames.add(fieldName);
                args.add((String) value);
            }
        }

        String tableName = IndexManager.INDEX_TABLE_PREFIX.concat(indexName);
        ContentValues contentValues = new ContentValues();
        int argIndex = 0;
        for (String fieldName: includeFieldNames) {
            contentValues.put(fieldName, args.get(argIndex));
            argIndex = argIndex + 1;
        }

        return new DBParameter(tableName, contentValues);
    }

    private long sequenceNumberForIndex(final String indexName) {

        long lastSequenceNumber = 0;
        Future<Long> sequenceNumber = queue.submit( new Callable<Long>() {
            @Override
            public Long call() {
                long result = 0;
                String sql = String.format("SELECT last_sequence FROM %s WHERE index_name = \"%s\"",
                                           IndexManager.INDEX_METADATA_TABLE_NAME,
                                           indexName);
                Cursor cursor = null;
                try {
                    cursor = database.rawQuery(sql, new String[]{});
                    while (cursor.moveToNext()) {
                        result = cursor.getLong(0);
                        // All rows for a given index will have the same last_sequence, so break
                        break;
                    }
                } catch (SQLException e) {
                    logger.log(Level.SEVERE, "Error getting last sequence number. ", e);
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
                return result;
            }
        });

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
        boolean success;

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
