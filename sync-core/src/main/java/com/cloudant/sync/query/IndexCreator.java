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

import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.sqlite.ContentValues;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.google.common.base.Joiner;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

class IndexCreator {

    private final SQLDatabase database;
    private final Datastore datastore;

    private final ExecutorService queue;

    private static final Logger logger = Logger.getLogger(IndexCreator.class.getName());

    public IndexCreator(SQLDatabase database, Datastore datastore, ExecutorService queue) {
        this.datastore = datastore;
        this.database = database;
        this.queue = queue;
    }

    protected static String ensureIndexed(ArrayList<Object> fieldNames,
                                          String indexName,
                                          String indexType,
                                          SQLDatabase database,
                                          Datastore datastore,
                                          ExecutorService queue) {
        IndexCreator executor = new IndexCreator(database, datastore, queue);

        return executor.ensureIndexed(fieldNames, indexName, indexType);
    }

    /**
     *  Add a single, possibly compound, index for the given field names.
     *
     *  This function generates a name for the new index.
     *
     *  @param fieldNames List of field names in the sort format
     *  @param indexName Name of index to create
     *  @param indexType "json" is the only supported type for now
     *  @return name of created index
     */
    private String ensureIndexed(ArrayList<Object> fieldNames,
                                 final String indexName,
                                 final String indexType) {
        if (fieldNames == null || fieldNames.isEmpty()) {
            logger.log(Level.SEVERE, "No field names were passed to ensureIndexed");
            return null;
        }

        if (indexName == null || indexName.isEmpty()) {
            logger.log(Level.SEVERE, "No index name was passed to ensureIndexed");
            return null;
        }

        final ArrayList<String> fieldNamesList = removeDirectionsFromFields(fieldNames);

        for (String fieldName: fieldNamesList) {
            if (!validFieldName(fieldName)) {
                // Logging handled in validFieldName
                return null;
            }
        }

        // Check there are no duplicate field names in the array
        Set<String> uniqueNames = new HashSet<String>(fieldNamesList);
        if (uniqueNames.size() != fieldNamesList.size()) {
            String msg = String.format("Cannot create index with duplicated field names %s"
                                       , fieldNames);
            logger.log(Level.SEVERE, msg);
        }

        // Prepend _id and _rev if it's not in the array
        if (!fieldNamesList.contains("_rev")) {
            fieldNamesList.add(0, "_rev");
        }

        if (!fieldNamesList.contains("_id")) {
            fieldNamesList.add(0, "_id");
        }

        // Does the index already exist; return success if it does and is same, else fail
        try {
            Map<String, Object> existingIndexes = listIndexesInDatabaseQueue();
            if (existingIndexes != null && existingIndexes.get(indexName) != null) {
                @SuppressWarnings("unchecked")
                Map<String, Object> index = (Map<String, Object>) existingIndexes.get(indexName);
                String existingType = (String) index.get("type");
                @SuppressWarnings("unchecked")
                List<String> existingFieldsList = (List<String>) index.get("fields");
                Set<String> existingFields = new HashSet<String>(existingFieldsList);
                Set<String> newFields = new HashSet<String>(fieldNamesList);

                if (existingType.equalsIgnoreCase(indexType) && existingFields.equals(newFields)) {
                    boolean success = IndexUpdater.updateIndex(indexName,
                                                               fieldNamesList,
                                                               database,
                                                               datastore,
                                                               queue);
                    return success ? indexName : null;
                }
            }
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Execution error encountered:", e);
            return null;
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Execution interrupted error encountered:", e);
            return null;
        }

        Future<Boolean> result = queue.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                Boolean transactionSuccess = true;
                database.beginTransaction();

                // Insert metadata table entries
                for (String fieldName: fieldNamesList) {
                    ContentValues parameters = new ContentValues();
                    parameters.put("index_name", indexName);
                    parameters.put("index_type", indexType);
                    parameters.put("field_name", fieldName);
                    parameters.put("last_sequence", 0);
                    long rowId = database.insert(IndexManager.INDEX_METADATA_TABLE_NAME,
                                                 parameters);
                    if (rowId < 0) {
                        transactionSuccess = false;
                        break;
                    }
                }

                String statement = null;
                try {
                    List<String> columnList = new ArrayList<String>();
                    for (String field: fieldNamesList) {
                        // Replace any instance of a quote in the field with two quotes.  This tells
                        // Sqlite that any instance of a quote in the field should be included as
                        // part of the field name.
                        field = field.replace("\"", "\"\"");
                        columnList.add("\"" + field + "\"");
                    }
                    // Create the table for the index
                    statement = createIndexTableStatementForIndexName(indexName, columnList);
                    database.execSQL(statement);

                    // Create the SQLite index on the index table
                    statement = createIndexIndexStatementForIndexName(indexName, columnList);
                    database.execSQL(statement);
                } catch (SQLException e) {
                    String msg = String.format("Index creation error occurred (%s):",statement);
                    logger.log(Level.SEVERE, msg, e);
                    transactionSuccess = false;
                }

                if (transactionSuccess) {
                    database.setTransactionSuccessful();
                }
                database.endTransaction();

                return transactionSuccess;
            }
        });

        // Update the new index if it's been created
        boolean success;
        try {
            success = result.get();
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Execution error encountered:", e);
            return null;
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Execution interrupted error encountered:", e);
            return null;
        }

        if (success) {
            success = IndexUpdater.updateIndex(indexName,
                                               fieldNamesList,
                                               database,
                                               datastore,
                                               queue);
        }

        return success ? indexName : null;
    }

    /**
     *  Validate the field name string is usable.
     *
     *  The only restriction so far is that the parts don't start with
     *  a $ sign, as this makes the query language ambiguous.
     */
    protected static boolean validFieldName(String fieldName) {
        String[] parts = fieldName.split("\\.");
        for (String part: parts) {
            if (part.startsWith("$")) {
                String msg = String.format("Field names cannot start with a $ in field %s", part);
                logger.log(Level.SEVERE, msg);
                return false;
            }
        }

        return true;
    }

    /**
     *  We don't support directions on field names, but they are an optimisation so
     *  we can discard them safely.
     */
    protected static ArrayList<String> removeDirectionsFromFields(ArrayList<Object> fieldNames) {
        ArrayList<String> result = new ArrayList<String>();

        for (Object field: fieldNames) {
            if (field instanceof Map) {
                Map specifier = (Map) field;
                if (specifier.size() == 1) {
                    for (Object key: specifier.keySet()) {
                        // This will iterate only once
                        result.add((String) key);
                    }
                }
            } else if (field instanceof String) {
                result.add((String) field);
            }
        }

        return result;
    }

    private Map<String, Object> listIndexesInDatabaseQueue() throws ExecutionException,
                                                                    InterruptedException {
        Future<Map<String, Object>> indexes = queue.submit(new Callable<Map<String, Object>>() {
            @Override
            public Map<String, Object> call() {
                return IndexManager.listIndexesInDatabase(database);
            }
        });

        return indexes.get();
    }

    private String createIndexTableStatementForIndexName(String indexName, List<String> columns) {
        String tableName = IndexManager.tableNameForIndex(indexName);
        Joiner joiner = Joiner.on(" NONE,").skipNulls();
        String cols = joiner.join(columns);

        return String.format("CREATE TABLE %s ( %s NONE )", tableName, cols);
    }

    private String createIndexIndexStatementForIndexName(String indexName, List<String> columns) {
        String tableName = IndexManager.tableNameForIndex(indexName);
        String sqlIndexName = tableName.concat("_index");
        Joiner joiner = Joiner.on(",").skipNulls();
        String cols = joiner.join(columns);

        return String.format("CREATE INDEX %s ON %s ( %s )", sqlIndexName, tableName, cols);
    }

}