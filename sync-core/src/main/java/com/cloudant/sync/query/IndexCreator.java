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

/**
 *  Handles creating indexes for a given datastore.
 */
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

    protected static String ensureIndexed(Index index,
                                          SQLDatabase database,
                                          Datastore datastore,
                                          ExecutorService queue) {
        IndexCreator executor = new IndexCreator(database, datastore, queue);

        return executor.ensureIndexed(index);
    }

    /**
     *  Add a single, possibly compound index for the given field names and ensure all indexing
     *  constraints are met.
     *
     *  This function generates a name for the new index.
     *
     *  @param index The object that defines an index.  Includes field list, name, type and options.
     *  @return name of created index
     */
    @SuppressWarnings("unchecked")
    private String ensureIndexed(final Index index) {
        if (index == null) {
            return null;
        }

        if (index.indexType.equalsIgnoreCase("text")) {
            if (!IndexManager.ftsAvailable(queue, database)) {
                logger.log(Level.SEVERE, "Text search not supported.  To add support for text " +
                                         "search, enable FTS compile options in SQLite.");
                return null;
            }
        }

        final List<String> fieldNamesList = removeDirectionsFromFields(index.fieldNames);

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
                                       , index.fieldNames);
            logger.log(Level.SEVERE, msg);
        }

        // Prepend _id and _rev if it's not in the array
        if (!fieldNamesList.contains("_rev")) {
            fieldNamesList.add(0, "_rev");
        }

        if (!fieldNamesList.contains("_id")) {
            fieldNamesList.add(0, "_id");
        }

        // Check the index limit.  Limit is 1 for "text" indexes and unlimited for "json" indexes.
        // Then check whether the index already exists; return success if it does and is same,
        // else fail.
        try {
            Map<String, Object> existingIndexes = listIndexesInDatabaseQueue();
            if (indexLimitReached(index, existingIndexes)) {
                String msg = String.format("Index limit reached.  Cannot create index %s.",
                                           index.indexName);
                logger.log(Level.SEVERE, msg);
                return null;
            }
            if (existingIndexes != null && existingIndexes.get(index.indexName) != null) {
                Map<String, Object> existingIndex =
                        (Map<String, Object>) existingIndexes.get(index.indexName);
                String existingType = (String) existingIndex.get("type");
                String existingSettings = (String) existingIndex.get("settings");
                List<String> existingFieldsList = (List<String>) existingIndex.get("fields");
                Set<String> existingFields = new HashSet<String>(existingFieldsList);
                Set<String> newFields = new HashSet<String>(fieldNamesList);
                if (existingFields.equals(newFields) &&
                    index.compareIndexTypeTo(existingType, existingSettings)) {
                    boolean success = IndexUpdater.updateIndex(index.indexName,
                                                               fieldNamesList,
                                                               database,
                                                               datastore,
                                                               queue);
                    return success ? index.indexName : null;
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
                    parameters.put("index_name", index.indexName);
                    parameters.put("index_type", index.indexType);
                    parameters.put("index_settings", index.settingsAsJSON());
                    parameters.put("field_name", fieldName);
                    parameters.put("last_sequence", 0);
                    long rowId = database.insert(IndexManager.INDEX_METADATA_TABLE_NAME,
                                                 parameters);
                    if (rowId < 0) {
                        transactionSuccess = false;
                        break;
                    }
                }

                // Create SQLite data structures to support the index
                // For JSON index type create a SQLite table and a SQLite index
                // For TEXT index type create a SQLite virtual table
                List<String> columnList = new ArrayList<String>();
                for (String field: fieldNamesList) {
                    columnList.add("\"" + field + "\"");
                }

                List<String> statements = new ArrayList<String>();
                if (index.indexType.equalsIgnoreCase(Index.TEXT_TYPE)) {
                    List<String> settingsList = new ArrayList<String>();
                    // Add text settings
                    for (String key : index.indexSettings.keySet()) {
                        settingsList.add(String.format("%s=%s", key, index.indexSettings.get(key)));
                    }
                    statements.add(createVirtualTableStatementForIndex(index.indexName,
                                                                       columnList,
                                                                       settingsList));
                } else {
                    statements.add(createIndexTableStatementForIndex(index.indexName, columnList));
                    statements.add(createIndexIndexStatementForIndex(index.indexName, columnList));
                }
                for (String statement : statements) {
                    try {
                        database.execSQL(statement);
                    } catch (SQLException e) {
                        String msg = String.format("Index creation error occurred (%s):",statement);
                        logger.log(Level.SEVERE, msg, e);
                        transactionSuccess = false;
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
            success = IndexUpdater.updateIndex(index.indexName,
                                               fieldNamesList,
                                               database,
                                               datastore,
                                               queue);
        }

        return success ? index.indexName : null;
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
    protected static List<String> removeDirectionsFromFields(List<Object> fieldNames) {
        List<String> result = new ArrayList<String>();

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

    /**
     * Based on the proposed index and the list of existing indexes, this method checks
     * whether another index can be created.  Currently the limit for TEXT indexes is 1.
     * JSON indexes are unlimited.
     *
     * @param index the proposed index
     * @param existingIndexes the list of already existing indexes
     * @return whether the index limit has been reached
     */
    @SuppressWarnings("unchecked")
    protected static boolean indexLimitReached(Index index, Map<String, Object> existingIndexes) {
        if (index.indexType.equalsIgnoreCase(Index.TEXT_TYPE)) {
            for (String name : existingIndexes.keySet()) {
                Map<String, Object> existingIndex = (Map<String, Object>) existingIndexes.get(name);
                String type = (String) existingIndex.get("type");
                if (type.equalsIgnoreCase(Index.TEXT_TYPE) &&
                    !name.equalsIgnoreCase(index.indexName)) {
                    logger.log(Level.SEVERE,
                            String.format("The text index %s already exists.  ", name) +
                            "One text index per datastore permitted.  " +
                            String.format("Delete %s and recreate %s.", name, index.indexName));
                    return true;
                }
            }
        }

        return false;
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

    private String createIndexTableStatementForIndex(String indexName, List<String> columns) {
        String tableName = IndexManager.tableNameForIndex(indexName);
        Joiner joiner = Joiner.on(" NONE,").skipNulls();
        String cols = joiner.join(columns);

        return String.format("CREATE TABLE %s ( %s NONE )", tableName, cols);
    }

    private String createIndexIndexStatementForIndex(String indexName, List<String> columns) {
        String tableName = IndexManager.tableNameForIndex(indexName);
        String sqlIndexName = tableName.concat("_index");
        Joiner joiner = Joiner.on(",").skipNulls();
        String cols = joiner.join(columns);

        return String.format("CREATE INDEX %s ON %s ( %s )", sqlIndexName, tableName, cols);
    }

    /**
     * This method generates the virtual table create SQL for the specified index.
     * Note:  Any column that contains an '=' will cause the statement to fail
     *        because it triggers SQLite to expect that a parameter/value is being passed in.
     *
     * @param indexName the index name to be used when creating the SQLite virtual table
     * @param columns the columns in the table
     * @param indexSettings the special settings to apply to the virtual table -
     *                      (only 'tokenize' is current supported)
     * @return the SQL to create the SQLite virtual table
     */
    private String createVirtualTableStatementForIndex(String indexName,
                                                       List<String> columns,
                                                       List<String> indexSettings) {
        String tableName = IndexManager.tableNameForIndex(indexName);
        Joiner joiner = Joiner.on(",").skipNulls();
        String cols = joiner.join(columns);
        String settings = joiner.join(indexSettings);

        return String.format("CREATE VIRTUAL TABLE %s USING FTS4 ( %s, %s )", tableName,
                                                                              cols,
                                                                              settings);
    }

}
