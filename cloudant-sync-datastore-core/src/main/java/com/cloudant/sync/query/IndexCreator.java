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

import com.cloudant.android.ContentValues;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLDatabaseQueue;
import com.google.common.base.Joiner;

import org.apache.commons.codec.binary.Hex;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  Handles creating indexes for a given datastore.
 */
class IndexCreator {

    private final Datastore datastore;
    private static Random indexNameRandom = new Random();

    private final SQLDatabaseQueue queue;

    private static final Logger logger = Logger.getLogger(IndexCreator.class.getName());

    public IndexCreator(Datastore datastore, SQLDatabaseQueue queue) {
        this.datastore = datastore;
        this.queue = queue;
    }

    protected static String ensureIndexed(Index index,
                                          Datastore datastore,
                                          SQLDatabaseQueue queue) {
        IndexCreator executor = new IndexCreator(datastore, queue);

        return executor.ensureIndexed(index);
    }

    /**
     *  Add a single, possibly compound index for the given field names and ensure all indexing
     *  constraints are met.
     *
     *  This function generates a name for the new index.
     *
     *  @param proposedIndex The object that defines an index.  Includes field list, name, type and options.
     *  @return name of created index
     */
    @SuppressWarnings("unchecked")
    private String ensureIndexed(Index proposedIndex) {
        if (proposedIndex == null) {
            return null;
        }

        if (proposedIndex.indexType == IndexType.TEXT) {
            if (!IndexManager.ftsAvailable(queue)) {
                logger.log(Level.SEVERE, "Text search not supported.  To add support for text " +
                                         "search, enable FTS compile options in SQLite.");
                return null;
            }
        }

        final List<String> fieldNamesList = removeDirectionsFromFields(proposedIndex.fieldNames);

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
                                       , proposedIndex.fieldNames);
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

            if(proposedIndex.indexName == null){
                // generate a name for the index.
                String indexName = IndexCreator.generateIndexName(existingIndexes.keySet());
                if(indexName == null){
                    logger.warning("Failed to generate unique index name");
                    return null;
                }

                proposedIndex = Index.getInstance(proposedIndex.fieldNames,
                                          indexName,
                        proposedIndex.indexType,
                        proposedIndex.indexSettings);
            }


            if (indexLimitReached(proposedIndex, existingIndexes)) {
                String msg = String.format("Index limit reached.  Cannot create index %s.",
                                           proposedIndex.indexName);
                logger.log(Level.SEVERE, msg);
                return null;
            }
            if (existingIndexes != null && existingIndexes.get(proposedIndex.indexName) != null) {
                Map<String, Object> existingIndex =
                        (Map<String, Object>) existingIndexes.get(proposedIndex.indexName);
                IndexType existingType = (IndexType) existingIndex.get("type");
                String existingSettings = (String) existingIndex.get("settings");
                List<String> existingFieldsList = (List<String>) existingIndex.get("fields");
                Set<String> existingFields = new HashSet<String>(existingFieldsList);
                Set<String> newFields = new HashSet<String>(fieldNamesList);
                if (existingFields.equals(newFields) &&
                        proposedIndex.compareIndexTypeTo(existingType, existingSettings)) {
                    boolean success = IndexUpdater.updateIndex(proposedIndex.indexName,
                                                               fieldNamesList,
                                                               datastore,
                                                               queue);
                    return success ? proposedIndex.indexName : null;
                }
            }
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Execution error encountered:", e);
            return null;
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Execution interrupted error encountered:", e);
            return null;
        }

        final Index index = proposedIndex;
        Future<Boolean> result = queue.submit(new SQLCallable<Boolean>() {
            @Override
            public Boolean call(SQLDatabase database) {
                Boolean transactionSuccess = true;
                database.beginTransaction();

                // Insert metadata table entries
                for (String fieldName: fieldNamesList) {
                    ContentValues parameters = new ContentValues();
                    parameters.put("index_name", index.indexName);
                    parameters.put("index_type", index.indexType.toString());
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
                if (index.indexType == IndexType.TEXT) {
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
        if (index.indexType == IndexType.TEXT) {
            for (Map.Entry<String, Object> entry : existingIndexes.entrySet()) {
                String name = entry.getKey();
                Map<String, Object> existingIndex = (Map<String, Object>) entry.getValue();
                IndexType type = (IndexType) existingIndex.get("type");
                if (type == IndexType.TEXT &&
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
        Future<Map<String, Object>> indexes = queue.submit(new SQLCallable<Map<String,Object>>() {
            @Override
            public Map<String, Object> call(SQLDatabase database) {
                return IndexManager.listIndexesInDatabase(database);
            }
        });

        return indexes.get();
    }

    private String createIndexTableStatementForIndex(String indexName, List<String> columns) {
        String tableName = String.format(Locale.ENGLISH, "\"%s\"", IndexManager.tableNameForIndex(indexName));
        Joiner joiner = Joiner.on(" NONE,").skipNulls();
        String cols = joiner.join(columns);

        return String.format("CREATE TABLE %s ( %s NONE )", tableName, cols);
    }

    private String createIndexIndexStatementForIndex(String indexName, List<String> columns) {
        String tableName = IndexManager.tableNameForIndex(indexName);
        String sqlIndexName = tableName.concat("_index");
        Joiner joiner = Joiner.on(",").skipNulls();
        String cols = joiner.join(columns);

        return String.format(Locale.ENGLISH, "CREATE INDEX \"%s\" ON \"%s\" ( %s )", sqlIndexName, tableName, cols);
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
        String tableName = String.format(Locale.ENGLISH, "\"%s\"", IndexManager
                .tableNameForIndex(indexName));
        Joiner joiner = Joiner.on(",").skipNulls();
        String cols = joiner.join(columns);
        String settings = joiner.join(indexSettings);

        return String.format("CREATE VIRTUAL TABLE %s USING FTS4 ( %s, %s )", tableName,
                                                                              cols,
                                                                              settings);
    }

    /**
     * Iterate candidate indexNames generated from the indexNameRandom generator
     * until we find one which doesn't already exist.
     *
     * We make sure the generated name is not an index already by the list
     * of index names returned by {@link #listIndexesInDatabaseQueue()} method.
     * This is because we avoid knowing about how indexes are stored in SQLite, however
     * this means that it is not thread safe, it is possible for a new index with the same
     * name to be created after a copy of the indexes has been taken from the database.
     *
     * We allow up to 200 random name generations, which should give us many millions
     * of indexes before a name fails to be generated and makes sure this method doesn't
     * loop forever.
     *
     * @param existingIndexNames The names of the indexes that exist in the database.
     *
     * @return The generated index name or {@code null} if it failed.
     *
     */
    private static String generateIndexName(Set<String> existingIndexNames) throws ExecutionException, InterruptedException {

        String indexName = null;
        Hex hex = new Hex();

        int tries = 0;
        byte[] randomBytes = new byte[20];
        while (tries < 200 && indexName == null) {
            indexNameRandom.nextBytes(randomBytes);
            String candidate = new String(hex.encode(randomBytes), Charset.forName("UTF-8"));

            if(!existingIndexNames.contains(candidate)){
                indexName = candidate;
            }
            tries++;
        }

        if (indexName != null) {
            return indexName;
        } else {
            return null;
        }
    }

}
