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
import com.cloudant.sync.datastore.Database;
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLDatabaseQueue;
import com.cloudant.sync.util.Misc;

import org.apache.commons.codec.binary.Hex;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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

    private final Database database;
    private static Random indexNameRandom = new Random();

    private final SQLDatabaseQueue queue;

    private static final Logger logger = Logger.getLogger(IndexCreator.class.getName());

    public IndexCreator(Database database, SQLDatabaseQueue queue) {
        this.database = database;
        this.queue = queue;
    }

    protected static String ensureIndexed(Index index,
                                          Database database,
                                          SQLDatabaseQueue queue) throws QueryException {
        IndexCreator executor = new IndexCreator(database, queue);

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
    private String ensureIndexed(Index proposedIndex) throws QueryException {
        Misc.checkNotNull(proposedIndex, "proposedIndex");

        if (proposedIndex.indexType == IndexType.TEXT) {
            if (!IndexManagerImpl.ftsAvailable(queue)) {
                String message = "Text search not supported.  To add support for text " +
                        "search, enable FTS compile options in SQLite.";
                logger.log(Level.SEVERE, message);
                throw new QueryException(message);
            }
        }

        // update proposedIndex with all fields ascending
        ArrayList<FieldSort> updatedFields = new ArrayList<FieldSort>();
        for (FieldSort f : proposedIndex.fieldNames) {
            updatedFields.add(new FieldSort(f.field, FieldSort.Direction.ASCENDING));
        }
        proposedIndex = new Index(updatedFields, proposedIndex.indexName, proposedIndex.indexType, proposedIndex.tokenize);

        final List<FieldSort> fieldNamesList = proposedIndex.fieldNames;

        Set<String> uniqueNames = new HashSet<String>();
        for (FieldSort fieldName: fieldNamesList) {
            uniqueNames.add(fieldName.field);
            Misc.checkArgument(validFieldName(fieldName.field), "Field "+fieldName.field+" is not valid");
        }

        // Check there are no duplicate field names in the array
        Misc.checkArgument(uniqueNames.size() == fieldNamesList.size(), String.format("Cannot create index with duplicated field names %s"
                , proposedIndex.fieldNames));

        // Prepend _id and _rev if it's not in the array
        if (!uniqueNames.contains("_rev")) {
            fieldNamesList.add(0, new FieldSort("_rev"));
        }

        if (!uniqueNames.contains("_id")) {
            fieldNamesList.add(0, new FieldSort("_id"));
        }

        // Check the index limit.  Limit is 1 for "text" indexes and unlimited for "json" indexes.
        // Then check whether the index already exists; return success if it does and is same,
        // else fail.
        try {

            List<Index> existingIndexes = listIndexesInDatabaseQueue();
            HashMap<String, Index> existingIndexNames = new HashMap<String, Index>();
            for (Index index : existingIndexes) {
                existingIndexNames.put(index.indexName, index);
            }

            if(proposedIndex.indexName == null){
                // generate a name for the index.
                String indexName = IndexCreator.generateIndexName(existingIndexNames.keySet());
                if(indexName == null){
                    String message = "Failed to generate unique index name";
                    logger.warning(message);
                    throw new QueryException(message);
                }

                proposedIndex = new Index(proposedIndex.fieldNames,
                                          indexName,
                        proposedIndex.indexType,
                        proposedIndex.tokenize);
            }

            if (indexLimitReached(proposedIndex, existingIndexes)) {
                String msg = String.format("Index limit reached.  Cannot create index %s.",
                                           proposedIndex.indexName);
                logger.log(Level.SEVERE, msg);
                throw new QueryException(msg);
            }
            if (existingIndexNames.containsKey(proposedIndex.indexName)) {
                Index existingIndex = existingIndexNames.get(proposedIndex.indexName);
                if (proposedIndex.equals(existingIndex)) {
                    // index name and fields match existing index, update index and return
                    IndexUpdater.updateIndex(proposedIndex.indexName,
                            fieldNamesList,
                            database,
                            queue);
                    return proposedIndex.indexName;
                }
            }
        } catch (ExecutionException e) {
            String message = "Execution error encountered in listIndexesInDatabaseQueue";
            logger.log(Level.SEVERE, message, e);
            throw new QueryException(message, e);
        } catch (InterruptedException e) {
            String message = "Execution interrupted error encountered in listIndexesInDatabaseQueue";
            logger.log(Level.SEVERE, message, e);
            throw new QueryException(message, e);
        }

        final Index index = proposedIndex;
        Future<Void> result = queue.submitTransaction(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase database) throws QueryException {

                // Insert metadata table entries
                for (FieldSort fieldName: fieldNamesList) {
                    ContentValues parameters = new ContentValues();
                    parameters.put("index_name", index.indexName);
                    parameters.put("index_type", index.indexType.toString());
                    parameters.put("index_settings", index.settingsAsJSON());
                    parameters.put("field_name", fieldName.field);
                    parameters.put("last_sequence", 0);
                    long rowId = database.insert(IndexManagerImpl.INDEX_METADATA_TABLE_NAME,
                                                 parameters);
                    if (rowId < 0) {
                        throw new QueryException("Error inserting index metadata");
                    }
                }

                // Create SQLite data structures to support the index
                // For JSON index type create a SQLite table and a SQLite index
                // For TEXT index type create a SQLite virtual table
                List<String> columnList = new ArrayList<String>();
                for (FieldSort field: fieldNamesList) {
                    columnList.add("\"" + field.field + "\"");
                }

                List<String> statements = new ArrayList<String>();
                if (index.indexType == IndexType.TEXT) {
                    String settings = String.format("tokenize=%s", index.tokenize);
                    statements.add(createVirtualTableStatementForIndex(index.indexName,
                            columnList,
                            Collections.singletonList(settings)));
                } else {
                    statements.add(createIndexTableStatementForIndex(index.indexName, columnList));
                    statements.add(createIndexIndexStatementForIndex(index.indexName, columnList));
                }
                for (String statement : statements) {
                    try {
                        database.execSQL(statement);
                    } catch (SQLException e) {
                        String msg = String.format("Index creation error occurred (%s):",statement);
                        throw new QueryException(msg, e);
                    }
                }
                return null;
            }
        });

        // Update the new index if it's been created
        try {
            result.get();
        } catch (ExecutionException e) {
            String message = "Execution error encountered whilst inserting index metadata";
            logger.log(Level.SEVERE, message, e);
            throw new QueryException(message, e);
        } catch (InterruptedException e) {
            String message = "Execution interrupted error encountered whilst inserting index metadata";
            logger.log(Level.SEVERE, message, e);
            throw new QueryException(message, e);
        }

        IndexUpdater.updateIndex(index.indexName,
                fieldNamesList,
                database,
                queue);

        return index.indexName;
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
     * Based on the proposed index and the list of existing indexes, this method checks
     * whether another index can be created.  Currently the limit for TEXT indexes is 1.
     * JSON indexes are unlimited.
     *
     * @param index the proposed index
     * @param existingIndexes the list of already existing indexes
     * @return whether the index limit has been reached
     */
    protected static boolean indexLimitReached(Index index, List<Index> existingIndexes) {
        if (index.indexType == IndexType.TEXT) {
            for (Index existingIndex : existingIndexes) {
                IndexType type = existingIndex.indexType;
                if (type == IndexType.TEXT &&
                    !existingIndex.indexName.equalsIgnoreCase(index.indexName)) {
                    logger.log(Level.SEVERE,
                            String.format("The text index %s already exists.  ", existingIndex.indexName) +
                            "One text index per datastore permitted.  " +
                            String.format("Delete %s and recreate %s.", existingIndex.indexName, index.indexName));
                    return true;
                }
            }
        }

        return false;
    }

    private List<Index> listIndexesInDatabaseQueue() throws ExecutionException,
                                                                    InterruptedException {
        Future<List<Index>> indexes = queue.submit(new SQLCallable<List<Index>>() {
            @Override
            public List<Index> call(SQLDatabase database) throws SQLException {
                return IndexManagerImpl.listIndexesInDatabase(database);
            }
        });

        return indexes.get();
    }

    private String createIndexTableStatementForIndex(String indexName, List<String> columns) {
        String tableName = String.format(Locale.ENGLISH, "\"%s\"", IndexManagerImpl.tableNameForIndex(indexName));
        String cols = Misc.join(" NONE, ", columns);

        return String.format("CREATE TABLE %s ( %s NONE )", tableName, cols);
    }

    private String createIndexIndexStatementForIndex(String indexName, List<String> columns) {
        String tableName = IndexManagerImpl.tableNameForIndex(indexName);
        String sqlIndexName = tableName.concat("_index");
        String cols = Misc.join(",", columns);

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
        String tableName = String.format(Locale.ENGLISH, "\"%s\"", IndexManagerImpl
                .tableNameForIndex(indexName));
        String cols = Misc.join(",", columns);
        String settings = Misc.join(",", indexSettings);

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
