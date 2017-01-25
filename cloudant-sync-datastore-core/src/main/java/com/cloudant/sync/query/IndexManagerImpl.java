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

//
// The metadata for an index is represented in the database table as follows:
//
//   index_name  |  index_type  |  field_name  |  last_sequence
//   -----------------------------------------------------------
//     name      |  json        |   _id        |     0
//     name      |  json        |   _rev       |     0
//     name      |  json        |   firstName  |     0
//     name      |  json        |   lastName   |     0
//     age       |  json        |   age        |     0
//
// The index itself is a single table, with a column for docId and each of the indexed fields:
//
//      _id      |   _rev      |  firstName   |  lastName
//   --------------------------------------------------------
//     miker     |  1-blah     |  Mike        |  Rhodes
//     johna     |  3-blob     |  John        |  Appleseed
//     joeb      |  2-blip     |  Joe         |  Bloggs
//
// There is a single SQLite index created on all columns of this table.
//
// N.b.: _id and _rev are automatically added to all indexes to allow them to be used to
// project DocumentRevisions without the need to load a document from the datastore.

package com.cloudant.sync.query;

import com.cloudant.sync.datastore.Database;
import com.cloudant.sync.datastore.DatabaseImpl;
import com.cloudant.sync.datastore.encryption.KeyProvider;
import com.cloudant.sync.datastore.migrations.SchemaOnlyMigration;
import com.cloudant.sync.event.Subscribe;
import com.cloudant.sync.notifications.DocumentPurged;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLDatabaseQueue;
import com.cloudant.sync.util.DatabaseUtils;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 *  Main interface to Cloudant query.
 *
 *  Use the manager to:
 *
 *  - create indexes
 *  - delete indexes
 *  - execute queries
 *  - update indexes (usually done automatically)
 *
 *  @api_public
 */
public class IndexManagerImpl implements IndexManager {

    private static final String INDEX_TABLE_PREFIX = "_t_cloudant_sync_query_index_";
    private static final String FTS_CHECK_TABLE_NAME = "_t_cloudant_sync_query_fts_check";
    public static final String INDEX_METADATA_TABLE_NAME = "_t_cloudant_sync_query_metadata";

    private static final String EXTENSION_NAME = "com.cloudant.sync.query";
    private static final String INDEX_FIELD_NAME_PATTERN = "^[a-zA-Z][a-zA-Z0-9_]*$";

    private static final Logger logger = Logger.getLogger(IndexManagerImpl.class.getName());

    private final Database database;
    private final Pattern validFieldName;

    private final SQLDatabaseQueue dbQueue;

    private boolean textSearchEnabled;

    /**
     *  Constructs a new IndexManager which indexes documents in 'datastore'
     *  @param database The {@link Database} to index
     */
    public IndexManagerImpl(Database database) {
        this.database = database;
        validFieldName = Pattern.compile(INDEX_FIELD_NAME_PATTERN);

        final String filename = ((DatabaseImpl) database).extensionDataFolder(EXTENSION_NAME) + File.separator
                                                                              + "indexes.sqlite";
        final KeyProvider keyProvider = ((DatabaseImpl) database).getKeyProvider();

        SQLDatabaseQueue queue = null;

        try {
            queue = new SQLDatabaseQueue(filename, keyProvider);
            queue.updateSchema(new SchemaOnlyMigration(QueryConstants.getSchemaVersion1()), 1);
            queue.updateSchema(new SchemaOnlyMigration(QueryConstants.getSchemaVersion2()), 2);
            textSearchEnabled = ftsAvailable(queue);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to open database", e);
            queue = null;
        }

        dbQueue = queue;

        // register so we can receive purge events
        this.database.getEventBus().register(this);
    }

    public void close() {
        this.database.getEventBus().unregister(this);
        dbQueue.shutdown();
    }

    /**
     *  Get a list of indexes and their definitions as a Map.
     *
     *  Returns:
     *
     *  { indexName: { type: json,
     *                 name: indexName,
     *                 fields: [field1, field2]
     *  }
     *
     *  @return Map of indexes in the database.
     */
    @Override
    public Map<String, Map<String, Object>> listIndexes() {
        try {
            return dbQueue.submit(new SQLCallable<Map<String, Map<String, Object>> >() {
                @Override
                public Map<String, Map<String, Object>> call(SQLDatabase database) throws Exception {
                     return IndexManagerImpl.listIndexesInDatabase(database);
                }
            }).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to list indexes",e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE,"Failed to list indexes",e);
            throw new RuntimeException(e);
        }

    }

    protected static Map<String, Map<String, Object>> listIndexesInDatabase(SQLDatabase db) {
        // Accumulate indexes and definitions into a map
        String sql;
        sql = String.format("SELECT index_name, index_type, field_name, index_settings FROM %s",
                             INDEX_METADATA_TABLE_NAME);
        Map<String, Map<String, Object>> indexes = null;
        Map<String, Object> index;
        List<String> fields = null;
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            indexes = new HashMap<String, Map<String, Object>>();
            while (cursor.moveToNext()) {
                String rowIndex = cursor.getString(0);
                IndexType rowType = IndexType.enumValue(cursor.getString(1));
                String rowField = cursor.getString(2);
                String rowSettings = cursor.getString(3);
                if (!indexes.containsKey(rowIndex)) {
                    index = new HashMap<String, Object>();
                    fields = new ArrayList<String>();
                    index.put("type", rowType);
                    index.put("name", rowIndex);
                    index.put("fields", fields);
                    if (rowSettings != null && !rowSettings.isEmpty()) {
                        index.put("settings", rowSettings);
                    }
                    indexes.put(rowIndex, index);
                }
                if (fields != null) {
                    fields.add(rowField);
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Failed to get a list of indexes in the database.", e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }

        return indexes;
    }

    /**
     *  Add a single, possibly compound, index for the given field names.
     *
     *  This method generates a name for the new index.
     *
     *  @param fieldNames List of field names in the sort format
     *  @return name of created index
     */
    @Override
    public String ensureIndexed(List<FieldSort> fieldNames) {
        return this.ensureIndexed(fieldNames, null);
    }

    /**
     *  Add a single, possibly compound, index for the given field names.
     *
     *  This function generates a name for the new index.
     *
     *  @param fieldNames List of field names in the sort format
     *  @param indexName Name of index to create or null to generate an index name.
     *  @return name of created index
     */
    @Override
    public String ensureIndexed(List<FieldSort> fieldNames, String indexName) {
        return IndexCreator.ensureIndexed(Index.getInstance(fieldNames, indexName),
                database,
                dbQueue);
    }

    /**
     *  Add a single, possibly compound, index for the given field names.
     *
     *  This function generates a name for the new index.
     *
     *  @param fieldNames List of field names in the sort format
     *  @param indexName Name of index to create or null to generate an index name.
     *  @param indexType The type of index (json or text currently supported)
     *  @return name of created index
     */
    @Override
    public String ensureIndexed(List<FieldSort> fieldNames, String indexName, IndexType indexType) {
        return ensureIndexed(fieldNames, indexName, indexType, null);
    }

    /**
     *  Add a single, possibly compound, index for the given field names.
     *
     *  This function generates a name for the new index.
     *
     *  @param fieldNames List of field names in the sort format
     *  @param indexName Name of index to create or null to generate an index name.
     *  @param indexType The type of index (json or text currently supported)
     *  @param indexSettings The optional settings to be applied to an index
     *                       Only text indexes support settings - Ex. { "tokenize" : "simple" }
     *  @return name of created index
     */
    @Override
    public String ensureIndexed(List<FieldSort> fieldNames,
                                String indexName,
                                IndexType indexType,
                                Map<String, String> indexSettings) {
        return IndexCreator.ensureIndexed(Index.getInstance(fieldNames,
                        indexName,
                        indexType,
                        indexSettings),
                database,
                dbQueue);
    }

    /**
     *  Delete an index.
     *
     *  @param indexName Name of index to delete
     */
    @Override
    public void deleteIndex(final String indexName) {
        if (indexName == null || indexName.isEmpty()) {
            logger.log(Level.WARNING, "TO delete an index, index name should be provided.");
            // TODO throw exception
        }

        Future<Boolean> result = dbQueue.submit(new SQLCallable<Boolean>() {
            @Override
            public Boolean call(SQLDatabase database) {
                Boolean transactionSuccess = true;
                database.beginTransaction();

                try {
                    // Drop the index table
                    String tableName = tableNameForIndex(indexName);
                    String sql = String.format("DROP TABLE \"%s\"", tableName);
                    database.execSQL(sql);

                    // Delete the metadata entries
                    String where = " index_name = ? ";
                    database.delete(INDEX_METADATA_TABLE_NAME, where, new String[]{ indexName });
                } catch (SQLException e) {
                    String msg = String.format("Failed to delete index: %s",indexName);
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

        boolean success;
        try {
            success = result.get();
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Execution error during index deletion:", e);
            // TODO throw exception
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Execution interrupted error during index deletion:", e);
            // TODO throw exception
        }

    }

    /**
     *  Update all indexes.
     *
     */
    @Override
    public void updateAllIndexes() {
        Map<String, Map<String, Object>> indexes = listIndexes();

        IndexUpdater.updateAllIndexes(indexes, database, dbQueue);
    }

    @Override
    public QueryResult find(Map<String, Object> query) {
        return find(query, 0, 0, null, null);
    }

    @Override
    public QueryResult find(Map<String, Object> query,
                            long skip,
                            long limit,
                            List<String> fields,
                            List<FieldSort> sortDocument) {
        if (query == null) {
            logger.log(Level.SEVERE, "-find called with null selector; bailing.");
            return null;
        }

        updateAllIndexes();

        QueryExecutor queryExecutor = new QueryExecutor(database, dbQueue);
        Map<String, Map<String, Object>> indexes = listIndexes();

        return queryExecutor.find(query, indexes, skip, limit, fields, sortDocument);
    }

    protected static String tableNameForIndex(String indexName) {
        return INDEX_TABLE_PREFIX.concat(indexName);
    }

    protected Database getDatabase() {
        return database;
    }

    /**
     * Check that support for text search exists in SQLite by building a VIRTUAL table.
     *
     * @return text search enabled setting
     */
    protected static boolean ftsAvailable(SQLDatabaseQueue q) {
        boolean ftsAvailable = false;
        Future<Boolean> result = q.submitTransaction(new SQLCallable<Boolean>() {
            @Override
            public Boolean call(SQLDatabase db) {
                Boolean transactionSuccess = true;
                db.beginTransaction();
                List<String> statements = new ArrayList<String>();
                statements.add(String.format("CREATE VIRTUAL TABLE %s USING FTS4 ( col )",
                                             FTS_CHECK_TABLE_NAME));
                statements.add(String.format("DROP TABLE %s", FTS_CHECK_TABLE_NAME));

                for (String statement : statements) {
                    try {
                        db.execSQL(statement);
                    } catch (SQLException e) {
                        // An exception here means that FTS is not enabled in SQLite.
                        // Logging happens in calling method.
                        transactionSuccess = false;
                        break;
                    }
                }

                if (transactionSuccess) {
                    db.setTransactionSuccessful();
                }
                db.endTransaction();

                return  transactionSuccess;
            }
        });

        try {
            ftsAvailable = result.get();
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Execution error encountered:", e);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Execution interrupted error encountered:", e);
        }

        return ftsAvailable;
    }

    @Override
    public boolean isTextSearchEnabled() {
        if (!textSearchEnabled) {
            logger.log(Level.INFO, "Text search is currently not supported.  " +
                    "To enable text search recompile SQLite with " +
                    "the full text search compile options enabled.");
        }
        return textSearchEnabled;
    }

    @Subscribe
    public void onPurge(DocumentPurged documentPurged) {
        // TODO remove from index
    }

}
