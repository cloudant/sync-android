/*
 * Copyright © 2014, 2016 IBM Corp. All rights reserved.
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
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLDatabaseQueue;
import com.cloudant.sync.util.DatabaseUtils;
import com.cloudant.sync.util.JSONUtils;
import com.cloudant.sync.util.Misc;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
 *
 *  @api_public
 */
public class IndexManagerImpl implements IndexManager {

    private static final String DB_FILE_NAME = "indexes.sqlite";

    private static final String INDEX_TABLE_PREFIX = "_t_cloudant_sync_query_index_";
    public static final String INDEX_METADATA_TABLE_NAME = "_t_cloudant_sync_query_metadata";

    private static final String EXTENSION_NAME = "com.cloudant.sync.query";
    private static final String INDEX_FIELD_NAME_PATTERN = "^[a-zA-Z][a-zA-Z0-9_]*$";

    private static final Logger logger = Logger.getLogger(IndexManagerImpl.class.getName());

    private final Database database;
    private final Pattern validFieldName;

    private final SQLDatabaseQueue dbQueue;

    /**
     *  Constructs a new IndexManager which indexes documents in 'datastore'
     *  @param database The {@link Database} to index
     */
    public IndexManagerImpl(Database database, File extensionsLocation, KeyProvider keyProvider) throws IOException, SQLException {
        this.database = database;
        validFieldName = Pattern.compile(INDEX_FIELD_NAME_PATTERN);

        File indexesLocation = new File(extensionsLocation, EXTENSION_NAME);
        File indexesDatabaseFile = new File(indexesLocation, DB_FILE_NAME);

        dbQueue = new SQLDatabaseQueue(indexesDatabaseFile, keyProvider);
        dbQueue.updateSchema(new SchemaOnlyMigration(QueryConstants.getSchemaVersion1()), 1);
        dbQueue.updateSchema(new SchemaOnlyMigration(QueryConstants.getSchemaVersion2()), 2);

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
    public List<Index> listIndexes() {
        try {
            return dbQueue.submit(new SQLCallable<List<Index>>() {
                @Override
                public List<Index> call(SQLDatabase database) throws Exception {
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

    protected static List<Index> listIndexesInDatabase(SQLDatabase db) throws SQLException {
        // Accumulate indexes and definitions into a map
        String sqlIndexNames = String.format("SELECT DISTINCT index_name FROM %s", INDEX_METADATA_TABLE_NAME);
        Cursor cursorIndexNames = null;
        ArrayList<Index> indexes = new ArrayList<Index>();
        try {
            cursorIndexNames = db.rawQuery(sqlIndexNames, new String[]{});
            while (cursorIndexNames.moveToNext()) {
                String indexName = cursorIndexNames.getString(0);
                String sqlIndexes = String.format("SELECT index_type, field_name, index_settings FROM %s "+
                        "WHERE index_name = ?",
                        INDEX_METADATA_TABLE_NAME);
                Cursor cursorIndexes = null;
                try {
                    cursorIndexes = db.rawQuery(sqlIndexes, new String[]{indexName});
                    IndexType indexType = null;
                    String settings = null;
                    ArrayList<FieldSort> fieldNames = new ArrayList<FieldSort>();
                    boolean first = true;
                    while (cursorIndexes.moveToNext()) {
                        if (first) {
                            // first time round
                            indexType = IndexType.enumValue(cursorIndexes.getString(0));
                            settings = cursorIndexes.getString(2);
                            first = false;
                        }
                        fieldNames.add(new FieldSort(cursorIndexes.getString(1)));
                    }
                    Map<String, Object> settingsMap = JSONUtils.deserialize(settings.getBytes(Charset.forName("UTF-8")));
                    if (settingsMap.containsKey("tokenize") && settingsMap.get("tokenize") instanceof String) {
                        indexes.add(new Index(fieldNames, indexName, indexType, (String)settingsMap.get("tokenize")));
                    } else {
                        indexes.add(new Index(fieldNames, indexName, indexType));
                    }
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursorIndexes);
                }


            }
        } finally {
            DatabaseUtils.closeCursorQuietly(cursorIndexNames);
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
    public String ensureIndexed(List<FieldSort> fieldNames) throws QueryException {
        List<Index> indexes = this.listIndexes();
        Collections.sort(fieldNames);
        for(Index index: indexes){
            Collections.sort(index.fieldNames);
            if (fieldNames.equals(filterMeta(index.fieldNames))){
                return index.indexName;
            }
        }

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
    public String ensureIndexed(List<FieldSort> fieldNames, String indexName) throws QueryException {
        return this.ensureIndexed(fieldNames, indexName, IndexType.JSON);
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
    public String ensureIndexed(List<FieldSort> fieldNames, String indexName, IndexType indexType) throws QueryException {
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
     *  @param tokenize
     *                       Only text indexes support settings - Ex. { "tokenize" : "simple" }
     *  @return name of created index
     */
    @Override
    public String ensureIndexed(List<FieldSort> fieldNames,
                                String indexName,
                                IndexType indexType,
                                String tokenize) throws QueryException {

        // If the index name is null, a name should be generated, but first we should check
        // if an index already exists that matches the request index definition.
        if (indexName == null) {
            List<Index> indexes = this.listIndexes();
            Collections.sort(fieldNames);
            for (Index index : indexes) {
                Collections.sort(index.fieldNames);
                if (fieldNames.equals(filterMeta(index.fieldNames))) {
                    return index.indexName;
                }
            }
        }

        return IndexCreator.ensureIndexed(new Index(fieldNames,
                        indexName,
                        indexType,
                        tokenize),
                database,
                dbQueue);
    }

    /**
     * Removes meta fields from a field sort list, the meta fields removed are "_id" and "_rev".
     * Meta fields will always be added if missing by the underlying indexing functions.
     */
    private List<FieldSort> filterMeta(List<FieldSort> fields){
        List<FieldSort> filtered = new ArrayList<FieldSort>();
        for (FieldSort field: fields){
            if (field.field.equals("_id") || field.field.equals("_rev")){
                continue;
            }
            filtered.add(field);
        }
        return filtered;
    }

    /**
     *  Delete an index.
     *
     *  @param indexName Name of index to delete
     */
    @Override
    public void deleteIndex(final String indexName) throws QueryException {
        Misc.checkNotNullOrEmpty(indexName, "indexName");

        Future<Void> result = dbQueue.submitTransaction(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase database) throws QueryException {
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
                    throw new QueryException(msg, e);
                }
                return null;
            }
        });

        try {
            result.get();
        } catch (ExecutionException e) {
            String message = "Execution error during index deletion";
            logger.log(Level.SEVERE, message, e);
            throw new QueryException(message, e);
        } catch (InterruptedException e) {
            String message = "Execution interrupted error during index deletion";
            logger.log(Level.SEVERE, message, e);
            throw new QueryException(message, e);
        }

    }

    /**
     *  Update all indexes.
     *
     */
    @Override
    public void updateAllIndexes() throws QueryException {
        List<Index> indexes = listIndexes();

        IndexUpdater.updateAllIndexes(indexes, database, dbQueue);
    }

    @Override
    public QueryResult find(Map<String, Object> query) throws QueryException {
        return find(query, 0, 0, null, null);
    }

    @Override
    public QueryResult find(Map<String, Object> query,
                            long skip,
                            long limit,
                            List<String> fields,
                            List<FieldSort> sortDocument) throws QueryException {
        Misc.checkNotNull(query, "query");

        updateAllIndexes();

        QueryExecutor queryExecutor = new QueryExecutor(database, dbQueue);
        List<Index> indexes = listIndexes();

        return queryExecutor.find(query, indexes, skip, limit, fields, sortDocument);
    }

    protected static String tableNameForIndex(String indexName) {
        return INDEX_TABLE_PREFIX.concat(indexName);
    }

    protected Database getDatabase() {
        return database;
    }


}
