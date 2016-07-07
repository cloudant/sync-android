/*
 *   Copyright (c) 2016 IBM Corporation. All rights reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 *   except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software distributed under the
 *   License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *   either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

package com.cloudant.sync.datastore;

import com.cloudant.sync.datastore.encryption.KeyProvider;
import com.cloudant.sync.datastore.encryption.NullKeyProvider;
import com.cloudant.sync.datastore.migrations.SchemaOnlyMigration;
import com.cloudant.sync.query.CheckedQueryException;
import com.cloudant.sync.query.Index;
import com.cloudant.sync.query.IndexCreator;
import com.cloudant.sync.query.IndexType;
import com.cloudant.sync.query.IndexUpdater;
import com.cloudant.sync.query.QueryConstants;
import com.cloudant.sync.query.QueryExecutor;
import com.cloudant.sync.query.QueryResult;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLDatabaseQueue;
import com.cloudant.sync.sqlite.SQLQueueCallable;
import com.cloudant.sync.util.DatabaseUtils;

import java.io.File;
import java.io.IOException;
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
 * Created by rhys on 04/07/2016.
 */
public class QueryableDatastore extends DatastoreImpl {


    private static final String FTS_CHECK_TABLE_NAME = "_t_cloudant_sync_query_fts_check";


    private static final String EXTENSION_NAME = "com.cloudant.sync.query";
    private static final String INDEX_FIELD_NAME_PATTERN = "^[a-zA-Z][a-zA-Z0-9_]*$";

    private static final Logger logger = Logger.getLogger(QueryableDatastore.class.getName());

    private final Pattern validFieldName;

    private final SQLDatabaseQueue indexDbQueue;

    private boolean textSearchEnabled;

    public QueryableDatastore(String dir, String name) throws SQLException, IOException, DatastoreException {
        this(dir, name, new NullKeyProvider());
    }

    public QueryableDatastore(String dir, String name, KeyProvider provider) throws SQLException, IOException, DatastoreException {
        super(dir, name, provider);

        validFieldName = Pattern.compile(INDEX_FIELD_NAME_PATTERN);

        final String filename = this.extensionDataFolder(EXTENSION_NAME) + File.separator
                + "indexes.sqlite";

        indexDbQueue = new SQLDatabaseQueue(filename, provider);
        indexDbQueue.updateSchema(new SchemaOnlyMigration(QueryConstants.getSchemaVersion1()), 1);
        indexDbQueue.updateSchema(new SchemaOnlyMigration(QueryConstants.getSchemaVersion2()), 2);
        textSearchEnabled = ftsAvailable(indexDbQueue);

    }

    /**
     * Check that support for text search exists in SQLite by building a VIRTUAL table.
     *
     * @return text search enabled setting
     */
    public static boolean ftsAvailable(SQLDatabaseQueue q) {
        boolean ftsAvailable = false;
        Future<Boolean> result = q.submitTransaction(new SQLQueueCallable<Boolean>() {
            @Override
            public Boolean call(SQLDatabase db) throws Exception {
                List<String> statements = new ArrayList<String>();
                statements.add(String.format("CREATE VIRTUAL TABLE %s USING FTS4 ( col )",
                        FTS_CHECK_TABLE_NAME));
                statements.add(String.format("DROP TABLE %s", FTS_CHECK_TABLE_NAME));

                for (String statement : statements) {
                    db.execSQL(statement);
                }
                // If we made it this far, FTS is enabled, otherwise `execSQL` will throw.
                // Logging happens in calling method.
                return  Boolean.TRUE;
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
    public Map<String, Object> listIndexes() {
        try {
            return indexDbQueue.submit(new SQLQueueCallable<Map<String, Object> >() {
                @Override
                public Map<String, Object> call(SQLDatabase database) throws Exception {
                    return QueryableDatastore.listIndexesInDatabase(database);
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

    public static Map<String, Object> listIndexesInDatabase(SQLDatabase db) {
        // Accumulate indexes and definitions into a map
        String sql;
        sql = String.format("SELECT index_name, index_type, field_name, index_settings FROM %s",
                QueryConstants.INDEX_METADATA_TABLE_NAME);
        Map<String, Object> indexes = null;
        Map<String, Object> index;
        List<String> fields = null;
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            indexes = new HashMap<String, Object>();
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

    @Override
    public String ensureIndexed(List<Object> fieldNames) throws CheckedQueryException {
        return this.ensureIndexed(fieldNames, null);
    }

    @Override
    public String ensureIndexed(List<Object> fieldNames, String indexName) throws CheckedQueryException {
        return IndexCreator.ensureIndexed(Index.getInstance(fieldNames, indexName),
                this,
                indexDbQueue);
    }

    @Override
    public String ensureIndexed(List<Object> fieldNames, String indexName, IndexType indexType) throws CheckedQueryException {
        return ensureIndexed(fieldNames, indexName, indexType, null);
    }

    @Override
    public String ensureIndexed(List<Object> fieldNames, String indexName, IndexType indexType, Map<String, String> indexSettings) throws CheckedQueryException {
        return IndexCreator.ensureIndexed(Index.getInstance(fieldNames,
                indexName,
                indexType,
                indexSettings),
                this,
                indexDbQueue);
    }

    @Override
    public boolean deleteIndexNamed(final String indexName) throws CheckedQueryException {
        if (indexName == null || indexName.isEmpty()) {
            logger.log(Level.WARNING, "To delete an index, index name should be provided.");
            return false;
        }

        Future<Boolean> result = indexDbQueue.submitTransaction(new SQLQueueCallable<Boolean>() {
            @Override
            public Boolean call(SQLDatabase database) throws Exception {
                // Drop the index table
                String tableName = QueryConstants.tableNameForIndex(indexName);
                String sql = String.format("DROP TABLE \"%s\"", tableName);
                database.execSQL(sql);

                // Delete the metadata entries
                String where = " index_name = ? ";
                database.delete(QueryConstants.INDEX_METADATA_TABLE_NAME, where, new String[]{ indexName });

                return Boolean.TRUE;
            }
        });

        boolean success;
        try {
            success = result.get();
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Execution error during index deletion:", e);
            return false;
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Execution interrupted error during index deletion:", e);
            return false;
        }

        return success;
    }

    @Override
    public boolean updateAllIndexes() {
        Map<String, Object> indexes = listIndexes();

        return IndexUpdater.updateAllIndexes(indexes, this, indexDbQueue);
    }

    @Override
    public boolean isTextSearchEnabled() throws CheckedQueryException {
        if (!textSearchEnabled) {
            logger.log(Level.INFO, "Text search is currently not supported.  " +
                    "To enable text search recompile SQLite with " +
                    "the full text search compile options enabled.");
        }
        return textSearchEnabled;
    }

    @Override
    public QueryResult find(Map<String, Object> query) throws CheckedQueryException {
        return find(query, 0, 0, null, null);
    }

    @Override
    public QueryResult find(Map<String, Object> query, long skip, long limit, List<String>
            fields, List<Map<String, String>> sortDocument) throws CheckedQueryException {
        if (query == null) {
            logger.log(Level.SEVERE, "-find called with null selector; bailing.");
            throw new CheckedQueryException(); //FIXME: use a proper exception.
        }

        if (!updateAllIndexes()) {
            throw new CheckedQueryException(); //FIXME: use a proper exception.
        }

        QueryExecutor queryExecutor = new QueryExecutor(this, indexDbQueue);
        Map<String, Object> indexes = listIndexes();

        return queryExecutor.find(query, indexes, skip, limit, fields, sortDocument);
    }

    @Override
    public void close() {
        indexDbQueue.shutdown();
        super.close();
    }

    public SQLDatabaseQueue getQueryQueue(){
        return this.indexDbQueue;
    }


}
