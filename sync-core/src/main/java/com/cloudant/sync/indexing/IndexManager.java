/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.indexing;

import com.cloudant.android.Log;
import com.cloudant.sync.datastore.*;
import com.cloudant.sync.notifications.DatabaseClosed;
import com.cloudant.sync.sqlite.ContentValues;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLDatabaseFactory;
import com.google.common.base.Preconditions;
import com.google.common.eventbus.Subscribe;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * <p>
 * Allows indexes for a {@link Datastore} to be defined and queried.
 * </p>
 *
 * <p>
 * {@code IndexManager} instances are not thread safe.
 * </p>
 */
public class IndexManager {

    private static final String LOG_TAG = "IndexManager";
    private final SQLDatabase sqlDb;
    private final Datastore datastore;

    private final Map<String, IndexFunction> indexFunctionMap;

    private static final String EXTENSION_NAME = "com.cloudant.indexing";

    /**
     * SQLite only allows for 64 joins in a single statement (http://www.sqlite.org/limits.html)
     */
    protected static final int JOINS_LIMIT_PER_QUERY = 64;

    private static final String INDEX_TABLE_PREFIX = "_t_cloudant_sync_index_";
    private static final String INDEX_METADATA_TABLE_NAME = "_t_cloudant_sync_indexes_metadata";
    protected static final String TABLE_INDEX_NAME_FORMAT = INDEX_TABLE_PREFIX + "%s";

    private static final String SQL_SELECT_INDEX_BY_NAME = "SELECT name, type, last_sequence " +
            "FROM " + INDEX_METADATA_TABLE_NAME + " " +
            "WHERE name = ?";

    private static final String SQL_DROP_INDEX_TABLE = "DROP TABLE IF EXISTS " + INDEX_TABLE_PREFIX + "%s";

    private static final String SQL_SELECT_ALL_INDEX = "SELECT name, type, last_sequence " +
            "FROM " + INDEX_METADATA_TABLE_NAME + "";


    static final String INDEX_FIELD_NAME_PATTERN = "^[a-zA-Z][a-zA-Z0-9_]*";

    private static final Pattern pattern = Pattern.compile(INDEX_FIELD_NAME_PATTERN);

    public static final int VERSION = 1;
    public static final String[] SCHEMA_INDEX = {
            "CREATE TABLE _t_cloudant_sync_indexes_metadata ( " +
                    "        name TEXT NOT NULL, " +       // name of the index
                    "        type TEXT NOT NULL, " +
                    "        last_sequence INTEGER NOT NULL); " };

    private static final String SQL_SELECT_UNIQUE = "SELECT DISTINCT value FROM %s";

    /**
     * Constructs an {@code IndexManager} for the passed
     * {@link com.cloudant.sync.datastore.Datastore}, allowing the documents
     * in that {@code Datastore} to be indexed and queried.
     */
    public IndexManager(Datastore datastore) throws SQLException, IOException {
        this.datastore = datastore;
        this.indexFunctionMap = new HashMap<String, IndexFunction>();
        String filename = datastore.extensionDataFolder(EXTENSION_NAME)
                + File.separator + "indexes.sqlite";
        this.sqlDb = SQLDatabaseFactory.openSqlDatabase(filename);
        SQLDatabaseFactory.updateSchema(this.sqlDb, SCHEMA_INDEX, VERSION);
    }

    /**
     * <p>Registers a new index with type {@link IndexType#STRING} that indexes
     * a top-level field of documents.</p>
     *
     * <p>This method is equivalent to:</p>
     *
     * <pre>
     *     IndexFunction f = new FieldIndexFunction(field);
     *     datastore.ensureIndexed(name, IndexType.STRING, f);
     * </pre>
     *
     * @param indexName case-sensitive name of the index. Can only contain letters,
     *             digits and underscores. It must not start with a digit.
     * @param fieldName top-level field use for index values
     * @throws IndexExistsException if an index with the same
     *          name has already been registered.
     *
     * @see com.cloudant.sync.indexing.QueryBuilder
     * @see IndexType
     * @see com.cloudant.sync.indexing.FieldIndexFunction
     */
    public void ensureIndexed(String indexName, String fieldName)
            throws IndexExistsException {
        this.ensureIndexed(indexName, fieldName, IndexType.STRING);
    }

    /**
     * <p>Registers a new index with a certain type that indexes a top-level
     * field of documents.</p>
     *
     * <p>This method is equivalent to:</p>
     *
     * <pre>
     *     IndexFunction f = new FieldIndexFunction(field);
     *     datastore.ensureIndexed(name, type, f);
     * </pre>
     *
     * @param indexName case-sensitive name of the index. Can only contain letters,
     *             digits and underscores. It must not start with a digit.
     * @param fieldName top-level field use for index values
     * @param type IndexType of the index.
     * @throws IndexExistsException if an index with the same
     *          name has already been registered.
     *
     * @see com.cloudant.sync.indexing.QueryBuilder
     * @see IndexType
     * @see com.cloudant.sync.indexing.FieldIndexFunction
     */
    public void ensureIndexed(String indexName, String fieldName, IndexType type)
            throws IndexExistsException {
        this.ensureIndexed(indexName, type, new FieldIndexFunction(fieldName));
    }

    /**
     * <p>Registers an index with the datastore and make it available for
     * use within the application.</p>
     *
     * <p>The name passed to this function is the one specified at query time,
     * via the {@link com.cloudant.sync.indexing.QueryBuilder}.</p>
     *
     * <p>The call will block until the given index is up to date.</p>
     *
     * @param indexName case-sensitive name of the index. Can only contain letters,
     *             digits and underscores. It must not start with a digit.
     * @param type type of the index.
     * @param indexFunction the function to use to map between a document and
     *                      indexed value(s).
     * @throws IndexExistsException if an index
     *          with the same name has already been registered.
     *
     * @see com.cloudant.sync.indexing.QueryBuilder
     * @see com.cloudant.sync.datastore.BasicDocumentRevision
     * @see IndexType
     * @see IndexFunction
     */
    public void ensureIndexed(String indexName, IndexType type, IndexFunction indexFunction)
            throws IndexExistsException {
        IndexManager.validateIndexName(indexName);
        Preconditions.checkNotNull(type, "type cannot be null");
        Preconditions.checkNotNull(indexFunction, "indexFunction cannot be null");

        if (this.indexFunctionMap.containsKey(indexName)) {
            throw new IndexExistsException(
                    "Index already registered with a call to ensureIndexed this session: " + indexName
            );
        }

        Index index = this.getIndex(indexName);

        boolean success = false;
        this.sqlDb.beginTransaction();
        try {
            if (index == null) {
                createIndexTable(indexName, type);
                insertIndexMetaData(indexName, type);
            }

            this.indexFunctionMap.put(indexName, indexFunction);

            updateIndex(indexName);

            success = true;
            this.sqlDb.setTransactionSuccessful();
        } catch (SQLException e) {
            throw new IllegalStateException("Error creating index:" + indexName, e);
        } finally {
            this.sqlDb.endTransaction();
            if (!success) {
                this.indexFunctionMap.remove(indexName);
            }
        }
    }

    private void createIndexTable(String indexName, IndexType type) throws SQLException {
        // Create index table with name: _t_cloudant_sync_index_<indexName>
        String sql = type.createSQLTemplate(INDEX_TABLE_PREFIX, indexName);
        getDatabase().execSQL(sql);
    }

    SQLDatabase getDatabase() {
        return sqlDb;
    }

    private void insertIndexMetaData(String indexName, IndexType type) {
        IndexManager.validateIndexName(indexName);
        ContentValues v = new ContentValues();
        v.put("name", indexName);
        v.put("last_sequence", Datastore.SEQUENCE_NUMBER_START);
        v.put("type", type.toString().toUpperCase());
        long rowId = this.sqlDb.insert(INDEX_METADATA_TABLE_NAME, v);
        if (rowId < 0) {
            throw new IllegalStateException("Error during insertIndexMetaData.");
        }
    }

    /**
     * <p>Makes sure all indexes are up to date.</p>
     *
     * <p>Each index records the last document indexed using the
     * {@code Datastore} object's sequence number. This call causes the
     * changes the the {@code Datastore} since the last indexed sequence
     * number to be added to all the indexes that this {@code IndexManager}
     * knows about.</p>
     *
     * <p>Only indexes registered using {@link com.cloudant.sync.indexing.IndexManager#ensureIndexed(String, IndexType, IndexFunction)}
     * or an overloaded version on this IndexManager object will be updated (as
     * otherwise the update function isn't defined).</p>
     */
    public void updateAllIndexes() {
        Set<Index> all = this.getAllIndexes();
        for (Index index : all) {
            // Be sure to only update indexes which have been registered
            // using ensureIndexed this session.
            String name = index.getName();
            if (this.indexFunctionMap.containsKey(name)) {
                updateIndex(name);
            }
        }
    }

    private void updateIndex(String indexName) {
        Index index = this.getIndex(indexName);
        Changes changes = datastore.changes(index.getLastSequence(), 100);
        while (changes.size() > 0) {
            this.updateIndex(index, changes);
            changes = datastore.changes(changes.getLastSequence(), 100);
        }
    }

    private void updateIndex(Index index, Changes changes) {
        for (BasicDocumentRevision ob : changes.getResults()) {
            indexDocument(index, ob.getId(), ob.asMap());
        }
        updateIndexLastSequence(index.getName(), changes.getLastSequence());
    }

    private void updateIndexLastSequence(String indexName, Long lastSequence) {
        ContentValues v = new ContentValues();
        v.put("last_sequence", lastSequence);
        int row = this.sqlDb.update(INDEX_METADATA_TABLE_NAME, v, " name = ? ", new String[]{indexName});
        if (row != 1) {
            throw new IllegalStateException("Last sequence number is not updated successfully: " + indexName);
        }
    }

    private void indexDocument(Index index, String docId, Map map) {

        if (!indexFunctionMap.containsKey(index.getName())) {
            String msg = String.format("Index %s does not exist", index.getName());
            throw new IllegalArgumentException(msg);
        }

        deleteIndexRowsForDocument(index, docId);

        IndexFunction f = indexFunctionMap.get(index.getName());
        List values = f.indexedValues(index.getName(), map);
        if(values == null) {
            return;
        }
        updateIndexRowsForDocument(index, docId, values);
    }

    private void deleteIndexRowsForDocument(Index index, String docId) {
        String indexTable = constructIndexTableName(index.getName());
        this.sqlDb.delete(
                indexTable,
                " docid = ? ",
                new String[]{docId}
        );

    }
    private void updateIndexRowsForDocument(Index index, String docId, List values) {
        IndexType indexType = index.getIndexType();
        String indexTable = constructIndexTableName(index.getName());
        for(Object value : values) {
            if (indexType.valueSupported(value)) {
                ContentValues v = new ContentValues();
                v.put("docid", docId);
                indexType.putIntoContentValues(v, "value", value);
                this.sqlDb.insert(indexTable, v);
            } else {
                Log.e(LOG_TAG, "Index value ignored, docId: " + docId + ", value: " + value);
            }
        }
    }

    private String constructIndexTableName(String indexName) {
        IndexManager.validateIndexName(indexName);
        return String.format(TABLE_INDEX_NAME_FORMAT, indexName);
    }

    /**
     * <p>Returns the {@link Index} for the given index name.</p>
     *
     * @param name index name
     * @return {@link Index} for the index, or {@code null} if that index
     *      doesn't exist.
     */
    protected Index getIndex(String name) {
        IndexManager.validateIndexName(name);
        try {
            Cursor cursor = this.sqlDb.rawQuery(IndexManager.SQL_SELECT_INDEX_BY_NAME, new String[]{name});
            if (cursor.moveToFirst()) {
                String indexName = cursor.getString(0);
                IndexType type = IndexType.valueOf(cursor.getString(1));
                Long lastSequence = cursor.getLong(2);
                return new BasicIndex(indexName, type, lastSequence);
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Error getting index: " + name, e);
        }
    }

    /**
     * <p>Returns a list of the indexes in this {@code Datastore}.</p>
     *
     * @return A list of {@link Index} objects for all the indexes.
     */
    protected Set<Index> getAllIndexes() {
        Set<Index> result = new HashSet<Index>();
        try {
            Cursor cursor = this.sqlDb.rawQuery(IndexManager.SQL_SELECT_ALL_INDEX, new String[]{});
            while (cursor.moveToNext()) {
                String indexName = cursor.getString(0);
                IndexType type = IndexType.valueOf(cursor.getString(1));
                Long lastSequence = cursor.getLong(2);
                result.add(new BasicIndex(indexName, type, lastSequence));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Error getting all indexes", e);
        }
        return result;
    }

    /**
     * <p>Deletes an index.</p>
     *
     * @param indexName name of the index to delete
     */
    public void deleteIndex(String indexName) {
        IndexManager.validateIndexName(indexName);
        if (this.getIndex(indexName) == null) {
            throw new IllegalArgumentException("Index does not exist: " + indexName);
        }

        this.sqlDb.beginTransaction();
        try {

            long row = this.sqlDb.delete("_t_cloudant_sync_indexes_metadata", " name = ? ",
                    new String[]{indexName});
            if (row <= 0) {
                throw new IllegalStateException("Error deleting index:" + indexName);
            }
            this.sqlDb.execSQL(String.format(IndexManager.SQL_DROP_INDEX_TABLE, indexName));
            this.sqlDb.setTransactionSuccessful();
        } catch (SQLException e) {
            throw new IllegalStateException("Error deleting index:" + indexName, e);
        } finally {
            this.sqlDb.endTransaction();
        }
    }

    /**
     * <p>Queries for {@code DocumentRevision} objects.</p>
     *
     * <p>Using a {@link com.cloudant.sync.indexing.QueryBuilder} to create
     * the {@code query} is recommended.</p>
     *
     * <p>Otherwise, the {@code query} should have fields as keys and the
     * query for that field as a value.</p>
     *
     * @param queryWithOptions map containing the following:
     *                         <ul>
     *                         <li>"query": a Map expressing the query</li>
     *                         <li>"options": an optional Map containing query options</li>
     *                         </ul>
     * @return an arbitrarily ordered list of {@code DocumentRevision}s matching the
     *          query.
     *
     * @see QueryResult
     */

    public QueryResult query(Map<String, Map<String, Object>> queryWithOptions) {

        updateAllIndexes();

        Map<String, Object> query = queryWithOptions.get("query");
        Map<String, Object> options = queryWithOptions.get("options");

        Preconditions.checkNotNull(query, "Input query must not be null");
        Preconditions.checkArgument(query.size() <= IndexManager.JOINS_LIMIT_PER_QUERY, "One query can not use more than 64 indexes");
        for (String index : query.keySet()) {
            if (!this.indexFunctionMap.containsKey(index)) {
                throw new IllegalArgumentException("Index used in the query does not exist: " + index);
            }
        }

        IndexJoinQueryBuilder sb = new IndexJoinQueryBuilder();
        for (String indexName : query.keySet()) {
            Index index = this.getIndex(indexName);
            sb.addQueryCriterion(constructIndexTableName(indexName), query.get(indexName), index.getIndexType());
        }

        if (options.containsKey("sort_by")) {
            String value = (String)options.get("sort_by");
            if (!this.indexFunctionMap.containsKey(value)) {
                throw new IllegalArgumentException("Index used in sort_by option does not exist: " + value);
            }
            String table = constructIndexTableName(value);
            // first - if this isn't in the query criteria then it needs to be added to the join clause
            if (!query.containsKey(value)) {
                sb.addJoinForSort(table);
            }
            // is ascending/descending specified?
            SortDirection direction = SortDirection.Ascending;
            if (options.containsKey("ascending")) {
                if (options.get("ascending").getClass() != Boolean.class) {
                    throw new IllegalArgumentException("Value for ascending option must be boolean");
                }
                direction = (Boolean)options.get("ascending") ? SortDirection.Ascending : SortDirection.Descending;
            }
            else if (options.containsKey("descending")) {
                if (options.get("descending").getClass() != Boolean.class) {
                    throw new IllegalArgumentException("Value for descending option must be boolean");
                }
                direction = !(Boolean)options.get("descending") ? SortDirection.Ascending : SortDirection.Descending;
            }
            sb.addSortByOption(table, direction);
        } if (options.containsKey("offset")) {
            if (options.get("offset").getClass() != Integer.class) {
                throw new IllegalArgumentException("Value for offset option must be integer");
            }
            sb.addOffsetOption((Integer)(options.get("offset")));
        } if (options.containsKey("limit")) {
            if (options.get("limit").getClass() != Integer.class) {
                throw new IllegalArgumentException("Value for limit option must be integer");
            }
            sb.addLimitOption((Integer)(options.get("limit")));
        }

        List<String> ids = executeIndexJoinQueryForDocumentIds(sb.toSQL());
        return new BasicQueryResult(ids, datastore);
    }

    private List<String> executeIndexJoinQueryForDocumentIds(String sql) {
        List<String> ids = new ArrayList<String>();
        try {
            Cursor cursor = this.sqlDb.rawQuery(sql, new String[]{});
            while (cursor.moveToNext()) {
                ids.add(cursor.getString(0));
            }
        } catch (SQLException e) {
            throw new IllegalArgumentException("Can not execute the query: " + sql, e);
        }
        return ids;
    }

    /**
     * Return a List of unique values for the given index.
     *
     * The unique values will be determined by the database's DISTINCT operator and will depend on the
     * data type.
     *
     * @param indexName the index to fetch the unique values for.
     *
     * @return a List of unique values. The type of the array members will be determined by the
     *         Indexer's implementation.
     *
     */
    public List uniqueValues(String indexName) {

        updateAllIndexes();

        List values = new ArrayList();

        Index index = this.getIndex(indexName);

        String table = constructIndexTableName(indexName);
        String sql = String.format(SQL_SELECT_UNIQUE, table);

        try {
            Cursor cursor = this.sqlDb.rawQuery(sql, new String[]{});
            while (cursor.moveToNext()) {
                switch(index.getIndexType()) {
                    case INTEGER:
                        values.add(cursor.getInt(0));
                        break;
                    case STRING:
                        values.add(cursor.getString(0));
                        break;
                }
            }
        } catch (SQLException e) {
            throw new IllegalArgumentException("Can not execute the query: " + sql, e);
        }
        return values;
    }

    @Subscribe
    public void onDatastoreClosed(DatabaseClosed databaseClosed) {
        this.getDatabase().close();
    }

    static void validateIndexName(String name) {
        if(!pattern.matcher(name).matches()) {
            throw new IllegalArgumentException("indexName is invalid.");
        }
    }

    @Override
    public void finalize() {
        this.sqlDb.close();
    }


    public void close(){
        this.sqlDb.close();
    }
}
