//  Copyright (c) 2014 Cloudant. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
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
// project CDTDocumentRevisions without the need to load a document from the datastore.
//

package com.cloudant.sync.query;

import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLDatabaseFactory;
import com.cloudant.sync.util.DatabaseUtils;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class IndexManager {

    public static final String INDEX_TABLE_PREFIX = "_t_cloudant_sync_query_index_";
    public static final String INDEX_METADATA_TABLE_NAME = "_t_cloudant_sync_query_metadata";

    private static final String EXTENSION_NAME = "com.cloudant.sync.query";
    private static final String INDEX_FIELD_NAME_PATTERN = "^[a-zA-Z][a-zA-Z0-9_]*$";

    public static final int VERSION = 1;

    private static final Logger logger = Logger.getLogger(IndexManager.class.getName());

    private final Datastore datastore;
    private final SQLDatabase database;
    private final Pattern validFieldName;
    private final ExecutorService queue;

    public IndexManager(Datastore datastore) {
        this.datastore = datastore;
        validFieldName = Pattern.compile(INDEX_FIELD_NAME_PATTERN);
        queue = Executors.newSingleThreadExecutor();

        String filename = datastore.extensionDataFolder(EXTENSION_NAME) + File.separator
                                                                        + "indexes.sqlite";
        SQLDatabase sqlDatabase = null;
        try {
            sqlDatabase = SQLDatabaseFactory.openSqlDatabase(filename);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Problem opening or creating database.", e);
        }
        database = sqlDatabase;
        try {
            String[] schemaIndex = { "CREATE TABLE " + INDEX_METADATA_TABLE_NAME + " ( "
                                                     + "        index_name TEXT NOT NULL, "
                                                     + "        index_type TEXT NOT NULL, "
                                                     + "        field_name TEXT NOT NULL, "
                                                     + "        last_sequence INTEGER NOT NULL);" };
            SQLDatabaseFactory.updateSchema(database, schemaIndex, VERSION);
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Failed to update schema.", e);
        }
    }

    public void close() {
        queue.shutdown();
        database.close();
    }

    /**
     *  Get a list of indexes and their definitions as a Map.
     *
     *  @return Map of indexes in the database.
     */
    public Map<String, Object> listIndexes() {
        return IndexManager.listIndexesInDatabase(database);
    }

    protected static Map<String, Object> listIndexesInDatabase(SQLDatabase db) {
        // Accumulate indexes and definitions into a map
        String sql = String.format("SELECT index_name, index_type, field_name FROM %s",
                                   INDEX_METADATA_TABLE_NAME);
        Map<String, Object> indexes = null;
        Map<String, Object> index;
        List<String> fields = null;
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            indexes = new HashMap<String, Object>();
            while (cursor.moveToNext()) {
                String rowIndex = cursor.getString(0);
                String rowType = cursor.getString(1);
                String rowField = cursor.getString(2);
                if (!indexes.containsKey(rowIndex)) {
                    index = new HashMap<String, Object>();
                    fields = new ArrayList<String>();
                    index.put("type", rowType);
                    index.put("name", rowIndex);
                    index.put("fields", fields);
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
    public String ensureIndexed(ArrayList<Object> fieldNames) {
        logger.log(Level.SEVERE, "ensureIndexed(fieldNames) not implemented.");

        return null;
    }

    /**
     *  Add a single, possibly compound, index for the given field names.
     *
     *  This function generates a name for the new index.
     *
     *  @param fieldNames List of field names in the sort format
     *  @param indexName Name of index to create
     *  @return name of created index
     */
    public String ensureIndexed(ArrayList<Object> fieldNames, String indexName) {
        return ensureIndexed(fieldNames, indexName, "json");
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
    public String ensureIndexed(ArrayList<Object> fieldNames, String indexName, String indexType) {
        if (fieldNames == null || fieldNames.isEmpty()) {
            return null;
        }

        if (indexName == null || indexName.isEmpty()) {
            return null;
        }

        if (indexType == null || !indexType.equalsIgnoreCase("json")) {
            return null;
        }

        return IndexCreator.ensureIndexed(fieldNames,
                                          indexName,
                                          indexType,
                                          database,
                                          datastore,
                                          queue);
    }

    /**
     *  Delete an index.
     *
     *  @param indexName Name of index to delete
     *  @return deletion status as true/false
     */
    public boolean deleteIndexNamed(String indexName) {
        boolean success = true;

        // TODO - implement method
        // Drop the index table
        // Delete the metadata entries

        logger.log(Level.SEVERE, "deleteIndexNamed(indexName) not implemented.");

        return false;
    }

    /**
     *  Update all indexes.
     *
     *  @return update status as true/false
     */
    public boolean updateAllIndexes() {

        // TODO
        // To start with, assume top-level fields only

        Map<String, Object> indexes = listIndexes();

        return IndexUpdater.updateAllIndexes(indexes, database, datastore, queue);
    }

    public QueryResultSet find(Map<String, Object> query) {

        // TODO - implement method

        logger.log(Level.SEVERE, "find(query) not implemented.");

        return null;
    }

    protected Datastore getDatastore() {
        return datastore;
    }

    protected ExecutorService getQueue() {
        return queue;
    }

    protected SQLDatabase getDatabase() {
        return database;
    }
}
