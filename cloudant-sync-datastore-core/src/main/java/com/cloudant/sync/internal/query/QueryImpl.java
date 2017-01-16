/*
 * Copyright © 2014, 2017 IBM Corp. All rights reserved.
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

package com.cloudant.sync.internal.query;

import com.cloudant.sync.documentstore.Database;
import com.cloudant.sync.documentstore.encryption.KeyProvider;
import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.internal.documentstore.migrations.SchemaOnlyMigration;
import com.cloudant.sync.internal.query.callables.DeleteIndexCallable;
import com.cloudant.sync.internal.query.callables.ListIndexesCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabaseQueue;
import com.cloudant.sync.internal.util.Misc;
import com.cloudant.sync.query.FieldSort;
import com.cloudant.sync.query.Index;
import com.cloudant.sync.query.IndexType;
import com.cloudant.sync.query.Query;
import com.cloudant.sync.query.QueryException;
import com.cloudant.sync.query.QueryResult;
import com.cloudant.sync.query.Tokenizer;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

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
 *  @api_private
 */
public class QueryImpl implements Query {

    private static final String DB_FILE_NAME = "indexes.sqlite";

    private static final String INDEX_TABLE_PREFIX = "_t_cloudant_sync_query_index_";
    public static final String INDEX_METADATA_TABLE_NAME = "_t_cloudant_sync_query_metadata";

    private static final String EXTENSION_NAME = "com.cloudant.sync.query";
    private static final String INDEX_FIELD_NAME_PATTERN = "^[a-zA-Z][a-zA-Z0-9_]*$";

    private static final Logger logger = Logger.getLogger(QueryImpl.class.getName());

    private final Database database;
    private final Pattern validFieldName;

    private final SQLDatabaseQueue dbQueue;

    /**
     *  Constructs a new IndexManager which indexes documents in 'datastore'
     *  @param database The {@link Database} to index
     */
    public QueryImpl(Database database, File extensionsLocation, KeyProvider keyProvider) throws IOException, SQLException {
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
    public List<Index> listIndexes() throws QueryException {
        try {
            return DatabaseImpl.get(dbQueue.submit(new ListIndexesCallable()));
        }  catch (ExecutionException e) {
            String msg = "Failed to list indexes";
            logger.log(Level.SEVERE, msg, e);
            throw new QueryException(msg, e);
        }

    }

    @Override
    public Index createJsonIndex(List<FieldSort> fields, String indexName) throws QueryException {
        return ensureIndexed(fields, indexName, IndexType.JSON, null);
    }

    @Override
    public Index createTextIndex(List<FieldSort> fields, String indexName, Tokenizer tokenizer) throws QueryException {
        return ensureIndexed(fields, indexName, IndexType.TEXT, tokenizer);
    }

    /**
     *  Add a single, possibly compound, index for the given field names.
     *
     *  This function generates a name for the new index.
     *
     *  @param fieldNames List of field names in the sort format
     *  @param indexName Name of index to create or null to generate an index name.
     *  @param indexType The type of index (json or text currently supported)
     *  @param tokenizer
     *                       Only text indexes support settings - Ex. { "tokenize" : "simple" }
     *  @return name of created index
     */
    private Index ensureIndexed(List<FieldSort> fieldNames,
                                String indexName,
                                IndexType indexType,
                                Tokenizer tokenizer) throws QueryException {
        // synchronized to prevent race conditions in IndexCreator when looking for existing indexes
        // which have the same name or definition
        synchronized (this) {
            return IndexCreator.ensureIndexed(new Index(fieldNames,
                            indexName,
                            indexType,
                            tokenizer),
                    database,
                    dbQueue);
        }
    }

    /**
     *  Delete an index.
     *
     *  @param indexName Name of index to delete
     */
    @Override
    public void deleteIndex(final String indexName) throws QueryException {
        Misc.checkNotNullOrEmpty(indexName, "indexName");

        Future<Void> result = dbQueue.submitTransaction(new DeleteIndexCallable(indexName));

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
    public void refreshAllIndexes() throws QueryException {
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
                            List<FieldSort> sortSpecification) throws QueryException {
        Misc.checkNotNull(query, "query");

        refreshAllIndexes();

        QueryExecutor queryExecutor = new QueryExecutor(database, dbQueue);
        List<Index> indexes = listIndexes();

        return queryExecutor.find(query, indexes, skip, limit, fields, sortSpecification);
    }

    public static String tableNameForIndex(String indexName) {
        return INDEX_TABLE_PREFIX.concat(indexName);
    }

    protected Database getDatabase() {
        return database;
    }


}
