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

import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreImpl;
import com.cloudant.sync.datastore.encryption.KeyProvider;
import com.cloudant.sync.datastore.migrations.SchemaOnlyMigration;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLDatabaseFactory;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
 *  @deprecated Use methods on {@link Datastore} instead.
 */
public class IndexManager {
    private static final Logger logger = Logger.getLogger(IndexManager.class.getName());

    private final Datastore datastore;

    /**
     *  Constructs a new IndexManager which indexes documents in 'datastore'
     *  @param datastore The {@link Datastore} to index
     */
    public IndexManager(Datastore datastore) {
        this.datastore = datastore;

    }

    public void close() {
        // do nothing.
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
    public Map<String, Object> listIndexes() {
        return datastore.listIndexes();
    }

    /**
     *  Add a single, possibly compound, index for the given field names.
     *
     *  This method generates a name for the new index.
     *
     *  @param fieldNames List of field names in the sort format
     *  @return name of created index
     */
    public String ensureIndexed(List<Object> fieldNames) {
        try {
            return datastore.ensureIndexed(fieldNames);
        } catch (CheckedQueryException e) {
            return null;
        }
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
    public String ensureIndexed(List<Object> fieldNames, String indexName) {
        try {
            return datastore.ensureIndexed(fieldNames, indexName);
        } catch (CheckedQueryException e) {
            return null;
        }
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
    public String ensureIndexed(List<Object> fieldNames, String indexName, IndexType indexType) {
        try {
            return datastore.ensureIndexed(fieldNames, indexName, indexType);
        } catch (CheckedQueryException e) {
            return null;
        }
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
    public String ensureIndexed(List<Object> fieldNames,
                                String indexName,
                                IndexType indexType,
                                Map<String, String> indexSettings) {
        try {
            return datastore.ensureIndexed(fieldNames, indexName, indexType, indexSettings);
        } catch (CheckedQueryException e) {
            return null;
        }
    }

    /**
     *  Delete an index.
     *
     *  @param indexName Name of index to delete
     *  @return deletion status as true/false
     */
    public boolean deleteIndexNamed(final String indexName) {
        try {
            return datastore.deleteIndexNamed(indexName);
        } catch (CheckedQueryException e) {
            return false;
        }
    }

    /**
     *  Update all indexes.
     *
     *  @return update status as true/false
     */
    public boolean updateAllIndexes() {
        return datastore.updateAllIndexes();
    }

    public QueryResult find(Map<String, Object> query) {
        try {
            return datastore.find(query);
        } catch (CheckedQueryException e) {
            return null;
        }
    }

    public QueryResult find(Map<String, Object> query,
                            long skip,
                            long limit,
                            List<String> fields,
                            List<Map<String, String>> sortDocument) {
        try {
            return datastore.find(query, skip, limit, fields, sortDocument);
        } catch (CheckedQueryException e) {
            return null;
        }

    }


    public boolean isTextSearchEnabled() {
        try {
            return datastore.isTextSearchEnabled();
        } catch (CheckedQueryException e) {
            return false;
        }
    }

}
