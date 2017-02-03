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
import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.internal.query.callables.CreateIndexCallable;
import com.cloudant.sync.internal.query.callables.ListIndexesCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabaseFactory;
import com.cloudant.sync.internal.sqlite.SQLDatabaseQueue;
import com.cloudant.sync.internal.util.Misc;
import com.cloudant.sync.query.FieldSort;
import com.cloudant.sync.query.Index;
import com.cloudant.sync.query.IndexType;
import com.cloudant.sync.query.QueryException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  Handles creating indexes for a given DocumentStore.
 */
class IndexCreator {

    private final static String GENERATED_INDEX_NAME_PREFIX = "com.cloudant.sync.query.GeneratedIndexName.";

    private final Database database;

    private final SQLDatabaseQueue queue;

    private static final Logger logger = Logger.getLogger(IndexCreator.class.getName());

    public IndexCreator(Database database, SQLDatabaseQueue queue) {
        this.database = database;
        this.queue = queue;
    }

    protected static Index ensureIndexed(Index index,
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
    private Index ensureIndexed(Index proposedIndex) throws QueryException {

        for (FieldSort fs : proposedIndex.fieldNames) {
            if (fs.sort == FieldSort.Direction.DESCENDING) {
                throw new UnsupportedOperationException("Indexes with Direction.DESCENDING are " +
                        "not supported. To return data in descending order, create an index with " +
                        "Direction.ASCENDING fields and execute the subsequent query with " +
                        "Direction.DESCENDING fields as required.");
            }
        }

        if (proposedIndex.indexType == IndexType.TEXT) {
            if (!SQLDatabaseFactory.FTS_AVAILABLE) {
                String message = "Text search not supported.  To add support for text " +
                        "search, enable FTS compile options in SQLite.";
                logger.log(Level.SEVERE, message);
                throw new QueryException(message);
            }
        }

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

        // get existing indexes
        List<Index> existingIndexes;
        try {
            existingIndexes = DatabaseImpl.get(this.queue.submit(new ListIndexesCallable()));
        } catch (ExecutionException e) {
            String msg = "Failed to list indexes";
            logger.log(Level.SEVERE, msg, e);
            throw new QueryException(msg, e);
        }

        if(proposedIndex.indexName == null){
            // generate a name for the index.
            String indexName = GENERATED_INDEX_NAME_PREFIX + proposedIndex.toString();
            // copy over definition of existing proposed index and create it with this name
            proposedIndex = new Index(proposedIndex.fieldNames,
                    indexName,
                    proposedIndex.indexType,
                    proposedIndex.tokenizer);
        }

        for (Index existingIndex : existingIndexes) {

            // Check the index limit.  Limit is 1 for "text" indexes and unlimited for "json" indexes.
            // If there are any existing "text" indexes, throw an exception
            if (proposedIndex.indexType == IndexType.TEXT &&
                    existingIndex.indexType == IndexType.TEXT) {
                String msg = String.format("Text index limit reached. There is a limit of one " +
                        "text index per database. There is an existing text index in this " +
                        "database called \"%s\".",
                        existingIndex.indexName);
                logger.log(Level.SEVERE, msg, existingIndex.indexName);
                throw new QueryException(msg);
            }

            //
            // check if an index of this name already exists
            //
            if (existingIndex.indexName.equals(proposedIndex.indexName)) {
                if (existingIndex.equals(proposedIndex)) {
                    // we already have an index with this name and the same definition, just update
                    // it and return it
                    logger.fine(String.format("Index with name \"%s\" already exists with same " +
                            "definition", proposedIndex.indexName));

                    IndexUpdater.updateIndex(existingIndex.indexName,
                            existingIndex.fieldNames,
                            database,
                            queue);
                    return existingIndex;
                } else {
                    throw new QueryException(String.format("Index with name \"%s\" already exists" +
                            " but has different definition to requested index", proposedIndex
                            .indexName));
                }
            }
            //
            // check if an index already exists that matches the request index definition, ignoring name
            //
            // construct an index for comparison which has the same values as the proposed index
            // but the name of the one we're comparing to
            Index compare = new Index(proposedIndex.fieldNames, existingIndex.indexName, proposedIndex
                    .indexType, proposedIndex.tokenizer);
            if (compare.equals(existingIndex)) {
                // we already have an index with the same definition but a different name, just
                // update it and return it
                logger.fine(String.format("Index with name \"%s\" exists which has same " +
                        "definition of requested index \"%s\"",
                        existingIndex.indexName, proposedIndex.indexName));

                IndexUpdater.updateIndex(existingIndex.indexName,
                        existingIndex.fieldNames,
                        database,
                        queue);
                return existingIndex;
            }
        }

        final Index index = proposedIndex;

        Future<Void> result = queue.submitTransaction(new CreateIndexCallable(fieldNamesList, index));

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

        return index;

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

}
