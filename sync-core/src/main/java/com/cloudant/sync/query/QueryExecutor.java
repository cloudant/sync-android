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

import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.DatabaseUtils;
import com.google.common.collect.Sets;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  Handles querying indexes for a given datastore.
 */
class QueryExecutor {

    private final SQLDatabase database;
    private final Datastore datastore;
    private final ExecutorService queue;

    private static final Logger logger = Logger.getLogger(QueryExecutor.class.getName());

    /**
     *  Constructs a new QueryExecutor using the indexes in 'database' to find documents from
     *  'datastore'.
     */
    QueryExecutor(SQLDatabase database, Datastore datastore, ExecutorService queue) {
        this.database = database;
        this.datastore = datastore;
        this.queue = queue;
    }

    /**
     *  Execute the query passed using the selection of index definition provided.
     *
     *  The index definitions are presumed to already exist and be up to date for the
     *  datastore and database passed to the constructor.
     *
     *  @param query query to execute.
     *  @param indexes indexes to use (this method will select the most appropriate).
     *  @param skip how many results to skip before returning results to caller
     *  @param limit number of documents the result should be limited to
     *  @param fields fields to project from the result documents
     *  @param sortDocument document specifying the order to return results, null to have no sorting
     *  @return the query result
     */
    public QueryResult find(Map<String, Object> query,
                            final Map<String, Object> indexes,
                            long skip,
                            long limit,
                            List<String> fields,
                            final List<Map<String, String>> sortDocument) {
        //
        // Validate inputs
        //

        if (!validateSortDocument(sortDocument)) {
            return null;  // validate logs the error if doc is invalid
        }

        fields = normaliseFields(fields);

        if (!validateFields(fields)) {
            return null;  // validate logs error message
        }

        // normalise and validate query by passing into the executors

        query = QueryValidator.normaliseAndValidateQuery(query);

        if (query == null) {
            return null;
        }

        //
        // Execute the query
        //

        Boolean[] indexesCoverQuery = new Boolean[]{ false };
        final ChildrenQueryNode root = translateQuery(query, indexes, indexesCoverQuery);

        if (root == null) {
            return null;
        }

        Future<List<String>> result = queue.submit(new Callable<List<String>>() {
            @Override
            public List<String> call() throws Exception {
                Set<String> docIdSet = executeQueryTree(root, database);
                List<String> docIdList;

                // sorting
                if (sortDocument != null && !sortDocument.isEmpty()) {
                    docIdList = sortIds(docIdSet, sortDocument, indexes, database);
                } else {
                    docIdList = docIdSet != null ? new ArrayList<String>(docIdSet) : null;
                }

                return docIdList;
            }
        });

        List<String> docIds;
        try {
            docIds = result.get();
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Execution error encountered:", e);
            return null;
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Execution interrupted error encountered:", e);
            return null;
        }

        if (docIds == null) {
            return null;
        }

        UnindexedMatcher matcher = matcherForIndexCoverage(indexesCoverQuery, query);

        if (matcher != null) {
            String msg = "Query could not be executed using indexes alone; falling back to ";
            msg += "filtering documents themselves. This will be VERY SLOW as each candidate ";
            msg += "document is loaded from the datastore and matched against the query selector.";
            logger.log(Level.WARNING, msg);
        }

        return new QueryResult(docIds, datastore, fields, skip, limit, matcher);
    }

    protected ChildrenQueryNode translateQuery(Map<String, Object> query,
                                               Map<String, Object> indexes,
                                               Boolean[] indexesCoverQuery) {
        return (ChildrenQueryNode) QuerySqlTranslator.translateQuery(query,
                                                                     indexes,
                                                                     indexesCoverQuery);
    }

    protected UnindexedMatcher matcherForIndexCoverage(Boolean[] indexesCoverQuery,
                                                       Map<String, Object> selector) {
        return indexesCoverQuery[0] ? null : UnindexedMatcher.matcherWithSelector(selector);
    }

    private boolean validateSortDocument(List<Map<String, String>> sortDocument) {
        if (sortDocument == null || sortDocument.isEmpty()) {
            return true; // empty or null sort docs just mean "don't sort", so are valid
        }

        for (Map<String, String> clause: sortDocument) {
            if (clause.size() > 1) {
                logger.log(Level.SEVERE, "Each order clause can only be a single field");
                return false;
            }
            String fieldName = (String) clause.keySet().toArray()[0];
            String direction = clause.get(fieldName);
            if (!direction.equalsIgnoreCase("ASC") && !direction.equalsIgnoreCase("DESC")) {
                String msg = String.format("Order direction %s not valid, use `asc` or `desc`",
                                           direction);
                logger.log(Level.SEVERE, msg);
                return false;
            }
        }

        return true;
    }

    private List<String> normaliseFields(List<String> fields) {
        if (fields == null || fields.isEmpty()) {
            String msg = "Projection fields array is empty, disabling project for this query";
            logger.log(Level.WARNING, msg);
            return null;
        }

        return fields;
    }

    protected Set<String> executeQueryTree(QueryNode node, SQLDatabase db) {
        if (node instanceof AndQueryNode) {
            Set<String> accumulator = null;

            AndQueryNode andNode = (AndQueryNode) node;
            for (QueryNode qNode: andNode.children) {
                Set<String> childIds = executeQueryTree(qNode, db);
                if (accumulator == null) {
                    accumulator = new HashSet<String>(childIds);
                } else {
                    accumulator = Sets.intersection(accumulator, childIds);
                }
            }

            return accumulator;
        }
        if (node instanceof OrQueryNode) {
            Set<String> accumulator = null;

            OrQueryNode orNode = (OrQueryNode) node;
            for (QueryNode qNode: orNode.children) {
                Set<String> childIds = executeQueryTree(qNode, db);
                if (accumulator == null) {
                    accumulator = new HashSet<String>(childIds);
                } else {
                    accumulator = Sets.union(accumulator, childIds);
                }
            }

            return accumulator;
        } else if (node instanceof SqlQueryNode) {
            SqlQueryNode sqlNode = (SqlQueryNode) node;
            SqlParts sqlParts = sqlNode.sql;

            List<String> docIds = new ArrayList<String>();

            Cursor cursor = null;
            try {
                cursor = db.rawQuery(sqlParts.sqlWithPlaceHolders, sqlParts.placeHolderValues);
                while (cursor.moveToNext()) {
                    String docId = cursor.getString(0);
                    docIds.add(docId);
                }
            } catch (SQLException e) {
                logger.log(Level.SEVERE, "Failed to get a list of doc ids.", e);
            } finally {
                DatabaseUtils.closeCursorQuietly(cursor);
            }

            return new HashSet<String>(docIds);
        } else {
            return null;
        }
    }

    /**
     *  Return ordered list of document IDs using provided indexes.
     *
     *  Method assumes 'sortDocument' is valid.
     *
     *  @param docIdSet Set of current results which are sorted
     *  @param sortDocument Array of ordering definitions
     *                      '[ {"fieldName": "asc"}, {"fieldName2", "desc"} ]'
     *  @param indexes dictionary of indexes
     *  @param db database containing 'indexes' to use when sorting documents
     *  @return an ordered list of document IDs using provided indexes.
     */
    private List<String> sortIds(Set<String> docIdSet,
                                 List<Map<String, String>> sortDocument,
                                 Map<String, Object> indexes,
                                 SQLDatabase db) {
        // TODO - sort logic...
        return null;
    }

    /**
     *  Checks if the fields are valid.
     */
    private boolean validateFields(List<String> fields) {
        if (fields == null) {
            return true;
        }
        for (String field: fields) {
            if (field.contains("\\.")) {
                String msg = String.format("Projection field cannot use dotted notation: %s",
                                           field);
                logger.log(Level.SEVERE, msg);
                return false;
            }
        }

        return true;
    }

}