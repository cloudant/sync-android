/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2014 Cloudant, Inc. All rights reserved.
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
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.query.FieldSort;
import com.cloudant.sync.query.Index;
import com.cloudant.sync.query.QueryException;
import com.cloudant.sync.query.QueryResult;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.sqlite.SQLDatabaseQueue;
import com.cloudant.sync.internal.util.DatabaseUtils;
import com.cloudant.sync.internal.util.Misc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  Handles querying indexes for a given datastore.
 */
class QueryExecutor {

    private final Database database;
    private final SQLDatabaseQueue queue;

    private static final Logger logger = Logger.getLogger(QueryExecutor.class.getName());

    private static final int SMALL_RESULT_SET_SIZE_THRESHOLD = 500;

    /**
     *  Constructs a new QueryExecutor using the indexes in 'database' to find documents from
     *  'datastore'.
     */
    QueryExecutor(Database database, SQLDatabaseQueue queue) {
        this.database = database;
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
                            final List<Index> indexes,
                            long skip,
                            long limit,
                            List<String> fields,
                            final List<FieldSort> sortDocument) throws QueryException {
        //
        // Validate inputs
        //

        fields = normaliseFields(fields);

        // will throw IllegalArgumentException if there are invalid fields
        validateFields(fields);

        // normalise and validate query by passing into the executors

        query = QueryValidator.normaliseAndValidateQuery(query);

        //
        // Execute the query
        //

        Boolean[] indexesCoverQuery = new Boolean[]{ false };
        final ChildrenQueryNode root = translateQuery(query, indexes, indexesCoverQuery);



        Future<List<String>> result = queue.submit(new SQLCallable<List<String>>() {
            @Override
            public List<String> call(SQLDatabase database) throws Exception {
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
            String message = "Execution error encountered";
            logger.log(Level.SEVERE, message, e);
            throw new QueryException(message, e.getCause());
        } catch (InterruptedException e) {
            String message = "Execution interrupted error encountered";
            logger.log(Level.SEVERE, message, e);
            throw new QueryException(message, e.getCause());
        }

        if (docIds == null) {
            return null;
        }

        UnindexedMatcher matcher = matcherForIndexCoverage(indexesCoverQuery, query);

        if (matcher != null) {
            String msg = "query could not be executed using indexes alone; falling back to ";
            msg += "filtering documents themselves. This will be VERY SLOW as each candidate ";
            msg += "document is loaded from the datastore and matched against the query selector.";
            logger.log(Level.WARNING, msg);
        }

        return new QueryResult(docIds, database, fields, skip, limit, matcher);
    }

    protected ChildrenQueryNode translateQuery(Map<String, Object> query,
                                               List<Index> indexes,
                                               Boolean[] indexesCoverQuery) {
        return (ChildrenQueryNode) QuerySqlTranslator.translateQuery(query,
                                                                     indexes,
                                                                     indexesCoverQuery);
    }

    protected UnindexedMatcher matcherForIndexCoverage(Boolean[] indexesCoverQuery,
                                                       Map<String, Object> selector) {
        return indexesCoverQuery[0] ? null : UnindexedMatcher.matcherWithSelector(selector);
    }

    /**
     *  Checks if the fields are valid.
     */
    private void validateFields(List<String> fields) {
        if (fields == null) {
            return;
        }
        List<String> badFields = new ArrayList<String>();
        for (String field: fields) {
            if (field.contains(".")) {
                badFields.add(field);
            }
        }

        if (!badFields.isEmpty()) {
            String msg = String.format("Projection field(s) cannot use dotted notation: %s",
                    Misc.join(", ", badFields));
            logger.log(Level.SEVERE, msg);
            throw new IllegalArgumentException(msg);
        }
    }

    private List<String> normaliseFields(List<String> fields) {
        if (fields == null || fields.isEmpty()) {
            String msg = "Projection fields array is empty, disabling project for this query";
            logger.log(Level.WARNING, msg);
            return null;
        }

        return fields;
    }

    protected Set<String> executeQueryTree(QueryNode node, SQLDatabase db) throws QueryException {
        if (node instanceof AndQueryNode) {
            Set<String> accumulator = null;

            AndQueryNode andNode = (AndQueryNode) node;
            for (QueryNode qNode: andNode.children) {
                Set<String> childIds = executeQueryTree(qNode, db);
                if (childIds == null) {
                    continue;
                }
                if (accumulator == null) {
                    accumulator = new HashSet<String>(childIds);
                } else {
                    accumulator.retainAll(childIds);
                }
            }

            return accumulator;
        }
        if (node instanceof OrQueryNode) {
            Set<String> accumulator = null;

            OrQueryNode orNode = (OrQueryNode) node;
            for (QueryNode qNode: orNode.children) {
                Set<String> childIds = executeQueryTree(qNode, db);
                if (childIds == null) {
                    continue;
                }
                if (accumulator == null) {
                    accumulator = new HashSet<String>(childIds);
                } else {
                    accumulator.addAll(childIds);
                }
            }

            return accumulator;
        } else if (node instanceof SqlQueryNode) {
            SqlQueryNode sqlNode = (SqlQueryNode) node;

            try {
                List<String> docIds = null;

                if (sqlNode.sql != null) {
                    docIds = new ArrayList<String>();
                    SqlParts sqlParts = sqlNode.sql;
                    Cursor cursor = null;
                    try {
                        cursor = db.rawQuery(sqlParts.sqlWithPlaceHolders, sqlParts.placeHolderValues);

                        while (cursor.moveToNext()) {
                            String docId = cursor.getString(0);
                            docIds.add(docId);
                        }
                    } catch (SQLException e) {
                        String message = "Failed to get a list of doc ids.";
                        logger.log(Level.SEVERE, message, e);
                        throw new QueryException(message, e);
                    } finally {
                        DatabaseUtils.closeCursorQuietly(cursor);
                    }
                } else {
                    // No SQL exists so we are now forced to go directly to the
                    // document datastore to retrieve the list of document ids.
                    docIds = database.getIds();
                }

                return new HashSet<String>(docIds);
            } catch (DocumentStoreException dse) {
                String message = "Failed to get a list of doc ids.";
                logger.log(Level.SEVERE, message, dse);
                throw new QueryException(message, dse.getCause());
            }
        } else {
            throw new QueryException("Unexpected node type "+node);
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
                                 List<FieldSort> sortDocument,
                                 List<Index> indexes,
                                 SQLDatabase db) throws QueryException {
        boolean smallResultSet = (docIdSet.size() < SMALL_RESULT_SET_SIZE_THRESHOLD);
        SqlParts orderBy = sqlToSortIds(docIdSet, sortDocument, indexes);

        List<String> sortedIds = null;
        // The query will iterate through a sorted list of docIds.
        // This means that if we create a new array and add entries
        // to that array as we iterate through the result set which
        // are part of the query's results, we'll end up with an
        // ordered set of results.
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(orderBy.sqlWithPlaceHolders, orderBy.placeHolderValues);
            while (cursor.moveToNext()) {
                if (sortedIds == null) {
                    sortedIds = new ArrayList<String>();
                }

                String candidateId = cursor.getString(0);

                if (smallResultSet) {
                    sortedIds.add(candidateId);
                } else {
                    if (docIdSet.contains(candidateId)) {
                        sortedIds.add(candidateId);
                    }
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Failed to sort doc ids.", e);
            return null;
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }

        return sortedIds;
    }

    /**
     *  Return SQL to get ordered list of docIds.
     *
     *  Method assumes `sortDocument` is valid.
     *
     *  @param docIdSet The original set of document ids
     *  @param sortDocument Array of ordering definitions
     *                      [ { "fieldName" : "asc" }, { "fieldName2", "desc" } ]
     *  @param indexes dictionary of indexes
     *  @return the SQL containing the order by clause
     */
    protected static SqlParts sqlToSortIds(Set<String> docIdSet,
                                  List<FieldSort> sortDocument,
                                  List<Index> indexes) throws QueryException {
        String chosenIndex = chooseIndexForSort(sortDocument, indexes);
        if (chosenIndex == null) {
            String msg = String.format(Locale.ENGLISH, "No single index can satisfy order %s", sortDocument);
            logger.log(Level.SEVERE, msg);
            throw new QueryException(msg);
        }

        String indexTable = QueryImpl.tableNameForIndex(chosenIndex);

        // for small result sets:
        // SELECT _id FROM idx WHERE _id IN (?, ?) ORDER BY fieldName ASC, fieldName2 DESC
        // for large result sets:
        // SELECT _id FROM idx ORDER BY fieldName ASC, fieldName2 DESC

        List<String> orderClauses = new ArrayList<String>();
        for (FieldSort clause : sortDocument) {
            String fieldName = clause.field;
            String direction = clause.sort == FieldSort.Direction.ASCENDING ? "asc" : "desc";

            String orderClause = String.format("\"%s\" %s", fieldName, direction.toUpperCase(Locale.ENGLISH));
            orderClauses.add(orderClause);
        }

        // If we have few results, it's more efficient to reduce the search space
        // for SQLite. 500 placeholders should be a safe value.
        List<String> parameterList = new ArrayList<String>();

        String whereClause = "";
        if (docIdSet.size() < SMALL_RESULT_SET_SIZE_THRESHOLD) {
            List<String> placeholders = new ArrayList<String>();
            for (String docId : docIdSet) {
                placeholders.add("?");
                parameterList.add(docId);
            }

            whereClause = String.format("WHERE _id IN (%s)", Misc.join(", ", placeholders));
        }

        String orderBy = Misc.join(", ", orderClauses);
        String sql = String.format("SELECT DISTINCT _id FROM %s %s ORDER BY %s", indexTable,
                                                                                 whereClause,
                                                                                 orderBy);
        String[] parameters = new String[parameterList.size()];
        return SqlParts.partsForSql(sql, parameterList.toArray(parameters));
    }

    @SuppressWarnings("unchecked")
    private static String chooseIndexForSort(List<FieldSort> sortDocument,
                                      List<Index> indexes) {
        if (indexes == null || indexes.isEmpty()) {
            return null;  // Can't choose an index if one does not exist.
        }
        Set<String> neededFields = new HashSet<String>();
        // Each orderSpecifier in the sortDocument is validated and normalised
        // already to be a Map with one key.
        for (FieldSort orderSpecifier : sortDocument) {
            neededFields.add(orderSpecifier.field);
        }

        if (neededFields.isEmpty()) {
            return null;  // no point in querying empty set of fields
        }

        String chosenIndex = null;
        for (Index index : indexes) {
            Set<String> providedFields = new HashSet<String>();
            for (FieldSort field : index.fieldNames) {
                providedFields.add(field.field);
            }
            if (providedFields.containsAll(neededFields)) {
                chosenIndex = index.indexName;
                break;
            }
        }

        return chosenIndex;
    }

}
