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

import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  This class translates Cloudant Query selectors into the SQL we need to use
 *  to query our indexes.
 *
 *  It creates a tree structure which contains AND/OR nodes, along with the SQL which
 *  needs to be used to get a list of document IDs for each level. This tree structure
 *  is passed back out of the translation process for execution by an interpreter which
 *  can perform the needed AND and OR operations between the document ID sets returned
 *  by the SQL queries.
 *
 *  This merging of results in code allows us to make more intelligent use of indexes
 *  within the SQLite database. As SQLite allows us to use just a single index per query,
 *  performing several queries over indexes and then using set operations works out
 *  more flexible and likely more efficient.
 *
 *  The SQL must be executed separately so we can do it in a transaction so we're doing
 *  it over a consistent view of the index.
 *
 *  The translator is a simple depth-first, recursive decent parser over the selector
 *  map(dictionary).
 *
 *  Some examples:
 *
 *  AND : [ { x: X }, { y: Y } ]
 *
 *  This can be represented by a single SQL query and AND tree node:
 *
 *  AND
 *   |
 *  sql
 *
 *
 *  OR : [ { x: X }, { y: Y } ]
 *
 *  This is a single OR node and two SQL queries:
 *
 *     OR
 *     /\
 *  sql sql
 *
 *  The interpreter then unions the results.
 *
 *
 *  OR : [ { AND : [ { x: X }, { y: Y } ] }, { y: Y } ]
 *
 *  This requires a more complex tree:
 *
 *      OR
 *     / \
 *  AND  sql
 *   |
 *  sql
 *
 *  We could collapse out the extra AND node.
 *
 *
 *  AND : [ { OR : [ { x: X }, { y: Y } ] }, { y: Y } ]
 *
 *  This is really the most complex situation:
 *
 *       AND
 *       / \
 *     OR  sql
 *     /\
 *  sql sql
 *
 *  These basic patterns can be composed into more complicate structures.
 */
class QuerySqlTranslator {

    private static final String AND = "$and";
    private static final String OR = "$or";
    private static final String EQ = "$eq";
    private static final String EXISTS = "$exists";

    private static final Logger logger = Logger.getLogger(QuerySqlTranslator.class.getName());

    public static QueryNode translateQuery(Map<String, Object> query,
                                           Map<String, Object> indexes,
                                           Boolean[] indexesCoverQuery) {
        TranslatorState state = new TranslatorState();
        QueryNode node = translateQuery(query, indexes, state);

        // If we haven't used a single index, we need to return a query
        // which returns every document, so the posthoc matcher can
        // run over every document to manually carry out the query.
        if (!state.atLeastOneIndexUsed) {
            // TODO
            return null;
        } else {
            indexesCoverQuery[0] = !state.atLeastOneIndexMissing;
            return node;
        }
    }

    @SuppressWarnings("unchecked")
    private static QueryNode translateQuery(Map<String, Object> query,
                                           Map<String, Object> indexes,
                                           TranslatorState state) {
        // At this point we will have a root compound predicate, AND or OR, and
        // the query will be reduced to a single entry:
        // { "$and": [ ... predicates (possibly compound) ... ] }
        // { "$or": [ ... predicates (possibly compound) ... ] }

        ChildrenQueryNode root = null;
        List<Object> clauses = new ArrayList<Object>();

        if (query.keySet().iterator().next().equals(AND)) {
            clauses = (ArrayList<Object>) query.get(AND);
            root = new AndQueryNode();
        } else if (query.keySet().iterator().next().equals(OR)) {
            clauses = (ArrayList<Object>) query.get(OR);
            root = new OrQueryNode();
        }

        //
        // First handle the simple "field": { "$operator": "value" } clauses. These are
        // handled differently for AND and OR parents, so we need to have the conditional
        // logic below.
        //

        List<Object> basicClauses = new ArrayList<Object>();

        for (Object rawClause: clauses) {
            Map<String, Object> clause = (Map<String, Object>) rawClause;
            String field = clause.keySet().iterator().next();
            if (!field.startsWith("$")) {
                basicClauses.add(rawClause);
            }
        }

        if (query.keySet().iterator().next().equals(AND)) {
            // For an AND query, we require a single compound index and we generate a
            // single SQL statement to use that index to satisfy the clauses.

            String chosenIndex = chooseIndexForAndClause(basicClauses, indexes);
            if (chosenIndex == null || chosenIndex.isEmpty()) {
                state.atLeastOneIndexMissing = true;
                String msg = String.format("No single index contains all of %s; %s",
                                           basicClauses.toString(),
                                           "add index for these fields to query efficiently.");
                logger.log(Level.WARNING, msg);
            } else {
                state.atLeastOneIndexUsed = true;

                // Execute SQL on that index with appropriate values
                String select = selectStatementForAndClause(basicClauses, chosenIndex);
                if (select == null || select.isEmpty()) {
                    String msg = String.format("Error generating SELECT clause for %s",
                                               basicClauses);
                    logger.log(Level.SEVERE, msg);
                    return null;
                }

                SqlQueryNode sqlNode = new SqlQueryNode();
                sqlNode.sql = select;

                if (root != null) {
                    root.children.add(sqlNode);
                }
            }
        } else if (query.keySet().iterator().next().equals(OR)) {
            // OR nodes require a query for each clause.
            //
            // We want to allow OR clauses to use separate indexes, unlike for AND, to allow
            // users to query over multiple indexes during a single query. This prevents users
            // having to create a single huge index just because one query in their application
            // requires it, slowing execution of all the other queries down.
            //
            // We could optimise for OR parts where we have an appropriate compound index,
            // but we don't for now.

            // TODO - implement OR logic...
        }

        //
        // AND and OR subclauses are handled identically whatever the parent is.
        // We go through the query twice to order the OR clauses before the AND
        // clauses, for predictability.
        //

        // Add subclauses that are OR
        // TODO

        // Add subclauses that are AND
        // TODO

        return root;
    }

    private static List<String> fieldsForAndClause(List<Object> clause) {
        List<String> fieldNames = new ArrayList<String>();
        for (Object rawTerm: clause) {
            @SuppressWarnings("unchecked")
            Map<String, Object> term = (Map<String, Object>) rawTerm;
            if (term.size() == 1) {
                fieldNames.add(term.keySet().iterator().next());
            }
        }

        return fieldNames;
    }

    private static String chooseIndexForAndClause(List<Object> clause,
                                                  Map<String, Object> indexes) {
        List<String> fieldList = fieldsForAndClause(clause);
        Set<String> neededFields = new HashSet<String>(fieldList);

        if (neededFields.isEmpty()) {
            String msg = String.format("Invalid clauses in $and clause %s.", clause.toString());
            logger.log(Level.SEVERE, msg);
            return null;
        }

        return chooseIndexForFields(neededFields, indexes);
    }

    @SuppressWarnings("unchecked")
    private static String chooseIndexForFields(Set<String> neededFields,
                                               Map<String, Object> indexes) {
        String chosenIndex = null;
        for (String indexName: indexes.keySet()) {
            Map<String, Object> indexDefinition = (Map<String, Object>) indexes.get(indexName);
            List<String> fieldList = (List<String>) indexDefinition.get("fields");
            Set<String> providedFields = new HashSet<String>(fieldList);
            if (providedFields.containsAll(neededFields)) {
                chosenIndex = indexName;
                break;
            }
        }

        return chosenIndex;
    }

    private static String selectStatementForAndClause(List<Object> clause,
                                                      String indexName) {
        if (clause == null || clause.isEmpty()) {
            return null;  // no query here
        }

        if (indexName == null || indexName.isEmpty()) {
            return null;
        }

        String where = whereSqlForAndClause(clause);

        if (where == null || where.isEmpty()) {
            return null;
        }

        String tableName = IndexManager.tableNameForIndex(indexName);

        return String.format("SELECT _id FROM %s WHERE %s", tableName, where);
    }

    @SuppressWarnings("unchecked")
    private static String whereSqlForAndClause(List<Object> clause) {
        if (clause == null || clause.isEmpty()) {
            return null;  //  no point in querying empty set of fields
        }

        // [ { "fieldName":  "mike"}, ...]

        List<String> sqlClauses = new ArrayList<String>();

        Map<String, String> operatorMap = new HashMap<String, String>();
        operatorMap.put("$eq", "=");
        operatorMap.put("$gt", ">");
        operatorMap.put("$gte", ">=");
        operatorMap.put("$lt", "<");
        operatorMap.put("$lte", "<=");
        operatorMap.put("$ne", "!=");

        // We apply these if the clause is negated, along with the NULL clause
        Map<String, String> notOperatorMap = new HashMap<String, String>();
        notOperatorMap.put("$eq", "!=");
        notOperatorMap.put("$gt", "<=");
        notOperatorMap.put("$gte", "<");
        notOperatorMap.put("$lt", ">=");
        notOperatorMap.put("$lte", ">");
        notOperatorMap.put("$ne", "=");

        for (Object rawComponent: clause) {
            Map<String, Object> component = (Map<String, Object>) rawComponent;
            if (component.size() != 1) {
                String msg = String.format("Expected single predicate per clause map, got %s",
                                           component.toString());
                logger.log(Level.SEVERE, msg);
                return null;
            }

            String fieldName = component.keySet().iterator().next();
            Map<String, Object> predicate = (Map<String, Object>) component.get(fieldName);

            if (predicate.size() != 1) {
                String msg = String.format("Expected single operator per predicate map, got %s",
                                           predicate.toString());
                logger.log(Level.SEVERE, msg);
                return null;
            }

            String operator = predicate.keySet().iterator().next();

            // $not specifies the opposite operator OR NULL documents be returned
            if (operator.equals("not")) {
                // TODO - implement NOT logic...
            } else {
                if (operator.equals(EXISTS)) {
                    // TODO - implement EXISTS logic...
                } else {
                    String sqlOperator = operatorMap.get(operator);
                    if (sqlOperator == null || sqlOperator.isEmpty()) {
                        String msg = String.format("Unsupported comparison operator %s", operator);
                        logger.log(Level.SEVERE, msg);
                        return null;
                    }

                    String sqlClause = String.format("\"%s\" %s", fieldName, sqlOperator);
                    sqlClause = addPredicateValueToSqlClause(sqlClause, predicate.get(operator));
                    if (sqlClause == null) {
                        logger.log(Level.SEVERE, "Unsupported predicate type");
                        return null;
                    }
                    sqlClauses.add(sqlClause);
                }
            }

        }
        Joiner joiner = Joiner.on(" AND ").skipNulls();

        return joiner.join(sqlClauses);
    }

    private static String addPredicateValueToSqlClause(String clausePrefix, Object predicateValue) {
        String sqlClause = null;
        if (predicateValue instanceof String) {
            sqlClause = String.format("%s \"%s\"", clausePrefix, predicateValue);
        } else if (predicateValue instanceof Byte ||
                   predicateValue instanceof Integer ||
                   predicateValue instanceof Long ||
                   predicateValue instanceof Short ||
                   predicateValue instanceof Double ||
                   predicateValue instanceof Float) {
            sqlClause = String.format("%s %s", clausePrefix, predicateValue);
        }

        return sqlClause;
    }

}