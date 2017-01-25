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

import static com.cloudant.sync.query.QueryConstants.AND;
import static com.cloudant.sync.query.QueryConstants.EQ;
import static com.cloudant.sync.query.QueryConstants.EXISTS;
import static com.cloudant.sync.query.QueryConstants.GT;
import static com.cloudant.sync.query.QueryConstants.GTE;
import static com.cloudant.sync.query.QueryConstants.IN;
import static com.cloudant.sync.query.QueryConstants.LT;
import static com.cloudant.sync.query.QueryConstants.LTE;
import static com.cloudant.sync.query.QueryConstants.MOD;
import static com.cloudant.sync.query.QueryConstants.NOT;
import static com.cloudant.sync.query.QueryConstants.OR;
import static com.cloudant.sync.query.QueryConstants.SEARCH;
import static com.cloudant.sync.query.QueryConstants.SIZE;
import static com.cloudant.sync.query.QueryConstants.TEXT;

import com.cloudant.sync.util.Misc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  This class translates Cloudant query selectors into the SQL we need to use
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

    private static final Logger logger = Logger.getLogger(QuerySqlTranslator.class.getName());

    public static QueryNode translateQuery(Map<String, Object> query,
                                           Map<String, Object> indexes,
                                           Boolean[] indexesCoverQuery) {
        TranslatorState state = new TranslatorState();
        QueryNode node = translateQuery(query, indexes, state);

        if (state.textIndexMissing) {
            String msg = "No text index defined, cannot execute query containing a text search.";
            logger.log(Level.SEVERE, msg);
            return null;
        } else if (state.textIndexRequired && state.atLeastOneIndexMissing) {
            String msg = String.format("query %s contains a text search but is missing \"json\"" +
                                       " index(es).  All indexes must exist in order to execute a" +
                                       " query containing a text search.  Create all necessary" +
                                       " indexes for the query and re-execute.",
                                       query.toString());
            logger.log(Level.SEVERE, msg);
            return null;
        } else if (!state.textIndexRequired &&
                      (!state.atLeastOneIndexUsed || state.atLeastOneORIndexMissing)) {
            // If we haven't used a single index or an OR clause is missing an index,
            // we need to return every document id, so that the post-hoc matcher can
            // run over every document to manually carry out the query.
            SqlQueryNode sqlNode = new SqlQueryNode();
            Set<String> neededFields = new HashSet<String>(Collections.singletonList("_id"));
            String allDocsIndex = chooseIndexForFields(neededFields, indexes);

            if (allDocsIndex != null && !allDocsIndex.isEmpty()) {
                String tableName = IndexManager.tableNameForIndex(allDocsIndex);
                String sql = String.format(Locale.ENGLISH, "SELECT _id FROM \"%s\"", tableName);
                sqlNode.sql = SqlParts.partsForSql(sql, new String[]{});
            }

            AndQueryNode root = new AndQueryNode();
            root.children.add(sqlNode);

            indexesCoverQuery[0] = false;
            return root;
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

        if (query.get(AND) != null) {
            clauses = (ArrayList<Object>) query.get(AND);
            root = new AndQueryNode();
        } else if (query.get(OR) != null) {
            clauses = (ArrayList<Object>) query.get(OR);
            root = new OrQueryNode();
        }

        // Compile a list of simple clauses to be handled below.  If a text clause is
        // encountered, store it separately from the simple clauses since it will be
        // handled later on its own.
        List<Object> basicClauses = null;
        Object textClause = null;

        for (Object rawClause: clauses) {
            Map<String, Object> clause = (Map<String, Object>) rawClause;
            String field = (String) clause.keySet().toArray()[0];
            if (!field.startsWith("$")) {
                if (basicClauses == null) {
                    basicClauses = new ArrayList<Object>();
                }
                basicClauses.add(rawClause);
            } else if (field.equalsIgnoreCase(TEXT)) {
                textClause = rawClause;
            }
        }

        // Handle the simple "field": { "$operator": "value" } clauses. These are
        // handled differently for AND and OR parents, so we need to have the conditional
        // logic below.
        if (basicClauses != null) {
            if (query.get(AND) != null) {
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
                    SqlParts select = selectStatementForAndClause(basicClauses, chosenIndex);
                    if (select == null) {
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
            } else if (query.get(OR) != null) {
                // OR nodes require a query for each clause.
                //
                // We want to allow OR clauses to use separate indexes, unlike for AND, to allow
                // users to query over multiple indexes during a single query. This prevents users
                // having to create a single huge index just because one query in their application
                // requires it, slowing execution of all the other queries down.
                //
                // We could optimise for OR parts where we have an appropriate compound index,
                // but we don't for now.

                for (Object basicClause : basicClauses) {
                    List<Object> wrappedClause = Arrays.asList(basicClause);
                    String chosenIndex = chooseIndexForAndClause(wrappedClause, indexes);
                    if (chosenIndex == null || chosenIndex.isEmpty()) {
                        state.atLeastOneIndexMissing = true;
                        state.atLeastOneORIndexMissing = true;
                        String msg = String.format("No single index contains all of %s; %s",
                                basicClauses.toString(),
                                "add index for these fields to query efficiently.");
                        logger.log(Level.WARNING, msg);
                    } else {
                        state.atLeastOneIndexUsed = true;

                        // Execute SQL on that index with appropriate values
                        SqlParts select = selectStatementForAndClause(wrappedClause, chosenIndex);
                        if (select == null) {
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
                }
            }
        }

        // A text clause such as { "$text" : { "$search" : "foo bar baz" } }
        // by nature uses its own text index.  It is therefor handled
        // separately from other simple clauses.
        if (textClause != null) {
            state.textIndexRequired = true;
            String textIndex = getTextIndex(indexes);
            if (textIndex == null || textIndex.isEmpty()) {
                state.textIndexMissing = true;
            } else {
                SqlParts select = selectStatementForTextClause(textClause, textIndex);
                if (select == null) {
                    String msg = String.format("Error generating SELECT clause for %s",
                            textClause.toString());
                    logger.log(Level.SEVERE, msg);
                    return null;
                }

                SqlQueryNode sqlNode = new SqlQueryNode();
                sqlNode.sql = select;

                if (root != null) {
                    root.children.add(sqlNode);
                }
            }
        }

        //
        // AND and OR subclauses are handled identically whatever the parent is.
        // We go through the query twice to order the OR clauses before the AND
        // clauses, for predictability.
        //

        // Add subclauses that are OR
        for (Object rawClause: clauses) {
            Map<String, Object> clause = (Map<String, Object>) rawClause;
            String field = (String) clause.keySet().toArray()[0];
            if (field.equals(OR)) {
                QueryNode orNode = translateQuery(clause, indexes, state);
                if (root != null) {
                    root.children.add(orNode);
                }
            }
        }

        // Add subclauses that are AND
        for (Object rawClause: clauses) {
            Map<String, Object> clause = (Map<String, Object>) rawClause;
            String field = (String) clause.keySet().toArray()[0];
            if (field.equals(AND)) {
                QueryNode andNode = translateQuery(clause, indexes, state);
                if (root != null) {
                    root.children.add(andNode);
                }
            }
        }

        return root;
    }

    private static List<String> fieldsForAndClause(List<Object> clause) {
        if (clause == null) {
            return null;
        }
        List<String> fieldNames = new ArrayList<String>();
        for (Object rawTerm: clause) {
            @SuppressWarnings("unchecked")
            Map<String, Object> term = (Map<String, Object>) rawTerm;
            if (term.size() == 1) {
                fieldNames.add((String) term.keySet().toArray()[0]);
            }
        }

        return fieldNames;
    }

    /*
     * Checks for the existence of an operator in a query clause list
     */
    protected static boolean isOperatorFoundInClause(String operator, List<Object> clause) {
        boolean found = false;
        for (Object rawTerm : clause){
            if (rawTerm instanceof Map) {
                Map term = (Map) rawTerm;
                if (term.size() == 1 && term.values().toArray()[0] instanceof Map) {
                    Map predicate = (Map) term.values().toArray()[0];
                    if (predicate.get(operator) != null) {
                        found = true;
                        break;
                    }
                }
            }
        }

        return found;
    }

    protected static String chooseIndexForAndClause(List<Object> clause,
                                                    Map<String, Object> indexes) {
        if (clause == null || clause.isEmpty()) {
            return null;
        }

        if (indexes == null || indexes.isEmpty()) {
            return null;
        }

        if (isOperatorFoundInClause(SIZE, clause)) {
            String msg = String.format("$size operator found in clause %s.  " +
                                       "Indexes are not used with $size operations.", clause);
            logger.log(Level.INFO, msg);
            return null;
        }

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
    protected static String chooseIndexForFields(Set<String> neededFields,
                                                 Map<String, Object> indexes) {
        String chosenIndex = null;
        for (Map.Entry<String, Object> entry: indexes.entrySet()) {
            Map<String, Object> indexDefinition = (Map<String, Object>) entry.getValue();

            // Don't choose a text index for a non-text query clause
            IndexType indexType = (IndexType) indexDefinition.get("type");
            if (indexType == IndexType.TEXT) {
                continue;
            }

            List<String> fieldList = (List<String>) indexDefinition.get("fields");
            Set<String> providedFields = new HashSet<String>(fieldList);
            if (providedFields.containsAll(neededFields)) {
                chosenIndex = entry.getKey();
                break;
            }
        }

        return chosenIndex;
    }

    @SuppressWarnings("unchecked")
    private static String getTextIndex(Map<String, Object> indexes) {
        String textIndex = null;
        for (Map.Entry<String, Object> entry: indexes.entrySet()) {
            Map<String, Object> indexDefinition = (Map<String, Object>) entry.getValue();
            IndexType indexType = (IndexType) indexDefinition.get("type");
            if (indexType == IndexType.TEXT) {
                textIndex = entry.getKey();
            }
        }

        return textIndex;
    }

    protected static SqlParts selectStatementForAndClause(List<Object> clause,
                                                          String indexName) {
        if (clause == null || clause.isEmpty()) {
            return null;  // no query here
        }

        if (indexName == null || indexName.isEmpty()) {
            return null;
        }

        SqlParts where = whereSqlForAndClause(clause, indexName);

        if (where == null) {
            return null;
        }

        String tableName = IndexManager.tableNameForIndex(indexName);

        String sql = String.format(Locale.ENGLISH,
                                   "SELECT _id FROM \"%s\" WHERE %s",
                                   tableName,
                                   where.sqlWithPlaceHolders);
        return SqlParts.partsForSql(sql, where.placeHolderValues);
    }

    @SuppressWarnings("unchecked")
    protected static SqlParts selectStatementForTextClause(Object clause,
                                                           String indexName) {
        if (clause == null) {
            return null;  // no query here
        }

        if (indexName == null || indexName.isEmpty()) {
            return null;
        }

        if (!(clause instanceof Map)) {
            return null;  // should never get here as this would not pass normalization
        }

        Map<String, Object> textClause = (Map<String, Object>) clause;
        Map<String, String> searchClause = (Map<String, String>) textClause.get(TEXT);

        String tableName = IndexManager.tableNameForIndex(indexName);
        String search = searchClause.get(SEARCH);
        search = search.replace("'", "''");

        String sql = String.format(Locale.ENGLISH, "SELECT _id FROM \"%s\" WHERE \"%s\" MATCH ?", tableName, tableName);
        return SqlParts.partsForSql(sql, new String[]{ search });
    }

    @SuppressWarnings("unchecked")
    protected static SqlParts whereSqlForAndClause(List<Object> clause, String indexName) {
        if (clause == null || clause.isEmpty()) {
            return null;  //  no point in querying empty set of fields
        }

        // [ { "fieldName":  "mike"}, ...]

        List<String> whereClauses = new ArrayList<String>();
        List<Object> sqlParameters = new ArrayList<Object>() {
            @Override
            public boolean add(Object o) {
                if (o instanceof Boolean){
                    Boolean bool = (Boolean) o;
                    return super.add(bool ? "1" : "0");
                } else {
                    return super.add(String.valueOf(o));
                }
            }
        };

        Map<String, String> operatorMap = new HashMap<String, String>();
        operatorMap.put(EQ, "=");
        operatorMap.put(GT, ">");
        operatorMap.put(GTE, ">=");
        operatorMap.put(LT, "<");
        operatorMap.put(LTE, "<=");
        operatorMap.put(IN, "IN");
        operatorMap.put(MOD, "%");

        for (Object rawComponent: clause) {
            Map<String, Object> component = (Map<String, Object>) rawComponent;
            if (component.size() != 1) {
                String msg = String.format("Expected single predicate per clause map, got %s",
                                           component.toString());
                logger.log(Level.SEVERE, msg);
                return null;
            }

            String fieldName = (String) component.keySet().toArray()[0];
            Map<String, Object> predicate = (Map<String, Object>) component.get(fieldName);

            if (predicate.size() != 1) {
                String msg = String.format("Expected single operator per predicate map, got %s",
                                           predicate.toString());
                logger.log(Level.SEVERE, msg);
                return null;
            }

            String operator = (String) predicate.keySet().toArray()[0];

            // $not specifies ALL documents NOT in the set of documents that match the operator.
            if (operator.equals(NOT)) {
                Map<String, Object> negatedPredicate = (Map<String, Object>) predicate.get(NOT);

                if (negatedPredicate.size() != 1) {
                    String msg = String.format("Expected single operator per predicate map, got %s",
                                               predicate.toString());
                    logger.log(Level.SEVERE, msg);
                    return null;
                }

                operator = (String) negatedPredicate.keySet().toArray()[0];
                Object predicateValue;

                if (operator.equals(EXISTS)) {
                    // what we do here depends on what the value of the exists are
                    predicateValue = negatedPredicate.get(operator);

                    boolean exists = !((Boolean) predicateValue);
                    // since this clause is negated we need to negate the bool value
                    whereClauses.add(convertExistsToSqlClauseForFieldName(fieldName, exists));
                } else {
                    String whereClause;
                    String sqlOperator = operatorMap.get(operator);
                    String tableName = IndexManager.tableNameForIndex(indexName);
                    String placeholder;
                    if (operator.equals(IN)) {
                        // The predicate map value must be a List here.
                        // This was validated during normalization.
                        List<Object> inList = (List<Object>) negatedPredicate.get(operator);
                        placeholder = placeholdersForInList(inList, sqlParameters);
                    } else if (operator.equals(MOD)) {
                        // The predicate map value must be a two element list containing integers
                        // here.  This was validated during normalization.
                        List<Integer> modulus = (List<Integer>) negatedPredicate.get(operator);
                        // Casting the remainder as integer ensures proper processing of the modulo
                        // operation by SQLite.  If left un-cast, unexpected results occur.
                        placeholder = String.format("? %s CAST(? AS INTEGER)", operatorMap.get(EQ));
                        sqlParameters.add(modulus.get(0));
                        sqlParameters.add(modulus.get(1));
                    } else {
                        // The predicate map value must be either a
                        // String, a non-Float Number or Boolean here.
                        // This was validated during normalization.
                        // Boolean values need to be converted into 1 or 0
                        // to match SQLite expected values.
                        predicateValue = negatedPredicate.get(operator);
                        placeholder = "?";
                        sqlParameters.add(predicateValue);

                    }

                    whereClause = whereClauseForNot(fieldName, sqlOperator, tableName, placeholder);
                    whereClauses.add(whereClause);
                }
            } else {
                if (operator.equals(EXISTS)) {
                    boolean exists = (Boolean) predicate.get(operator);
                    whereClauses.add(convertExistsToSqlClauseForFieldName(fieldName, exists));
                } else {
                    String whereClause;
                    String sqlOperator = operatorMap.get(operator);
                    String placeholder;
                    if (operator.equals(IN)) {
                        // The predicate map value must be a List here.
                        // This was validated during normalization.
                        List<Object> inList = (List<Object>) predicate.get(operator);
                        placeholder = placeholdersForInList(inList, sqlParameters);
                    } else if (operator.equals(MOD)) {
                        // The predicate map value must be a two element list containing integers
                        // here.  This was validated during normalization.
                        List<Integer> modulus = (List<Integer>) predicate.get(operator);
                        // Casting the remainder as integer ensures proper processing of the modulo
                        // operation by SQLite.  If left un-cast, unexpected results occur.
                        placeholder = String.format("? %s CAST(? AS INTEGER)", operatorMap.get(EQ));
                        sqlParameters.add(modulus.get(0));
                        sqlParameters.add(modulus.get(1));
                    } else {
                        // The predicate map value must be either a
                        // String, a non-Float Number or Boolean here.
                        // This was validated during normalization.
                        // Boolean values need to be converted into 1 or 0
                        // to match SQLite expected values.
                        Object predicateValue = predicate.get(operator);
                        placeholder = "?";
                        sqlParameters.add(predicateValue);
                    }

                    whereClause = String.format("\"%s\" %s %s", fieldName,
                                                                sqlOperator,
                                                                placeholder);
                    whereClauses.add(whereClause);
                }
            }
        }
        String where = Misc.join(" AND ", whereClauses);
        String[] parameterArray = new String[sqlParameters.size()];
        int idx = 0;
        for (Object parameter: sqlParameters) {
            parameterArray[idx] = String.valueOf(parameter);
            idx++;
        }

        return SqlParts.partsForSql(where, parameterArray);
    }

    private static String placeholdersForInList(List<Object> values, List<Object> sqlParameters) {
        List<String> inOperands = new ArrayList<String>();
        for (Object value : values) {
            inOperands.add("?");
            sqlParameters.add(String.valueOf(value));
        }

        return String.format("( %s )", Misc.join(", ", inOperands));
    }

    /**
     * WHERE clause representation of $not must be handled by using a
     * sub-SELECT statement of the operator which is then applied to
     * _id NOT IN (...).  This is because this process is the only
     * way that we can ensure that documents that contain arrays are
     * handled correctly.
     *
     * @param fieldName the field to be NOT-ted
     * @param sqlOperator the SQL operator used in the sub-SELECT
     * @param tableName the chosen table index
     * @return the NOT-ted WHERE clause for the fieldName and sqlOperator
     */
    private static String whereClauseForNot(String fieldName,
                                            String sqlOperator,
                                            String tableName,
                                            String operand) {
        String whereForSubSelect = String.format(Locale.ENGLISH, "\"%s\" %s %s", fieldName, sqlOperator, operand);
        String subSelect = String.format(Locale.ENGLISH, "SELECT _id FROM \"%s\" WHERE %s",
                                         tableName,
                                         whereForSubSelect);

        return String.format("_id NOT IN (%s)", subSelect);
    }

    private static String convertExistsToSqlClauseForFieldName(String fieldName, boolean exists) {
        String sqlClause;
        if (exists) {
            // so this field needs to exist
            sqlClause = String.format("(\"%s\" IS NOT NULL)", fieldName);
        } else {
            // must not exist
            sqlClause = String.format("(\"%s\" IS NULL)", fieldName);
        }

        return sqlClause;
    }

}
