//  Copyright (c) 2015 Cloudant. All rights reserved.
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

import com.cloudant.sync.datastore.DocumentRevision;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  Determine whether a document matches a selector.
 *
 *  This class is used when a selector cannot be satisfied using
 *  indexes alone. It takes a selector, compiles it into an internal
 *  representation and is able to then determine whether a document
 *  matches that selector.
 *
 *  The matcher works by first creating a simple tree, which is then
 *  executed against each document it's asked to match.
 *
 *
 *  Some examples:
 *
 *  AND : [ { x: X }, { y: Y } ]
 *
 *  This can be represented by a two operator expressions and AND tree node:
 *
 *          AND
 *         /   \
 *  { x: X }  { y: Y }
 *
 *
 *  OR : [ { x: X }, { y: Y } ]
 *
 *  This is a single OR node and two operator expressions:
 *
 *          OR
 *         /  \
 *  { x: X }  { y: Y }
 *
 *  The interpreter then unions the results.
 *
 *
 *  OR : [ { AND : [ { x: X }, { y: Y } ] }, { y: Y } ]
 *
 *  This requires a more complex tree:
 *
 *              OR
 *             /  \
 *          AND   { y: Y }
 *         /   \
 *  { x: X }  { y: Y }
 *
 *
 *  AND : [ { OR : [ { x: X }, { y: Y } ] }, { y: Y } ]
 *
 *  This is really the most complex situation:
 *
 *              AND
 *             /   \
 *           OR   { y: Y }
 *         /    \
 *  { x: X }  { y: Y }
 *
 *  These basic patterns can be composed into more complicate structures.
 */
class UnindexedMatcher {

    private ChildrenQueryNode root;

    private static final Logger logger = Logger.getLogger(UnindexedMatcher.class.getName());

    private static final String AND = "$and";
    private static final String OR = "$or";
    private static final String NOT = "$not";

    /**
     *  Return a new initialised matcher.
     *
     *  Assumes selector is valid as we're calling this late in
     *  the query processing.
     */
    public static UnindexedMatcher matcherWithSelector(Map<String, Object> selector) {
        ChildrenQueryNode root = buildExecutionTreeForSelector(selector);

        if (root == null) {
            return null;
        }

        UnindexedMatcher matcher = new UnindexedMatcher();
        matcher.root = root;

        return matcher;
    }

    @SuppressWarnings("unchecked")
    private static ChildrenQueryNode buildExecutionTreeForSelector(Map<String, Object> selector) {
        // At this point we will have a root compound predicate, AND or OR, and
        // the query will be reduced to a single entry:
        // { "$and": [ ... predicates (possibly compound) ... ] }
        // { "$or": [ ... predicates (possibly compound) ... ] }

        ChildrenQueryNode root = null;
        List<Object> clauses = new ArrayList<Object>();

        if (selector.get(AND) != null) {
            clauses = (List<Object>) selector.get(AND);
            root = new AndQueryNode();
        } else if (selector.get(OR) != null) {
            clauses = (List<Object>) selector.get(OR);
            root = new OrQueryNode();
        }

        //
        // First handle the simple "field": { "$operator": "value" } clauses.
        //

        List<Object> basicClauses = new ArrayList<Object>();

        for (Object rawClause: clauses) {
            Map<String, Object> clause = (Map<String, Object>) rawClause;
            String field = (String) clause.keySet().toArray()[0];
            if (!field.startsWith("$")) {
                basicClauses.add(rawClause);
            }
        }

        // Execution step will evaluate each child node and AND or OR the results.
        for (Object expression: basicClauses) {
            OperatorExpressionNode node = new OperatorExpressionNode();
            node.expression = (Map<String, Object>) expression;
            if (root != null) {
                root.children.add(node);
            }
        }

        //
        // AND and OR subclauses are handled identically whatever the parent is.
        // We go through the query twice to order the OR clauses before the AND
        // clauses, for predictability.
        //

        // Add subclauses that are OR
        // TODO - Handle OR subclauses

        // Add subclauses that are AND
        // TODO - Handle AND subclauses

        return root;
    }

    /**
     * Returns true is a document matches this matcher's selector.
     *
     * @param rev The document revision to match selector to.
     * @return document and matcher's selector matching status.
     */
    public boolean matches(DocumentRevision rev) {
        return executeSelectorTree(root, rev);
    }

    @SuppressWarnings("unchecked")
    private boolean executeSelectorTree(QueryNode node, DocumentRevision rev) {
        if (node instanceof AndQueryNode) {
            boolean passed = true;

            AndQueryNode andNode = (AndQueryNode) node;

            for (QueryNode child: andNode.children) {
                passed = passed && executeSelectorTree(child, rev);
            }

            return passed;
        }
        if (node instanceof OrQueryNode) {
            boolean passed = false;

            OrQueryNode orNode = (OrQueryNode) node;

            for (QueryNode child: orNode.children) {
                passed = passed || executeSelectorTree(child, rev);
            }

            return passed;
        } else if (node instanceof OperatorExpressionNode) {
            Map<String, Object> expression = ((OperatorExpressionNode) node).expression;

            // Here we could have:
            //   { fieldName: { operator: value } }
            // or
            //   { fieldName: { $not: { operator: value } } }

            // Next evaluate the result
            String fieldName = (String) expression.keySet().toArray()[0];
            Map<String, Object> operatorExpression;
            operatorExpression = (Map<String, Object>) expression.get(fieldName);

            String operator = (String) operatorExpression.keySet().toArray()[0];

            // First work out whether we need to invert the result when done
            boolean invertResult = operator.equals(NOT);
            if (invertResult) {
                operatorExpression = (Map<String, Object>) operatorExpression.get(NOT);
                operator = (String) operatorExpression.keySet().toArray()[0];
            }

            Object expected = operatorExpression.get(operator);
            Object actual = ValueExtractor.extractValueForFieldName(fieldName, rev);

            boolean passed = false;
            // For array actual values, the operator expression is matched
            // if any of the array values match it. We need to be careful
            // to invert the match status of every candidate, rather than
            // just flipping the result at the end.
            //
            // This is because { "$not": { "$eq": "white_cat" } } needs
            // to be taken as an atomic check, meaning:
            //   "there's an item in the array that matches `!= "white_cat"`"
            // rather than:
            //   "not (there's an item that matches white_cat)"
            // The latter is satisfied using the $nin operator.
            if (actual instanceof List) {
                for (Object item: (List<Object>) actual) {
                    // OR as any value in the array can match
                    boolean currentItemPassed = valueCompare(item, operator, expected);
                    passed = passed || (invertResult ? !currentItemPassed : currentItemPassed);
                }
            } else {
                passed = valueCompare(actual, operator, expected);
                passed = invertResult ? !passed : passed;
            }

            return passed;
        } else {
            // We constructed the tree, so shouldn't end up here; error if we do.
            String msg = String.format("Found unexpected selector execution tree: %s", node);
            logger.log(Level.SEVERE, msg);
            return false;
        }
    }

    private boolean valueCompare(Object actual, String operator, Object expected) {
        boolean passed;

        if (operator.equals("$eq")) {
            passed = compareEq(actual, expected);
        } else if (operator.equals("$ne")) {
            passed = compareNE(actual, expected);
        } else if (operator.equals("$lt")) {
            passed = compareLT(actual, expected);
        } else if (operator.equals("$lte")) {
            passed = compareLTE(actual, expected);
        } else if (operator.equals("$gt")) {
            passed = compareGT(actual, expected);
        } else if (operator.equals("$gte")) {
            passed = compareGTE(actual, expected);
        } else if (operator.equals("$exists")) {
            boolean expectedBool = (Boolean) expected;
            boolean exists = (actual != null);
            passed = (exists == expectedBool);
        } else {
            String msg = String.format("Found unexpected operator in selector: %s", operator);
            logger.log(Level.WARNING, msg);
            passed = false;
        }

        return passed;
    }

    protected static boolean compareEq(Object l, Object r) {
        if (l instanceof String && r instanceof String) {
            return l.equals(r);
        }
        if (l instanceof Boolean && r instanceof Boolean) {
            return l == r;
        }
        return l instanceof Number &&
               r instanceof Number &&
               !(l instanceof Float || r instanceof Float) &&
               ((Number) l).doubleValue() == ((Number) r).doubleValue();
    }

    protected static boolean compareNE(Object l, Object r) {
        return !(compareEq(l, r));
    }

    //
    // Try to respect SQLite's ordering semantics:
    //  1. NULL
    //  2. INT/REAL
    //  3. TEXT
    //  4. BLOB
    protected static boolean compareLT(Object l, Object r) {
        // TODO: 275 - 309 CDTQUnindexedMatcher.m
        return false;
    }

    protected static boolean compareLTE(Object l, Object r) {
        // TODO: 311 - 323 CDTQUnindexedMatcher.m
        return false;
    }

    protected static boolean compareGT(Object l, Object r) {
        // TODO: 325 - 337 CDTQUnindexedMatcher.m
        return false;
    }

    protected static boolean compareGTE(Object l, Object r) {
        // TODO: 339 - 351 CDTQUnindexedMatcher.m
        return false;
    }

}