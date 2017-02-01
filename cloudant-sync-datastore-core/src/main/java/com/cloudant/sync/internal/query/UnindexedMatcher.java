/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2015 Cloudant, Inc. All rights reserved.
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

import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.internal.documentstore.InternalDocumentRevision;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
public class UnindexedMatcher {

    private ChildrenQueryNode root;

    private static final Logger logger = Logger.getLogger(UnindexedMatcher.class.getName());

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

        if (selector.get(QueryConstants.AND) != null) {
            clauses = (List<Object>) selector.get(QueryConstants.AND);
            root = new AndQueryNode();
        } else if (selector.get(QueryConstants.OR) != null) {
            clauses = (List<Object>) selector.get(QueryConstants.OR);
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
            OperatorExpressionNode node = new OperatorExpressionNode((Map<String, Object>) expression);
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
        for (Object rawClause: clauses) {
            Map<String, Object> clause = (Map<String, Object>) rawClause;
            String field = (String) clause.keySet().toArray()[0];
            if (field.equals(QueryConstants.OR)) {
                QueryNode orNode = buildExecutionTreeForSelector(clause);
                if (root != null) {
                    root.children.add(orNode);
                }
            }
        }

        // Add subclauses that are AND
        for (Object rawClause: clauses) {
            Map<String, Object> clause = (Map<String, Object>) rawClause;
            String field = (String) clause.keySet().toArray()[0];
            if (field.equals(QueryConstants.AND)) {
                QueryNode andNode = buildExecutionTreeForSelector(clause);
                if (root != null) {
                    root.children.add(andNode);
                }
            }
        }

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
            boolean invertResult = operator.equals(QueryConstants.NOT);
            if (invertResult) {
                operatorExpression = (Map<String, Object>) operatorExpression.get(QueryConstants.NOT);
                operator = (String) operatorExpression.keySet().toArray()[0];
            }

            Object expected = operatorExpression.get(operator);
            Object actual = ValueExtractor.extractValueForFieldName(fieldName, rev);

            boolean passed = false;
            List<String> specialCaseOperators = Arrays.asList(QueryConstants.MOD, QueryConstants.SIZE);
            if (specialCaseOperators.contains(operator)) {
                // If an operator like $mod or $size is found we need to treat the
                // comparison as a special case.
                //
                // $mod: perform modulo arithmetic on the actual value using the first
                //       element in the expected list as the divisor before comparing
                //       the result to the second element in the expected list.
                //
                // $size: check whether the actual value is a list, then compare the
                //        actual list size with the expected value.
                passed = valueCompare(actual, operator, expected);
            } else {
                // Since $in is the same as a series of $eq comparisons -
                // Treat them the same by:
                // - Ensuring that both expected and actual are lists/collections.
                // - Convert the $in operator to the $eq operator.
                if (!(expected instanceof List)) {
                    expected = Collections.singletonList(expected);
                }
                if (!(actual instanceof List)) {
                    actual = Collections.singletonList(actual);
                }
                if (operator.equals(QueryConstants.IN)) {
                    operator = QueryConstants.EQ;
                }

                for (Object expectedItem : (List<Object>) expected) {
                    for (Object actualItem: (List<Object>) actual) {
                        // OR since any actual item can match any value in the expected list
                        passed = passed || valueCompare(actualItem, operator, expectedItem);
                    }
                }
            }

            return invertResult ? !passed : passed;
        } else {
            // We constructed the tree, so shouldn't end up here; error if we do.
            String msg = String.format("Found unexpected selector execution tree: %s", node);
            logger.log(Level.SEVERE, msg);
            return false;
        }
    }

    private boolean valueCompare(Object actual, String operator, Object expected) {
        boolean passed;

        if (operator.equals(QueryConstants.EQ)) {
            passed = compareEq(actual, expected);
        } else if (operator.equals(QueryConstants.LT)) {
            passed = compareLT(actual, expected);
        } else if (operator.equals(QueryConstants.LTE)) {
            passed = compareLTE(actual, expected);
        } else if (operator.equals(QueryConstants.GT)) {
            passed = compareGT(actual, expected);
        } else if (operator.equals(QueryConstants.GTE)) {
            passed = compareGTE(actual, expected);
        } else if (operator.equals(QueryConstants.MOD)) {
            passed = compareMOD(actual, expected);
        } else if (operator.equals(QueryConstants.SIZE)) {
            passed = compareSIZE(actual, expected);
        } else if (operator.equals(QueryConstants.EXISTS)) {
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
        } else if (l instanceof Boolean && r instanceof Boolean) {
            return l == r;
        } else {
            return l instanceof Number && r instanceof Number &&
                    ((Number) l).doubleValue() == ((Number) r).doubleValue();
        }
    }

    //
    // Try to respect SQLite's ordering semantics:
    //  1. NULL
    //  2. INT/REAL
    //  3. TEXT
    //  4. BLOB
    protected static boolean compareLT(Object l, Object r) {
        if (l == null || r == null) {
            return false;  // null fails all lt/gt/lte/gte tests
        } else if (!(l instanceof String || l instanceof Number)) {
            String msg = String.format("Value in document not a Number or String: %s", l);
            logger.log(Level.WARNING, msg);
            return false;  // Not sure how to compare values that are not numbers or strings
        } else if (l instanceof String) {
            if (r instanceof Number) {
                return false;  // INT < STRING
            }

            String lStr = (String) l;
            String rStr = (String) r;

            return lStr.compareTo(rStr) < 0;
        } else if (r instanceof String) {
            // At this point in the logic l can only be a number
            return true;  // INT < STRING
        } else {
            // At this point in the logic both l and r can only be numbers
            Number lNum = (Number) l;
            Number rNum = (Number) r;

            return lNum.doubleValue() < rNum.doubleValue();
        }
    }

    protected static boolean compareLTE(Object l, Object r) {
        if (l == null || r == null) {
            return false;  // null fails all lt/gt/lte/gte tests
        } else if (!(l instanceof String || l instanceof Number)) {
            String msg = String.format("Value in document not a Number or String: %s", l);
            logger.log(Level.WARNING, msg);
            return false;  // Not sure how to compare values that are not numbers or strings
        } else {
            return compareLT(l, r) || compareEq(l, r);
        }
    }

    protected static boolean compareGT(Object l, Object r) {
        if (l == null || r == null) {
            return false;  // null fails all lt/gt/lte/gte tests
        } else if (!(l instanceof String || l instanceof Number)) {
            String msg = String.format("Value in document not a Number or String: %s", l);
            logger.log(Level.WARNING, msg);
            return false;  // Not sure how to compare values that are not numbers or strings
        } else {
            return !compareLTE(l, r);
        }
    }

    protected static boolean compareGTE(Object l, Object r) {
        if (l == null || r == null) {
            return false;  // null fails all lt/gt/lte/gte tests
        } else if (!(l instanceof String || l instanceof Number)) {
            String msg = String.format("Value in document not a Number or String: %s", l);
            logger.log(Level.WARNING, msg);
            return false;  // Not sure how to compare values that are not numbers or strings
        } else {
            return !compareLT(l, r);
        }
    }

    @SuppressWarnings("unchecked")
    protected static boolean compareMOD(Object l, Object r) {
        if (!(l instanceof Number)) {
            return false;
        }

        // r should be a list containing two numbers.  These two numbers are assured
        // to be integers and the divisor is assured to not be 0.  This would have been
        // handled during normalization and validation.
        int divisor = ((List<Integer>) r).get(0);
        int expectedRemainder = ((List<Integer>) r).get(1);

        // Calculate the actual remainder based on the truncated whole
        // number value of l which is the actual number from the document.
        // This is the desired behavior to replicate the SQL engine.
        int actualRemainder = ((Number) l).intValue() % divisor;

        return actualRemainder == expectedRemainder;
    }

    protected static boolean compareSIZE(Object l, Object r) {
        // The actual value must be a list and the expected value must be a number in
        // order to perform a size comparison.
        if (!(l instanceof List) || !(r instanceof Number)) {
            return false;
        }

        // actualSize and expectedSize are compared as Numbers and not integers
        // because it is valid to assign expectedSize a non-integer number value.
        Number actualSize = ((List) l).size();
        Number expectedSize = (Number) r;

        return actualSize.equals(expectedSize);
    }

}
