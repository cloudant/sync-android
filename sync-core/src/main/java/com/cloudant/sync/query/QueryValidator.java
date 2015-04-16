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

import static com.cloudant.sync.query.QueryConstants.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  This class contains common validation options for the
 *  two different implementations of query.
 */
class QueryValidator {

    // negatedShortHand is used for operator shorthand processing.
    // For example:
    // The shorthand operator $ne has a longhand representation
    // that is { "$not" : { "$eq" : ... } }.  Therefore the negation
    // of the $ne operator is the $eq operator.
    private static final Map<String, String> negatedShortHand = new HashMap<String, String>() {
        {
            put(NE, EQ);
            put(NIN, IN);
        }
    };
    private static final Logger logger = Logger.getLogger(QueryValidator.class.getName());

    /**
     *  Expand implicit operators in a query, and validate
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> normaliseAndValidateQuery(Map<String, Object> query) {
        boolean isWildCard = false;
        if (query.isEmpty()) {
            isWildCard = true;
        }

        // First expand the query to include a leading compound predicate
        // if there isn't one already.
        query = addImplicitAnd(query);

        // At this point we will have a single entry map, key AND or OR,
        // forming the compound predicate.
        String compoundOperator = (String) query.keySet().toArray()[0];
        List<Object> predicates = new ArrayList<Object>();
        if (query.get(compoundOperator) instanceof List) {
            // Next make sure all the predicates have an operator -- the EQ
            // operator is implicit and we need to add it if there isn't one.
            // Take
            //     [ {"field1": "mike"}, ... ]
            // and make
            //     [ {"field1": { "$eq": "mike"} }, ... ]
            predicates = addImplicitEq((List<Object>) query.get(compoundOperator));

            // Then all shorthand operators like $ne, if present, need to be converted
            // to their logical longhand equivalent.
            // Take
            //     [ { "field1": { "$ne": "mike"} }, ... ]
            // and make
            //     [ { "field1": { "$not" : { "$eq": "mike"} } }, ... ]
            predicates = handleShortHandOperators(predicates);

            // Now in the event that extraneous $not operators exist in the query,
            // these operators must be compressed down to the their logical equivalent.
            // Take
            //     [ { "field1": { "$not" : { $"not" : { "$eq": "mike"} } } }, ... ]
            // and make
            //     [ { "field1": { "$eq": "mike"} }, ... ]
            predicates = compressMultipleNotOperators(predicates);
        }

        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put(compoundOperator, predicates);
        if (isWildCard || validateSelector(selector)) {
            return selector;
        }

        return null;
    }

    private static Map<String, Object> addImplicitAnd(Map<String, Object> query) {
        // query is:
        //  either { "field1": "value1", ... } -- we need to add $and
        //  or     { "$and": [ ... ] } -- we don't
        //  or     { "$or": [ ... ] } -- we don't

        if (query.size() == 1 && (query.get(AND) != null || query.get(OR) != null)) {
            return query;
        } else {
            // Take
            //     {"field1": "mike", ...}
            //     {"field1": [ "mike", "bob" ], ...}
            // and make
            //     [ {"field1": "mike"}, ... ]
            //     [ {"field1": [ "mike", "bob" ]}, ... ]
            List<Object> andClause = new ArrayList<Object>();
            for (String k: query.keySet()) {
                Object predicate = query.get(k);
                Map<String, Object> element = new HashMap<String, Object>();
                element.put(k, predicate);
                andClause.add(element);
            }
            query.clear();
            query.put(AND, andClause);
            return query;
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Object> addImplicitEq(List<Object> clause) {
        List<Object> accumulator = new ArrayList<Object>();

        for (Object fieldClause: clause) {
            // fieldClause is:
            //  either { "field1": "mike"} -- we need to add the $eq operator
            //  or     { "field1": { "$operator": "value" } -- we don't
            //  or     { "$and": [ ... ] } -- we don't
            //  or     { "$or": [ ... ] } -- we don't
            Object predicate;
            String fieldName;
            if (fieldClause instanceof Map && !((Map) fieldClause).isEmpty()) {
                Map<String, Object> fieldClauseMap = (Map<String, Object>) fieldClause;
                fieldName = (String) fieldClauseMap.keySet().toArray()[0];
                predicate = fieldClauseMap.get(fieldName);
            } else {
                // if this isn't a map, we don't know what to do so add the clause
                // to the accumulator to be dealt with later as part of the final selector
                // validation.
                accumulator.add(fieldClause);
                continue;
            }

            // If the clause isn't a special clause (the field name starts with
            // $, e.g., $and), we need to check whether the clause already
            // has an operator. If not, we need to add the implicit $eq.
            if (!fieldName.startsWith("$")) {
                if (!(predicate instanceof Map)) {
                    Map<String, Object> eqPredicate = new HashMap<String, Object>();
                    eqPredicate.put(EQ, predicate);
                    predicate = eqPredicate;
                }
            } else if (predicate instanceof List) {
                predicate = addImplicitEq((List<Object>) predicate);
            }

            Map<String, Object> element = new HashMap<String, Object>();
            element.put(fieldName, predicate);
            accumulator.add(element);  // can't put null into accumulator
        }

        return accumulator;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> handleShortHandOperators(List<Object> clause) {
        List<Object> accumulator = new ArrayList<Object>();

        for (Object fieldClause: clause) {
            Object predicate;
            String fieldName;
            if (fieldClause instanceof Map && !((Map) fieldClause).isEmpty()) {
                Map<String, Object> fieldClauseMap = (Map<String, Object>) fieldClause;
                fieldName = (String) fieldClauseMap.keySet().toArray()[0];
                predicate = fieldClauseMap.get(fieldName);
                if (fieldName.startsWith("$") && predicate instanceof List) {
                    predicate = handleShortHandOperators((List<Object>) predicate);
                } else if (predicate instanceof Map && !((Map) predicate).isEmpty()) {
                    // if the clause isn't a special clause (the field name starts with
                    // $, e.g., $and), we need to check whether the clause has a shorthand
                    // operator like $ne. If it does, we need to convert it to its longhand
                    // version.
                    // Take:  { "$ne" : ... }
                    // Make:  { "$not" : { "$eq" : ... } }
                    predicate = replaceWithLonghand((Map <String, Object>) predicate);
                } else {
                    accumulator.add(fieldClause);
                    continue;
                }
            } else {
                // if this isn't a map, we don't know what to do so add the clause
                // to the accumulator to be dealt with later as part of the final selector
                // validation.
                accumulator.add(fieldClause);
                continue;
            }
            Map<String, Object> element = new HashMap<String, Object>();
            element.put(fieldName, predicate);
            accumulator.add(element);
        }

        return accumulator;
    }

    /**
     * This method traverses the predicate map and once it reaches the last operator
     * in the tree, it checks it for a shorthand representation.  If one exists then
     * that shorthand representation is replaced with its longhand version.
     * For example:   { "$ne" : ... }
     * is replaced by { "$not" : { "$eq" : ... } }
     *
     * @param predicate the map to process
     * @return the processed map
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> replaceWithLonghand(Map<String, Object> predicate) {
        Map<String, Object> accumulator = new HashMap<String, Object>();

        if (predicate == null || predicate.isEmpty()) {
            return predicate;
        }
        String operator = (String) predicate.keySet().toArray()[0];
        Object subPredicate = predicate.get(operator);
        if (subPredicate instanceof Map) {
            accumulator.put(operator, replaceWithLonghand((Map<String, Object>) subPredicate));
        } else if (negatedShortHand.get(operator) != null) {
            Map<String, Object> positivePredicate = new HashMap<String, Object>();
            positivePredicate.put(negatedShortHand.get(operator), subPredicate);
            accumulator.put(NOT, positivePredicate);
        } else {
            accumulator.put(operator, subPredicate);
        }
        return accumulator;
    }

    /**
     * This method takes a string of $not operators down to either none or a single $not
     * operator.  For example:  { "$not" : { "$not" : { "$eq" : "mike" } } }
     * should compress down to  { "$not" : { "$eq" : "mike" } }
     *
     * @param clause the clause to be normalized/compressed
     * @return the "$not" compressed clause
     */
    @SuppressWarnings("unchecked")
    private static List<Object> compressMultipleNotOperators(List<Object> clause) {
        List<Object> accumulator = new ArrayList<Object>();

        for (Object fieldClause: clause) {
            Object predicate;
            String fieldName;
            if (fieldClause instanceof Map && !((Map) fieldClause).isEmpty()) {
                Map<String, Object> fieldClauseMap = (Map<String, Object>) fieldClause;
                fieldName = (String) fieldClauseMap.keySet().toArray()[0];
                predicate = fieldClauseMap.get(fieldName);
            } else {
                // if this isn't a map, we don't know what to do so add the clause
                // to the accumulator to be dealt with later as part of the final selector
                // validation.
                accumulator.add(fieldClause);
                continue;
            }

            if (fieldName.startsWith("$") && predicate instanceof List) {
                predicate = compressMultipleNotOperators((List<Object>) predicate);
            } else {
                String operator;
                Object operatorPredicate;
                if (predicate instanceof Map && !((Map) predicate).isEmpty()) {
                    Map<String, Object> predicateMap = (Map<String, Object>) predicate;
                    operator = (String) predicateMap.keySet().toArray()[0];
                    operatorPredicate = predicateMap.get(operator);
                } else {
                    // if this isn't a map, we don't know what to do so add the clause
                    // to the accumulator to be dealt with later as part of the final selector
                    // validation.
                    accumulator.add(fieldClause);
                    continue;
                }
                if (operator.equals(NOT)) {
                    // If a $not operator is encountered we need to check for
                    // a series of nested $not operators.
                    boolean notOpFound = true;
                    boolean negateOperator = false;
                    Object originalOperatorPredicate = operatorPredicate;
                    while (notOpFound) {
                        // if a series of nested $not operators are found then they need to
                        // be compressed down to one $not operator or in the case of an
                        // even set of $not operators, down to zero $not operators.
                        if (operatorPredicate instanceof Map) {
                            Map<String, Object> notClauseMap;
                            notClauseMap = (Map<String, Object>) operatorPredicate;
                            String nextOperator = (String) notClauseMap.keySet().toArray()[0];
                            if (nextOperator.equals(NOT)) {
                                // Each time we find a $not operator we flip the negateOperator's
                                // boolean value.
                                negateOperator = !negateOperator;
                                operatorPredicate = notClauseMap.get(nextOperator);
                            } else {
                                notOpFound = false;
                            }
                        } else {
                            // unexpected condition - revert back to original
                            operatorPredicate = originalOperatorPredicate;
                            negateOperator = false;
                            notOpFound = false;
                        }
                    }
                    if (negateOperator) {
                        Map<String, Object> operatorPredicateMap;
                        operatorPredicateMap = (Map<String, Object>) operatorPredicate;
                        operator = (String) operatorPredicateMap.keySet().toArray()[0];
                        operatorPredicate = operatorPredicateMap.get(operator);
                    }
                    ((Map<String, Object>) predicate).clear();
                    ((Map<String, Object>) predicate).put(operator, operatorPredicate);
                }
            }

            Map<String, Object> element = new HashMap<String, Object>();
            element.put(fieldName, predicate);
            accumulator.add(element);
        }

        return accumulator;
    }

    private static boolean validateCompoundOperatorOperand(Object operand) {
        if (!(operand instanceof List)) {
            String msg = String.format("Argument to compound operator is not a List: %s",
                                       operand.toString());
            logger.log(Level.SEVERE, msg);
            return false;
        }
        return true;
    }

    /**
     *  we are going to need to walk the query tree to validate it before executing it
     */
    @SuppressWarnings("unchecked")
    private static boolean validateSelector(Map<String, Object> selector) {
        String topLevelOp = (String) selector.keySet().toArray()[0];

        // top level op can only be $and or $or after normalisation
        if (topLevelOp.equals(AND) || topLevelOp.equals(OR)) {
            Object topLevelArg = selector.get(topLevelOp);
            if (topLevelArg instanceof List) {
                // safe we know its a List
                return validateCompoundOperatorClauses((List<Object>) topLevelArg,
                                                       new Boolean[]{ false });
            }
        }

        return false;
    }

    /**
     * This method runs the list of clauses making up the selector through a series of
     * validation steps and returns whether the clause list is valid or not.
     *
     * @param clauses A list of clauses making up a query selector
     * @param textClauseLimitReached A flag used to track the text clause limit
     *                               throughout the validation process.  The
     *                               current limit is one text clause per query.
     * @return true/false whether the list of clauses passed validation.
     */
    @SuppressWarnings("unchecked")
    private static boolean validateCompoundOperatorClauses(List<Object> clauses,
                                                           Boolean[] textClauseLimitReached) {
        boolean valid = false;

        for (Object obj : clauses) {
            valid = false;
            if (!(obj instanceof Map)) {
                String msg = String.format("Operator argument must be a Map %s",
                                           clauses.toString());
                logger.log(Level.SEVERE, msg);
                break;
            }
            Map<String, Object> clause = (Map<String, Object>) obj;
            if (clause.size() != 1) {
                String msg;
                msg = String.format("Operator argument clause should have one key value pair: %s",
                                    clauses.toString());
                logger.log(Level.SEVERE, msg);
                break;
            }

            String key = (String) clause.keySet().toArray()[0];
            if (Arrays.asList(OR, NOT, AND).contains(key)) {
                // this should have a list as top level type
                Object compoundClauses = clause.get(key);
                if (validateCompoundOperatorOperand(compoundClauses)) {
                    // validate list
                    valid = validateCompoundOperatorClauses((List<Object>) compoundClauses,
                                                            textClauseLimitReached);
                }
            } else if (!(key.startsWith("$"))) {
                // this should have a map
                // send this for validation
                valid = validateClause((Map<String, Object>) clause.get(key));
            } else if (key.equalsIgnoreCase(TEXT)) {
                // this should have a map
                // send this for validation

                // TODO Enable text search as part of text search unit tests PR.
                // valid = validateTextClause(clause.get(key),
                //                           textClauseLimitReached);
                logger.log(Level.INFO, "Text search is currently not supported.");
                break;
            } else {
                String msg = String.format("%s operator cannot be a top level operator", key);
                logger.log(Level.SEVERE, msg);
                break;
            }

            if (!valid) {
                break;  // if we have gotten here with valid being false, we should abort
            }
        }

        return valid;
    }

    @SuppressWarnings("unchecked")
    private static boolean validateClause(Map<String, Object> clause) {
        List<String> validOperators = Arrays.asList("$eq",
                                                    "$lt",
                                                    "$gt",
                                                    "$exists",
                                                    "$not",
                                                    "$gte",
                                                    "$lte",
                                                    "$in");
        if (clause.size() == 1) {
            String operator = (String) clause.keySet().toArray()[0];
            if (validOperators.contains(operator)) {
                // contains correct operator
                Object clauseOperand = clause.get(operator);
                // Handle special cases:
                //  - $not is the only operator that expects a Map
                //  - $in is the only operator that expects a List
                if (operator.equals(NOT)) {
                    return clauseOperand instanceof Map && validateClause((Map) clauseOperand);
                } else if (operator.equals(IN)) {
                    return clauseOperand instanceof List &&
                           validateInListValues((List<Object>) clauseOperand);
                } else {
                    return validatePredicateValue(clauseOperand, operator);
                }
            }
        }

        return false;
    }

    /**
     * This method handles the special case where a text search clause is encountered.
     * This case is special because a $text operator expects a Map value whose key can
     * only be the $search operator.
     *
     * @param clause The text clause to validate
     * @param textClauseLimitReached A flag used to track the text clause limit
     *                               throughout the validation process.  The
     *                               current limit is one text clause per query.
     * @return true/false whether the clause is valid
     */
    @SuppressWarnings("unchecked")
    private static boolean validateTextClause(Object clause,
                                              Boolean[] textClauseLimitReached) {
        Map<String, Object> textClause;
        if (!(clause instanceof Map)) {
            String msg = String.format("Text search expects a Map, found %s instead.", clause);
            logger.log(Level.SEVERE, msg);
            return false;
        }

        textClause = (Map<String, Object>) clause;
        if (textClause.size() != 1) {
            String msg = String.format("Unexpected content %s in text search.", textClause);
            logger.log(Level.SEVERE, msg);
            return false;
        }

        String operator = (String) textClause.keySet().toArray()[0];
        if (!operator.equals(SEARCH)) {
            String msg = String.format("Invalid operator %s in text search", operator);
            logger.log(Level.SEVERE, msg);
            return false;
        }


        if (textClauseLimitReached[0]) {
            logger.log(Level.SEVERE, "Multiple text search clauses not allowed in a query.  " +
                                     "Rewrite query to contain at most one text search clause.");
            return false;
        }

        textClauseLimitReached[0] = true;
        return validatePredicateValue(textClause.get(operator), operator);
    }

    private static boolean validateInListValues(List<Object> inListValues) {
        boolean valid = true;

        for (Object value : inListValues) {
            if (!validatePredicateValue(value, IN)) {
                valid = false;
                break;
            }
        }

        return valid;
    }

    private static boolean validatePredicateValue(Object predicateValue, String operator) {
        if (operator.equals(EXISTS)) {
            return validateExistsArgument(predicateValue);
        } else if (operator.equals(SEARCH)) {
            return validateTextSearchArgument(predicateValue);
        } else if (validateNotAFloat(predicateValue)) {
            return (predicateValue instanceof String || predicateValue instanceof Number);
        }

        return false;
    }

    private static boolean validateExistsArgument(Object exists) {
        boolean valid = true;

        if (!(exists instanceof Boolean)) {
            valid = false;
            logger.log(Level.SEVERE, "$exists operator expects true or false");
        }

        return valid;
    }

    private static boolean validateTextSearchArgument(Object textSearch) {
        boolean valid = true;

        if (!(textSearch instanceof String)) {
            valid = false;
            logger.log(Level.SEVERE, "$search operator expects a String");
        }

        return valid;
    }

    private static boolean validateNotAFloat(Object value) {
        boolean valid = true;

        if (value instanceof Float) {
            valid = false;
            logger.log(Level.SEVERE, "Float value found in query: %f - Use Double instead.", value);
        }

        return valid;
    }

}
