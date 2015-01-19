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

    public static final String AND = "$and";
    public static final String OR = "$or";
    public static final String EQ = "$eq";

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

        if (!validateQueryValue(query)) {
            String msg = String.format("Invalid value encountered in query: %s", query.toString());
            logger.log(Level.SEVERE, msg);
            return null;
        }

        // First expand the query to include a leading compound predicate
        // if there isn't one already.
        query = addImplicitAnd(query);

        // At this point we will have a single entry map, key AND or OR,
        // forming the compound predicate.
        // Next make sure all the predicates have an operator -- the EQ
        // operator is implicit and we need to add it if there isn't one.
        // Take
        //     [ {"field1": @"mike"}, ... ]
        // and make
        //     [ {"field1": { "$eq": "mike"} }, ... } ]
        String compoundOperator = (String) query.keySet().toArray()[0];
        List<Object> predicates = new ArrayList<Object>();
        if (query.get(compoundOperator) instanceof List) {
            predicates = addImplicitEq((List<Object>) query.get(compoundOperator));
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
            // if fieldClause isn't a dictionary, we don't know what to do so pass it back
            if (fieldClause instanceof Map && !((Map) fieldClause).isEmpty()) {
                Map<String, Object> fieldClauseMap = (Map<String, Object>) fieldClause;
                fieldName = (String) fieldClauseMap.keySet().toArray()[0];
                predicate = fieldClauseMap.get(fieldName);
            } else {
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

    private static boolean validateCompoundOperatorOperand(Object operand) {
        if (!(operand instanceof List)) {
            String msg = String.format("Argument to compound operator is not an NSArray: %s",
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
                return validateCompoundOperatorClauses((List<Object>) topLevelArg);
            }
        }

        return false;
    }

    @SuppressWarnings("unchecked")
    private static boolean validateCompoundOperatorClauses(List<Object> clauses) {
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
            if (Arrays.asList("$or", "$not", "$and").contains(key)) {
                // this should have a list as top level type
                Object compoundClauses = clause.get(key);
                if (validateCompoundOperatorOperand(compoundClauses)) {
                    // validate list
                    valid = validateCompoundOperatorClauses((List) compoundClauses);
                }
            } else if (!(key.startsWith("$"))) {
                // this should have a map
                // send this for validation
                valid = validateClause((Map<String, Object>) clause.get(key));
            } else {
                String msg = String.format("%s operator cannot be a top level operator", key);
                logger.log(Level.SEVERE, msg);
                break;
            }

            if (!valid) {
                break;  // if we have gotten here with valid being no, we should abort
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
                                                    "$ne",
                                                    "$gte",
                                                    "$lte");
        if (clause.size() == 1) {
            String operator = (String) clause.keySet().toArray()[0];
            if (validOperators.contains(operator)) {
                // contains correct operator
                Object clauseOperand = clause.get(operator);
                // handle special case, $not is the only op that expects a dict
                if (operator.equals("$not") && clauseOperand instanceof Map) {
                    return validateClause((Map) clauseOperand);
                } else if (validatePredicateValue(clauseOperand, operator)) {
                    return true;
                }
            }
        }

        return false;
    }

    private static boolean validatePredicateValue(Object predicateValue, String operator) {
        if (operator.equals("$exists")) {
            return validateExistsArgument(predicateValue);
        } else {
            return (predicateValue instanceof String ||
                    predicateValue instanceof Number && !(predicateValue instanceof Float));
        }
    }

    private static boolean validateExistsArgument(Object exists) {
        boolean valid = true;

        if (!(exists instanceof Boolean)) {
            valid = false;
            logger.log(Level.SEVERE, "$exists operator expects true or false");
        }

        return valid;
    }

    private static boolean validateQueryValue(Object value) {
        boolean valid = true;
        if (value instanceof Map) {
            for (Object key: ((Map) value).keySet()) {
                valid = valid && validateQueryValue(((Map) value).get(key));
            }
        } else if (value instanceof List) {
            for (Object element : (List) value) {
                valid = valid && validateQueryValue(element);
            }
        } else if (value instanceof Float) {
            valid = false;
        }

        return valid;
    }

}