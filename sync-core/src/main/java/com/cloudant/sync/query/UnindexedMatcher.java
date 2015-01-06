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

import java.util.Map;

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

    private static ChildrenQueryNode buildExecutionTreeForSelector(Map<String, Object> selector) {
        // At this point we will have a root compound predicate, AND or OR, and
        // the query will be reduced to a single entry:
        // { "$and": [ ... predicates (possibly compound) ... ] }
        // { "$or": [ ... predicates (possibly compound) ... ] }

        // TODO - build execution tree for selector
        return null;
    }

}