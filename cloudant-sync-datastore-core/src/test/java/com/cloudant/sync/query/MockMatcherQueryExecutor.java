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

import com.cloudant.sync.datastore.Database;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLDatabaseQueue;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This sub class of the {@link com.cloudant.sync.query.QueryExecutor} along with
 * {@link com.cloudant.sync.query.MockMatcherIndexManager} is used by query
 * executor tests to force the tests to exclusively exercise the post hoc matcher logic.
 * This class is used for testing purposes only.
 *
 * @see com.cloudant.sync.query.QueryExecutor
 * @see com.cloudant.sync.query.MockMatcherIndexManager
 */
public class MockMatcherQueryExecutor extends QueryExecutor{

    private final Set<String> docIds;

    MockMatcherQueryExecutor(Database database, SQLDatabaseQueue queue) {
        super(database, queue);
        docIds = new HashSet<String>(database.getAllDocumentIds());
    }

    // return just a blank node (we don't execute it anyway).
    @Override
    protected ChildrenQueryNode translateQuery(Map<String, Object> query,
                                               Map<String, Map<String, Object>> indexes,
                                               Boolean[] indexesCoverQuery) {
        return new AndQueryNode();
    }

    // just return all doc IDs rather than executing the query nodes
    @Override
    protected Set<String> executeQueryTree(QueryNode node, SQLDatabase db) {
        return docIds;
    }
}
