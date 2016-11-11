//  Copyright © 2015 Cloudant. All rights reserved.
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

package com.cloudant.sync.internal.query;

import com.cloudant.sync.documentstore.Database;
import com.cloudant.sync.internal.sqlite.SQLDatabaseQueue;

import java.util.Map;

/**
 * This sub class of the {@link QueryExecutor} along with
 * {@link MockSQLOnlyQuery} is used by query
 * executor tests to force the tests to exclusively exercise the SQL engine logic.
 * This class is used for testing purposes only.
 *
 * @see QueryExecutor
 * @see MockSQLOnlyQuery
 */
public class MockSQLOnlyQueryExecutor extends QueryExecutor{

    MockSQLOnlyQueryExecutor(Database database, SQLDatabaseQueue queue) {
        super(database, queue);
    }

    @Override
    protected UnindexedMatcher matcherForIndexCoverage(Boolean[] indexesCoverQuery, Map<String, Object> selector) {
        return null;
    }
}
