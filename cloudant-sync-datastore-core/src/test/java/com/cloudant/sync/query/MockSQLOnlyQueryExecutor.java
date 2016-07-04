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

import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLDatabaseQueue;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * This sub class of the {@link com.cloudant.sync.query.QueryExecutor} along with
 * {@link com.cloudant.sync.query.MockSQLOnlyIndexManager} is used by query
 * executor tests to force the tests to exclusively exercise the SQL engine logic.
 * This class is used for testing purposes only.
 *
 * @see com.cloudant.sync.query.QueryExecutor
 * @see com.cloudant.sync.query.MockSQLOnlyIndexManager
 */
public class MockSQLOnlyQueryExecutor extends QueryExecutor{

    MockSQLOnlyQueryExecutor( Datastore datastore, SQLDatabaseQueue queue) {
        super( datastore, queue);
    }

    @Override
    protected UnindexedMatcher matcherForIndexCoverage(Boolean[] indexesCoverQuery, Map<String, Object> selector) {
        return null;
    }
}
