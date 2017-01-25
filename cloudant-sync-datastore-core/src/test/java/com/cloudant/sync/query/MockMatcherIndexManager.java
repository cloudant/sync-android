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

import com.cloudant.sync.util.Misc;
import com.cloudant.sync.util.TestUtils;

import java.util.List;
import java.util.Map;

/**
 * This sub class of the {@link IndexManagerImpl} along with
 * {@link com.cloudant.sync.query.MockMatcherQueryExecutor} is used by query
 * executor tests to force the tests to exclusively exercise the post hoc matcher logic.
 * This class is used for testing purposes only.
 *
 * @see IndexManagerImpl
 * @see com.cloudant.sync.query.MockMatcherQueryExecutor
 */
public class MockMatcherIndexManager extends DelegatingMockIndexManager {

    public MockMatcherIndexManager(IndexManagerImpl im) {
        super(im);
    }

    /*
     * Override this one variant of find to use the MockMatcherQueryExecutor.
     */
    @Override
    public QueryResult find(Map<String, Object> query,
                            long skip,
                            long limit,
                            List<String> fields,
                            List<FieldSort> sortDocument) throws QueryException {
        Misc.checkNotNull(query, "query");

        updateAllIndexes();

        MockMatcherQueryExecutor queryExecutor = null;
        try {
            queryExecutor = new MockMatcherQueryExecutor(delegate.getDatabase(),
                    TestUtils.getDBQueue(delegate));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        List<Index> indexes = listIndexes();
        return queryExecutor.find(query, indexes, skip, limit, fields, sortDocument);
    }
}
