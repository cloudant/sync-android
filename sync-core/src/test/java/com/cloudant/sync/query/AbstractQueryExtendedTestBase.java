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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 *  The aim is to make sure that the post hoc matcher class behaves the
 *  same as the SQL query engine.
 *
 *  To accomplish this, we test the entire test execution pipeline contained
 *  within this class using {@link com.cloudant.sync.query.QueryExecutorMatcherTest}
 *  which in exercises the post hoc matcher matching functionality.  We then test the
 *  execution pipeline with {@link com.cloudant.sync.query.QueryExecutorStandardTest}
 *  which tests query functionality under standard "production" conditions.
 *
 *  Note: We do not execute the tests contained within this abstract class against the
 *  {@link com.cloudant.sync.query.MockSQLOnlyQueryExecutor} since the queries contained
 *  within this class specifically only test queries without covering indexes.  Therefore
 *  these tests would all fail.
 *
 *  @see com.cloudant.sync.query.QueryExecutorSQLOnlyTest
 *  @see com.cloudant.sync.query.QueryExecutorMatcherTest
 *  @see com.cloudant.sync.query.MockSQLOnlyQueryExecutor
 */
public abstract class AbstractQueryExtendedTestBase extends AbstractQueryTestBase {

    // When executing AND queries

    @Test
    public void canQueryWithoutIndexSingleClause() {
        setUpWithoutCoveringIndexesQueryData();
        // query - { "town" : "bristol" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("town", "bristol");
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike72", "fred12"));
    }

    @Test
    public void canQueryWithoutIndexMultiClause() {
        setUpWithoutCoveringIndexesQueryData();
        // query - { "pet" : { "$eq" : "cat" }, { "age" : { "$eq" : 12 } }
        Map<String, Object> op1 = new HashMap<String, Object>();
        op1.put("$eq", "cat");
        Map<String, Object> op2 = new HashMap<String, Object>();
        op2.put("$eq", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", op1);
        query.put("age", op2);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

}