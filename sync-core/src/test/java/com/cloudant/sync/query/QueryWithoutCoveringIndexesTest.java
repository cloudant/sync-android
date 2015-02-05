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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.runners.Parameterized.Parameters;

import com.cloudant.sync.util.SQLDatabaseTestUtils;
import com.cloudant.sync.util.TestUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 *  The aim is to make sure that the post hoc matcher class behaves the
 *  same as the SQL query engine.
 *
 *  To accomplish this, we test the entire test execution pipeline contained
 *  within this class using {@link com.cloudant.sync.query.MockMatcherIndexManager}
 *  which exercises the post hoc matcher matching functionality.  We then test the
 *  execution pipeline with {@link com.cloudant.sync.query.IndexManager}
 *  which tests query functionality under standard "production" conditions.
 *
 *  Note: We do not execute the tests contained within this class against the
 *  {@link com.cloudant.sync.query.MockSQLOnlyIndexManager} since the queries contained
 *  within this class specifically only test queries without covering indexes.  Therefore
 *  these tests would all fail.
 *
 *  @see com.cloudant.sync.query.MockMatcherIndexManager
 *  @see com.cloudant.sync.query.IndexManager
 *  @see com.cloudant.sync.query.MockSQLOnlyIndexManager
 */
@RunWith(Parameterized.class)
public class QueryWithoutCoveringIndexesTest extends AbstractQueryTestBase {

    private static final String QUERY_TYPE = "Queries without covering indexes";
    private static final String STANDARD_EXECUTION = String.format("Standard - %s", QUERY_TYPE);
    private static final String MATCHER_EXECUTION = String.format("Matcher - %s", QUERY_TYPE);

    private String testType = null;

    @Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{ {MATCHER_EXECUTION},
                                             {STANDARD_EXECUTION} });
    }

    public QueryWithoutCoveringIndexesTest(String testType) {
        this.testType = testType;
    }

    @Override
    public void setUp() throws SQLException {
        super.setUp();
        if (testType.equals(MATCHER_EXECUTION)) {
            im = new MockMatcherIndexManager(ds);
        } else if (testType.equals(STANDARD_EXECUTION)) {
            im = new IndexManager(ds);
        }
        assertThat(im, is(notNullValue()));
        db = TestUtils.getDatabaseConnectionToExistingDb(im.getDatabase());
        assertThat(db, is(notNullValue()));
        assertThat(im.getQueue(), is(notNullValue()));
        String[] metadataTableList = new String[] { IndexManager.INDEX_METADATA_TABLE_NAME };
        SQLDatabaseTestUtils.assertTablesExist(db, metadataTableList);
    }

    // When executing AND queries

    @Test
    public void canQueryWithoutIndexSingleClause() {
        setUpWithoutCoveringIndexesQueryData();
        // query - { "town" : "bristol" }
        // indexes - { "basic" : { "name" : "basic", "type" : "json", "fields" : [ "_id",
        //                                                                         "_rev",
        //                                                                         "name",
        //                                                                         "age" ] } }
        //
        //         - { "pet" : { "name" : "pet", "type" : "json", "fields" : [ "_id",
        //                                                                     "_rev",
        //                                                                     "name",
        //                                                                     "pet" ] } }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("town", "bristol");
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike72", "fred12"));
    }

    @Test
    public void canQueryWithoutIndexMultiClause() {
        setUpWithoutCoveringIndexesQueryData();
        // query - { "pet" : { "$eq" : "cat" }, { "age" : { "$eq" : 12 } }
        // indexes - { "basic" : { "name" : "basic", "type" : "json", "fields" : [ "_id",
        //                                                                         "_rev",
        //                                                                         "name",
        //                                                                         "age" ] } }
        //
        //         - { "pet" : { "name" : "pet", "type" : "json", "fields" : [ "_id",
        //                                                                     "_rev",
        //                                                                     "name",
        //                                                                     "pet" ] } }
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

    @Test
    public void postHocMatchesProjectingOverNonQueriedFields() {
        setUpWithoutCoveringIndexesQueryData();
        // query - { "town" : "bristol" }
        // indexes - { "basic" : { "name" : "basic", "type" : "json", "fields" : [ "_id",
        //                                                                         "_rev",
        //                                                                         "name",
        //                                                                         "age" ] } }
        //
        //         - { "pet" : { "name" : "pet", "type" : "json", "fields" : [ "_id",
        //                                                                     "_rev",
        //                                                                     "name",
        //                                                                     "pet" ] } }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("town", "bristol");
        QueryResult queryResult = im.find(query, 0, 0, Arrays.asList("name"), null);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike72", "fred12"));
    }

    // When executing OR queries

    @Test
    public void canQueryORWithAMissingIndex() {
        setUpWithoutCoveringIndexesQueryData();
        // query - { "$or" : [ { "pet" : { "$eq" : "cat" } }, { "town" : { "$eq" : "bristol" } } ] }
        // indexes - { "basic" : { "name" : "basic", "type" : "json", "fields" : [ "_id",
        //                                                                         "_rev",
        //                                                                         "name",
        //                                                                         "age" ] } }
        //
        //         - { "pet" : { "name" : "pet", "type" : "json", "fields" : [ "_id",
        //                                                                     "_rev",
        //                                                                     "name",
        //                                                                     "pet" ] } }
        Map<String, Object> op1 = new HashMap<String, Object>();
        op1.put("$eq", "cat");
        Map<String, Object> op2 = new HashMap<String, Object>();
        op2.put("$eq", "bristol");
        Map<String, Object> petEq = new HashMap<String, Object>();
        petEq.put("pet", op1);
        Map<String, Object> townEq = new HashMap<String, Object>();
        townEq.put("town", op2);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(petEq, townEq));
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike72",
                                                                 "fred34",
                                                                 "fred12"));
    }

    @Test
    public void canQueryORWithoutAnyIndexes() {
        setUpWithoutCoveringIndexesQueryData();
        im.deleteIndexNamed("pet");
        assertThat(im.listIndexes().keySet(), contains("basic"));
        // query - { "$or" : [ { "pet" : { "$eq" : "cat" } }, { "town" : { "$eq" : "bristol" } } ] }
        // indexes - { "basic" : { "name" : "basic", "type" : "json", "fields" : [ "_id",
        //                                                                         "_rev",
        //                                                                         "name",
        //                                                                         "age" ] } }
        Map<String, Object> op1 = new HashMap<String, Object>();
        op1.put("$eq", "cat");
        Map<String, Object> op2 = new HashMap<String, Object>();
        op2.put("$eq", "bristol");
        Map<String, Object> petEq = new HashMap<String, Object>();
        petEq.put("pet", op1);
        Map<String, Object> townEq = new HashMap<String, Object>();
        townEq.put("town", op2);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(petEq, townEq));
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike72",
                                                                 "fred34",
                                                                 "fred12"));
    }

}