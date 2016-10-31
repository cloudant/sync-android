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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.runners.Parameterized.Parameters;

import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.util.SQLDatabaseTestUtils;
import com.cloudant.sync.util.TestUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  The aim is to make sure that the post hoc matcher class behaves the
 *  same as the SQL query engine.
 *
 *  To accomplish this, we test the entire test execution pipeline contained
 *  within this class using {@link com.cloudant.sync.query.MockSQLOnlyIndexManager}
 *  which exercises the SQL query engine matching functionality.  We then test
 *  the same test execution pipeline contained within this class using
 *  {@link com.cloudant.sync.query.MockMatcherIndexManager} which in turn
 *  exercises the post hoc matcher matching functionality.  Finally we test the
 *  execution pipeline with {@link IndexManagerImpl}
 *  which tests query functionality under standard "production" conditions.
 *
 *  @see com.cloudant.sync.query.MockSQLOnlyIndexManager
 *  @see com.cloudant.sync.query.MockMatcherIndexManager
 *  @see IndexManagerImpl
 */
@RunWith(Parameterized.class)
public class QueryCoveringIndexesTest extends AbstractQueryTestBase {

    private static final String QUERY_TYPE = "Queries covering indexes";
    private static final String STANDARD_EXECUTION = String.format("Standard - %s", QUERY_TYPE);
    private static final String MATCHER_EXECUTION = String.format("Matcher - %s", QUERY_TYPE);
    private static final String SQL_ONLY_EXECUTION = String.format("SQL Engine - %s", QUERY_TYPE);

    private String testType = null;

    private IndexManager idxMgr = null;

    @Parameters(name = "{0}")
    public static Iterable<Object[]> data() throws Exception {
        return Arrays.asList(new Object[][]{ { SQL_ONLY_EXECUTION },
                                             { MATCHER_EXECUTION },
                                             { STANDARD_EXECUTION } });
    }

    public QueryCoveringIndexesTest(String testType) {
        this.testType = testType;
    }

    @Override
    public void setUp() throws Exception{
        super.setUp();
        if (testType.equals(SQL_ONLY_EXECUTION)) {
            idxMgr = new MockSQLOnlyIndexManager(im);
        } else if (testType.equals(MATCHER_EXECUTION)) {
            idxMgr = new MockMatcherIndexManager(im);
        } else if (testType.equals(STANDARD_EXECUTION)) {
            idxMgr = im;
        }
        indexManagerDatabaseQueue = TestUtils.getDBQueue(im);
        assertThat(im, is(notNullValue()));
        assertThat(indexManagerDatabaseQueue, is(notNullValue()));
        String[] metadataTableList = new String[] { IndexManagerImpl.INDEX_METADATA_TABLE_NAME };
        SQLDatabaseTestUtils.assertTablesExist(TestUtils.getDBQueue(im), metadataTableList);
    }

    // When executing AND queries

    @Test(expected = NullPointerException.class)
    public void returnsNullForNoQuery() throws Exception {
        setUpBasicQueryData();
        idxMgr.find(null);
    }

    @Test
    // Since Floats are not allowed, the query is rejected when a Float is encountered as a value.
    public void returnsNullWhenQueryContainsInvalidValue() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "eq" : "mike" }, "age" : { "$eq" : 12.0f } }
        Map<String, Object> nameOperator = new HashMap<String, Object>();
        nameOperator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", nameOperator);
        Map<String, Object> ageOperator = new HashMap<String, Object>();
        ageOperator.put("$eq", 12.0f);
        query.put("age", ageOperator);
        assertThat(idxMgr.find(query), is(nullValue()));
    }

    @Test
    public void returnsDocForQueryWithBool() throws Exception {
        setUpBasicQueryData();
        DocumentRevision rev = new DocumentRevision("marriedMike");
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        bodyMap.put("married", true);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);
        assertThat(idxMgr.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age"), new FieldSort("married")), "married"), is("married"));
        // query - { "married" : { "eq" : true } }
        Map<String, Object> marriedOperator = new HashMap<String, Object>();
        marriedOperator.put("$eq", true);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("married", marriedOperator);
        QueryResult result = idxMgr.find(query);
        assertThat(result, is(notNullValue()));
        assertThat(result.documentIds().size(), is(1));
    }

    @Test
    public void returnsDocForQueryAgainstIndexWithSpecialChar() throws Exception {
        DocumentRevision rev = new DocumentRevision("mike12");
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);
        assertThat(idxMgr.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "basic index"), is
                ("basic index"));

        // query - { "name" : "mike" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        QueryResult queryResult = idxMgr.find(query);
        List<String> docCheckList = new ArrayList<String>();
        for (DocumentRevision revision: queryResult) {
            assertThat(revision.getId(), is(notNullValue()));
            assertThat(revision.getBody(), is(notNullValue()));
            docCheckList.add(revision.getId());
        }
        assertThat(queryResult.size(), is(docCheckList.size()));
        assertThat(queryResult.documentIds(), containsInAnyOrder(docCheckList.toArray()));
    }

    @Test
    public void validateIteratorContentWithDocumentIdsList() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : "mike" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        QueryResult queryResult = idxMgr.find(query);
        List<String> docCheckList = new ArrayList<String>();
        for (DocumentRevision rev: queryResult) {
            assertThat(rev.getId(), is(notNullValue()));
            assertThat(rev.getBody(), is(notNullValue()));
            docCheckList.add(rev.getId());
        }
        assertThat(queryResult.size(), is(docCheckList.size()));
        assertThat(queryResult.documentIds(), containsInAnyOrder(docCheckList.toArray()));
	}

    @Test
    public void returnsAllDocsForEmptyQuery() throws Exception {
        setUpBasicQueryData();
        Map<String, Object> query = new HashMap<String, Object>();
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike34",
                                                                 "mike72",
                                                                 "fred34",
                                                                 "fred12"));
    }

    @Test
    public void canQueryOverOneStringField() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : "mike" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "mike72"));
    }

    @Test
    public void canQueryOverOneStringFieldNormalized() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "mike72"));
    }

    @Test
    public void canQueryOverOneNumberField() throws Exception {
        setUpBasicQueryData();
        // query - { "age" : 12 }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", 12);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred12"));
    }

    @Test
    public void canQueryOverOneNumberFieldNormalized() throws Exception {
        setUpBasicQueryData();
        // query - { "age" : { "$eq" : 12 } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", operator);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred12"));
    }

    @Test
    public void canQueryOverTwoStringFields() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : "mike", "pet" : "cat" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        query.put("pet", "cat");
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike72"));
    }

    @Test
    public void canQueryOverTwoStringFieldsNormalized() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" }, "pet" : { "$eq" : "cat" } }
        Map<String, Object> nameOperator = new HashMap<String, Object>();
        nameOperator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", nameOperator);
        Map<String, Object> petOperator = new HashMap<String, Object>();
        petOperator.put("$eq", "cat");
        query.put("pet", petOperator);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike72"));
    }

    @Test
    public void canQueryOverTwoMixedFields() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : "mike", "age" : "12" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        query.put("age", 12);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    @Test
    public void canQueryOverTwoMixedFieldsNormalized() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" }, "age" : { "$eq" : 12 } }
        Map<String, Object> nameOperator = new HashMap<String, Object>();
        nameOperator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", nameOperator);
        Map<String, Object> ageOperator = new HashMap<String, Object>();
        ageOperator.put("$eq", 12);
        query.put("age", ageOperator);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    @Test
    public void noResultsWhenQueryOverOneFieldIsMismatched() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : "bill" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "bill");
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.size(), is(0));
    }

    @Test
    public void noResultsWhenQueryOverTwoFieldsOneIsMismatched() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : "bill", "age" : 12 }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "bill");
        query.put("age", 12);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.size(), is(0));
    }

    @Test
    public void noResultsWhenQueryOverTwoFieldsBothAreMismatched() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : "bill", "age" : 17 }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "bill");
        query.put("age", 17);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.size(), is(0));
    }

    @Test
    public void failsWhenUsingUnsupportedOperator() throws Exception {
        setUpBasicQueryData();
        // query - { "age" : { "$blah" : 12 } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$blah", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", operator);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult, is(nullValue()));
    }

    @Test
    public void worksWhenUsingGTOperatorAlone() throws Exception {
        setUpBasicQueryData();
        // query - { "age" : { "$gt" : 12 } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$gt", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", operator);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike34", "mike72", "fred34"));
    }

    @Test
    public void worksWhenUsingGTOperatorWithOthers() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" }, "age" : { "$gt" : 12 } }
        Map<String, Object> eqOp = new HashMap<String, Object>();
        eqOp.put("$eq", "mike");
        Map<String, Object> gtOp = new HashMap<String, Object>();
        gtOp.put("$gt", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", eqOp);
        query.put("age", gtOp);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike34", "mike72"));
    }

    @Test
    public void canCompareStringsWhenUsingGTOperator() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$gt" : "fred" } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gt", "fred");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", op);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "mike72"));
    }

    @Test
    public void canCompareStringsAsPartOfANDQueryWhenUsingGTOperator() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$gt" : "fred" }, "age" : 34 }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gt", "fred");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", op);
        query.put("age", 34);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("mike34"));
    }

    @Test
    public void worksWhenUsingGTEOperatorAlone() throws Exception {
        setUpBasicQueryData();
        // query - { "age" : { "$gte" : 12 } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$gte", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", operator);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike34",
                                                                 "mike72",
                                                                 "fred12",
                                                                 "fred34"));
    }

    @Test
    public void worksWhenUsingGTEOperatorWithOthers() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" }, "age" : { "$gte" : 12 } }
        Map<String, Object> eqOp = new HashMap<String, Object>();
        eqOp.put("$eq", "mike");
        Map<String, Object> gteOp = new HashMap<String, Object>();
        gteOp.put("$gte", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", eqOp);
        query.put("age", gteOp);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "mike72"));
    }

    @Test
    public void worksWhenUsingLTOperatorAlone() throws Exception {
        setUpBasicQueryData();
        // query - { "age" : { "$lt" : 12 } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$lt", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", operator);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.size(), is(0));
    }

    @Test
    public void worksWhenUsingLTOperatorWithOthers() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" }, "age" : { "$lt" : 12 } }
        Map<String, Object> eqOp = new HashMap<String, Object>();
        eqOp.put("$eq", "mike");
        Map<String, Object> ltOp = new HashMap<String, Object>();
        ltOp.put("$lt", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", eqOp);
        query.put("age", ltOp);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.size(), is(0));
    }

    @Test
    public void canCompareStringsWhenUsingLTOperator() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$lt" : "mike" } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lt", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", op);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred12", "fred34"));
    }

    @Test
    public void canCompareStringsAsPartOfANDQueryWhenUsingLTOperator() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$lt" : "mike" }, "age" : 34 }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lt", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", op);
        query.put("age", 34);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("fred34"));
    }

    @Test
    public void worksWhenUsingLTEOperatorAlone() throws Exception {
        setUpBasicQueryData();
        // query - { "age" : { "$lte" : 12 } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$lte", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", operator);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred12"));
    }

    @Test
    public void worksWhenUsingLTEOperatorWithOthers() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" }, "age" : { "$lte" : 12 } }
        Map<String, Object> eqOp = new HashMap<String, Object>();
        eqOp.put("$eq", "mike");
        Map<String, Object> lteOp = new HashMap<String, Object>();
        lteOp.put("$lte", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", eqOp);
        query.put("age", lteOp);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    // When limiting and offsetting results

    @Test
    public void returnsAllForSkip0Limit0() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = idxMgr.find(query, 0, 0, null, null);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "mike72"));
    }

    @Test
    public void limitsQueryResults() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = idxMgr.find(query, 0, 1, null, null);
        assertThat(queryResult.size(), is(1));
    }

    @Test
    public void limitsQueryAndOffsetsStartingPoint() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult offsetResults = idxMgr.find(query, 1, 1, null, null);
        QueryResult results = idxMgr.find(query, 0, 2, null, null);
        assertThat(results.size(), is(2));
        assertThat(results.documentIds().get(1), is(offsetResults.documentIds().get(0)));
    }

    @Test
    public void disablesLimitFor0() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = idxMgr.find(query, 1, 0, null, null);
        assertThat(queryResult.size(), is(2));
    }

    @Test
    public void returnsAllWhenLimitOverResultBounds() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = idxMgr.find(query, 0, 4, null, null);
        assertThat(queryResult.size(), is(3));
    }

    @Test
    public void returnsAllWhenLimitVeryLarge() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = idxMgr.find(query, 0, 1000, null, null);
        assertThat(queryResult.size(), is(3));
    }

    @Test
    public void returnsEmptyResultSetWhenRangeOutOfBounds() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = idxMgr.find(query, 4, 4, null, null);
        assertThat(queryResult.size(), is(0));
    }

    @Test
    public void returnsAppropriateResultsSkipAndLargeLimitUsed() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = idxMgr.find(query, 1000, 1000, null, null);
        assertThat(queryResult.size(), is(0));
    }

    // When using dotted notation

    @Test
    public void noResultsTwoLevelDottedQuery() throws Exception {
        setUpDottedQueryData();
        // query - { "pet.name" : { "$eq" : "fred" }, "age" : { "$eq" : 12 } }
        Map<String, Object> op1 = new HashMap<String, Object>();
        op1.put("$eq", "fred");
        Map<String, Object> op2 = new HashMap<String, Object>();
        op2.put("$eq", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet.name", op1);
        query.put("age", op2);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult, is(notNullValue()));
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void oneResultTwoLevelDottedQuery() throws Exception {
        setUpDottedQueryData();
        // query - { "pet.name" : { "$eq" : "mike" }, "age" : { "$eq" : 12 } }
        Map<String, Object> op1 = new HashMap<String, Object>();
        op1.put("$eq", "mike");
        Map<String, Object> op2 = new HashMap<String, Object>();
        op2.put("$eq", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet.name", op1);
        query.put("age", op2);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    @Test
    public void multiResultTwoLevelDottedQuery() throws Exception {
        setUpDottedQueryData();
        // query - { "pet.species" : { "$eq" : "cat" } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$eq", "cat");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet.species", op);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike23", "mike34"));
    }

    @Test
    public void resultThreeLevelDottedQuery() throws Exception {
        setUpDottedQueryData();
        // query - { "pet.name.first" : { "$eq" : "mike" } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet.name.first", op);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("mike23"));
    }

    // When using non-ascii text

    @Test
    public void canQueryForNonAsciiValues() throws Exception {
        setUpNonAsciiQueryData();
        assertThat(idxMgr.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name")), "nonascii"), is("nonascii"));
        // query - { "name" : { "$eq" : "اسم" } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$eq", "اسم");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", op);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("اسم34"));
    }

    @Test
    public void canQueryUsingFieldsWithOddNames() throws Exception {
        setUpNonAsciiQueryData();
        assertThat(idxMgr.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("اسم"), new FieldSort("datatype"), new FieldSort("age")), "nonascii"),
                is("nonascii"));
        // query - { "اسم" : { "$eq" : "fred" }, "age" : { "$eq" : 12 } }
        Map<String, Object> op1 = new HashMap<String, Object>();
        op1.put("$eq", "fred");
        Map<String, Object> op2 = new HashMap<String, Object>();
        op2.put("$eq", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("اسم", op1);
        query.put("age", op2);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("fredarabic"));

        // query - { "datatype" : { "$eq" : "fred" }, "age" : { "$eq" : 12 } }
        query.clear();
        query.put("datatype", op1);
        query.put("age", op2);
        queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("freddatatype"));
    }

    // When using OR queries

    @Test
    public void supportsUsingOR() throws Exception {
        setUpOrQueryData();
        // query - { "$or" : [ { "name" : "mike" }, { "pet" : "cat" } ] }
        Map<String, Object> op1 = new HashMap<String, Object>();
        op1.put("name", "mike");
        Map<String, Object> op2 = new HashMap<String, Object>();
        op2.put("pet", "cat");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(op1, op2));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike23",
                                                                 "mike34",
                                                                 "mike72",
                                                                 "fred34"));
    }

    @Test
    public void supportsUsingORWithSpecifiedOperator() throws Exception {
        setUpOrQueryData();
        // query - { "$or" : [ { "name" : "mike" }, { "age" : { "$gt" : 30 } } ] }
        Map<String, Object> op1 = new HashMap<String, Object>();
        op1.put("name", "mike");  // 4
        Map<String, Object> gtAge = new HashMap<String, Object>();
        gtAge.put("$gt", 30);
        Map<String, Object> op2 = new HashMap<String, Object>();
        op2.put("age", gtAge);    // 1
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(op1, op2));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike23",
                                                                 "mike34",
                                                                 "mike72",
                                                                 "fred34"));
    }

    @Test
    public void supportsUsingORInSubTrees() throws Exception {
        setUpOrQueryData();
        // query - { "$or" : [ { "name" : "fred" },
        //                     { "$or" : [ { "age" : 12 },
        //                                 { "pet" : "cat" }
        //                               ]
        //                     }
        //                   ]
        //         }
        Map<String, Object> op1 = new HashMap<String, Object>();
        op1.put("name", "fred");
        Map<String, Object> eqAge = new HashMap<String, Object>();
        eqAge.put("age", 12);
        Map<String, Object> eqPet = new HashMap<String, Object>();
        eqPet.put("pet", "cat");
        Map<String, Object> op2 = new HashMap<String, Object>();
        op2.put("$or", Arrays.<Object>asList(eqAge, eqPet));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(op1, op2));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred12",
                                                                 "fred34",
                                                                 "mike12",
                                                                 "mike72"));
    }

    @Test
    public void supportsUsingORWithSingleOperand() throws Exception {
        setUpOrQueryData();
        // query - { "$or" : [ { "name" : "mike" } ] }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("name", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(op));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike23",
                                                                 "mike34",
                                                                 "mike72"));
    }

    // When using nested queries

    @Test
    public void queryWithTwoLevels() throws Exception {
        setUpNestedQueryData();
        // query - { "$or" : [ { "name" : "mike" },
        //                     { "age" : 34 },
        //                     { "$and" : [ { "name" : "fred" },
        //                                  { "pet" : "snake" }
        //                                ]
        //                     }
        //                   ]
        //         }
        Map<String, Object> nameMapLvl1 = new HashMap<String, Object>();
        nameMapLvl1.put("name", "mike");
        Map<String, Object> ageMapLvl1 = new HashMap<String, Object>();
        ageMapLvl1.put("age", 34);
        Map<String, Object> nameMapLvl2 = new HashMap<String, Object>();
        nameMapLvl2.put("name", "fred");
        Map<String, Object> petMapLvl2 = new HashMap<String, Object>();
        petMapLvl2.put("pet", "snake");
        Map<String, Object> opLvl2 = new HashMap<String, Object>();
        opLvl2.put("$and", Arrays.<Object>asList(nameMapLvl2, petMapLvl2));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(nameMapLvl1, ageMapLvl1, opLvl2));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike23",
                                                                 "mike34",
                                                                 "john34",
                                                                 "fred43"));
    }

    @Test
    public void queryORWithSubOR() throws Exception {
        setUpNestedQueryData();
        // query - { "$or" : [ { "name" : "mike" },
        //                     { "age" : 34 },
        //                     { "$or" : [ { "name" : "fred" },
        //                                  { "pet" : "hamster" }
        //                                ]
        //                     }
        //                   ]
        //         }
        Map<String, Object> nameMapLvl1 = new HashMap<String, Object>();
        nameMapLvl1.put("name", "mike");
        Map<String, Object> ageMapLvl1 = new HashMap<String, Object>();
        ageMapLvl1.put("age", 34);
        Map<String, Object> nameMapLvl2 = new HashMap<String, Object>();
        nameMapLvl2.put("name", "fred");
        Map<String, Object> petMapLvl2 = new HashMap<String, Object>();
        petMapLvl2.put("pet", "hamster");
        Map<String, Object> opLvl2 = new HashMap<String, Object>();
        opLvl2.put("$or", Arrays.<Object>asList(nameMapLvl2, petMapLvl2));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(nameMapLvl1, ageMapLvl1, opLvl2));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike23",
                                                                 "mike34",
                                                                 "john34",
                                                                 "fred43",
                                                                 "fred12"));
    }

    @Test
    public void queryANDWithSubAND() throws Exception {
        setUpNestedQueryData();
        // No docs match all of these
        //
        // query - { "$and" : [ { "name" : "mike" },
        //                      { "age" : 34 },
        //                      { "$and" : [ { "name" : "fred" },
        //                                   { "pet" : "snake" }
        //                                 ]
        //                      }
        //                    ]
        //         }
        Map<String, Object> nameMapLvl1 = new HashMap<String, Object>();
        nameMapLvl1.put("name", "mike");
        Map<String, Object> ageMapLvl1 = new HashMap<String, Object>();
        ageMapLvl1.put("age", 34);
        Map<String, Object> nameMapLvl2 = new HashMap<String, Object>();
        nameMapLvl2.put("name", "fred");
        Map<String, Object> petMapLvl2 = new HashMap<String, Object>();
        petMapLvl2.put("pet", "snake");
        Map<String, Object> opLvl2 = new HashMap<String, Object>();
        opLvl2.put("$and", Arrays.<Object>asList(nameMapLvl2, petMapLvl2));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$and", Arrays.<Object>asList(nameMapLvl1, ageMapLvl1, opLvl2));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void queryORWithSubAND() throws Exception {
        setUpNestedQueryData();
        // No docs match all of these
        //
        // query - { "$or" : [ { "name" : "mike" },
        //                     { "age" : 34 },
        //                     { "$and" : [ { "name" : "fred" },
        //                                  { "pet" : "cat" }
        //                                ]
        //                     }
        //                   ]
        //         }
        Map<String, Object> nameMapLvl1 = new HashMap<String, Object>();
        nameMapLvl1.put("name", "mike");
        Map<String, Object> ageMapLvl1 = new HashMap<String, Object>();
        ageMapLvl1.put("age", 34);
        Map<String, Object> nameMapLvl2 = new HashMap<String, Object>();
        nameMapLvl2.put("name", "fred");
        Map<String, Object> petMapLvl2 = new HashMap<String, Object>();
        petMapLvl2.put("pet", "cat");
        Map<String, Object> opLvl2 = new HashMap<String, Object>();
        opLvl2.put("$and", Arrays.<Object>asList(nameMapLvl2, petMapLvl2));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(nameMapLvl1, ageMapLvl1, opLvl2));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike23",
                                                                 "mike34",
                                                                 "john34"));
    }

    @Test
    public void queryANDWithSubOR() throws Exception {
        setUpNestedQueryData();
        // No docs match all of these
        //
        // query - { "$and" : [ { "name" : "mike" },
        //                      { "age" : 34 },
        //                      { "$or" : [ { "name" : "fred" },
        //                                  { "pet" : "cat" }
        //                                ]
        //                      }
        //                    ]
        //         }
        Map<String, Object> nameMapLvl1 = new HashMap<String, Object>();
        nameMapLvl1.put("name", "mike");
        Map<String, Object> ageMapLvl1 = new HashMap<String, Object>();
        ageMapLvl1.put("age", 34);
        Map<String, Object> nameMapLvl2 = new HashMap<String, Object>();
        nameMapLvl2.put("name", "fred");
        Map<String, Object> petMapLvl2 = new HashMap<String, Object>();
        petMapLvl2.put("pet", "dog");
        Map<String, Object> opLvl2 = new HashMap<String, Object>();
        opLvl2.put("$or", Arrays.<Object>asList(nameMapLvl2, petMapLvl2));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$and", Arrays.<Object>asList(nameMapLvl1, ageMapLvl1, opLvl2));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("mike34"));
    }

    @Test
    public void queryMatchANDWithSubANDWithSubAND() throws Exception {
        setUpNestedQueryData();
        // query - { "$and" : [ { "name" : "mike" },
        //                      { "age" : { "$gt" : 10} },
        //                      { "age" : { "$lt" : 30} },
        //                      { "$and" : [ { "$and" : [ { "pet" : "cat" },
        //                                                { "pet" : { "$gt" : "ant" }
        //                                              ]
        //                                   }
        //                                 ]
        //                      }
        //                    ]
        //         }
        Map<String, Object> nameMapLvl1 = new HashMap<String, Object>();
        nameMapLvl1.put("name", "mike");
        Map<String, Object> ageGtOp = new HashMap<String, Object>();
        ageGtOp.put("$gt", 10);
        Map<String, Object> ageLtOp = new HashMap<String, Object>();
        ageLtOp.put("$lt", 30);
        Map<String, Object> ageMapGtLvl1 = new HashMap<String, Object>();
        ageMapGtLvl1.put("age", ageGtOp);
        Map<String, Object> ageMapLtLvl1 = new HashMap<String, Object>();
        ageMapLtLvl1.put("age", ageLtOp);
        Map<String, Object> petMapEqLvl3 = new HashMap<String, Object>();
        petMapEqLvl3.put("pet", "cat");
        Map<String, Object> petGtOpLvl3 = new HashMap<String, Object>();
        petGtOpLvl3.put("$gt", "ant");
        Map<String, Object> petMapGtLvl3 = new HashMap<String, Object>();
        petMapGtLvl3.put("pet", petGtOpLvl3);
        Map<String, Object> opLvl3 = new HashMap<String, Object>();
        opLvl3.put("$and", Arrays.<Object>asList(petMapEqLvl3, petMapGtLvl3));
        Map<String, Object> opLvl2 = new HashMap<String, Object>();
        opLvl2.put("$and", Arrays.<Object>asList(opLvl3));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$and", Arrays.<Object>asList(nameMapLvl1, ageMapGtLvl1, ageMapLtLvl1, opLvl2));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    @Test
    public void queryNoMatchANDWithSubANDWithSubAND() throws Exception {
        setUpNestedQueryData();
        // query - { "$and" : [ { "name" : "mike" },
        //                      { "age" : { "$gt" : 10} },
        //                      { "age" : { "$lt" : 12} },
        //                      { "$and" : [ { "$and" : [ { "pet" : "cat" },
        //                                                { "pet" : { "$gt" : "ant" }
        //                                              ]
        //                                   }
        //                                 ]
        //                      }
        //                    ]
        //         }
        Map<String, Object> nameMapLvl1 = new HashMap<String, Object>();
        nameMapLvl1.put("name", "mike");
        Map<String, Object> ageGtOp = new HashMap<String, Object>();
        ageGtOp.put("$gt", 10);
        Map<String, Object> ageLtOp = new HashMap<String, Object>();
        ageLtOp.put("$lt", 12);
        Map<String, Object> ageMapGtLvl1 = new HashMap<String, Object>();
        ageMapGtLvl1.put("age", ageGtOp);
        Map<String, Object> ageMapLtLvl1 = new HashMap<String, Object>();
        ageMapLtLvl1.put("age", ageLtOp);
        Map<String, Object> petMapEqLvl3 = new HashMap<String, Object>();
        petMapEqLvl3.put("pet", "cat");
        Map<String, Object> petGtOpLvl3 = new HashMap<String, Object>();
        petGtOpLvl3.put("$gt", "ant");
        Map<String, Object> petMapGtLvl3 = new HashMap<String, Object>();
        petMapGtLvl3.put("pet", petGtOpLvl3);
        Map<String, Object> opLvl3 = new HashMap<String, Object>();
        opLvl3.put("$and", Arrays.<Object>asList(petMapEqLvl3, petMapGtLvl3));
        Map<String, Object> opLvl2 = new HashMap<String, Object>();
        opLvl2.put("$and", Arrays.<Object>asList(opLvl3));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$and", Arrays.<Object>asList(nameMapLvl1, ageMapGtLvl1, ageMapLtLvl1, opLvl2));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void queryORWithSubORAndSubAND() throws Exception {
        setUpNestedQueryData();
        // query - { "$or" : [ { "name" : "mike" },
        //                     { "pet" : "cat" },
        //                     { "$or" : [ { "name" : "mike" },
        //                                 { "pet" : "snake" }
        //                               ]
        //                     },
        //                     { "$and" : [ { "name" : "mike" },
        //                                  { "pet" : "snake" }
        //                                ]
        //                     }
        //                   ]
        //         }
        Map<String, Object> nameMapLvl1 = new HashMap<String, Object>();
        nameMapLvl1.put("name", "mike");
        Map<String, Object> petMapLvl1 = new HashMap<String, Object>();
        petMapLvl1.put("pet", "cat");
        Map<String, Object> nameMapOrLvl2 = new HashMap<String, Object>();
        nameMapOrLvl2.put("name", "mike");
        Map<String, Object> petMapOrLvl2 = new HashMap<String, Object>();
        petMapOrLvl2.put("pet", "snake");
        Map<String, Object> orOpLvl2 = new HashMap<String, Object>();
        orOpLvl2.put("$or", Arrays.<Object>asList(nameMapOrLvl2, petMapOrLvl2));
        Map<String, Object> nameMapAndLvl2 = new HashMap<String, Object>();
        nameMapAndLvl2.put("name", "mike");
        Map<String, Object> petMapAndLvl2 = new HashMap<String, Object>();
        petMapAndLvl2.put("pet", "snake");
        Map<String, Object> andOpLvl2 = new HashMap<String, Object>();
        andOpLvl2.put("$and", Arrays.<Object>asList(nameMapAndLvl2, petMapAndLvl2));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(nameMapLvl1, petMapLvl1, orOpLvl2, andOpLvl2));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike23",
                                                                 "mike34",
                                                                 "fred43"));
    }

    // When querying using _id

    @Test
    public void queryUsing_idWorksAsASingleClause() throws Exception {
        setUpBasicQueryData();
        // query - { "_id" : "mike12" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("_id", "mike12");
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    @Test
    public void queryUsing_idWorksWithOtherClauses() throws Exception {
        setUpBasicQueryData();
        // query - { "_id" : "mike12", "name" : "mike" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("_id", "mike12");
        query.put("name", "mike");
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    // When querying using _rev

    @Test
    public void queryUsing_revWorksAsASingleClause() throws Exception {
        setUpBasicQueryData();
        String docRev = ds.getDocument("mike12").getRevision();
        // query - { "_rev" : <docRev> }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("_rev", docRev);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    @Test
    public void queryUsing_revWorksWithOtherClauses() throws Exception {
        setUpBasicQueryData();
        String docRev = ds.getDocument("mike12").getRevision();
        // query - { "_rev" : <docRev>, "name" : "mike" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("_rev", docRev);
        query.put("name", "mike");
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    // When querying using $not operator

    @Test
    public void canQueryOverOneStringFieldUsingNOT() throws Exception {
        setUpBasicQueryData();
        // query - { "name" : { "$not" : { "$eq" : "mike" } } }
        Map<String, Object> eqOp = new HashMap<String, Object>();
        eqOp.put("$eq", "mike");
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", eqOp);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", notOp);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred12", "fred34"));
    }

    @Test
    public void canQueryOverOneIntegerFieldUsingNOT() throws Exception {
        setUpBasicQueryData();
        // query - { "age" : { "$not" : { "$gt" : 34 } } }
        Map<String, Object> gtOp = new HashMap<String, Object>();
        gtOp.put("$gt", 34);
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", gtOp);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", notOp);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred12",
                                                                 "fred34",
                                                                 "mike12",
                                                                 "mike34"));
    }

    @Test
    public void includesDocumentsWithoutFieldIndexedUsingNOT() throws Exception {
        setUpBasicQueryData();
        // query - { "pet" : { "$not" : { "$eq" : "cat" } } }
        Map<String, Object> eqOp = new HashMap<String, Object>();
        eqOp.put("$eq", "cat");
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", eqOp);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", notOp);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred12", "mike34"));
    }

    @Test
    public void queryMatchWithANDCompoundOperatorUsingNOT() throws Exception {
        setUpBasicQueryData();
        // query - { "$and: [ { "pet" : { "$not" : { "$eq" : "cat" } } },
        //                    { "pet" : { "$not" : { "$eq" : "dog" } } }
        //                  ]
        //         }
        Map<String, Object> eqOp1 = new HashMap<String, Object>();
        eqOp1.put("$eq", "cat");
        Map<String, Object> notOp1 = new HashMap<String, Object>();
        notOp1.put("$not", eqOp1);
        Map<String, Object> petMap1 = new HashMap<String, Object>();
        petMap1.put("pet", notOp1);
        Map<String, Object> eqOp2 = new HashMap<String, Object>();
        eqOp2.put("$eq", "dog");
        Map<String, Object> notOp2 = new HashMap<String, Object>();
        notOp2.put("$not", eqOp2);
        Map<String, Object> petMap2 = new HashMap<String, Object>();
        petMap2.put("pet", notOp2);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$and", Arrays.<Object>asList(petMap1, petMap2));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred12"));
    }

    @Test
    public void queryNoMatchWithANDCompoundOperatorUsingNOT() throws Exception {
        setUpBasicQueryData();
        // query - { "$and: [ { "pet" : { "$not" : { "$eq" : "cat" } } },
        //                    { "pet" : { "$eq" : "cat" } }
        //                  ]
        //         }
        Map<String, Object> eqOp1 = new HashMap<String, Object>();
        eqOp1.put("$eq", "cat");
        Map<String, Object> notOp1 = new HashMap<String, Object>();
        notOp1.put("$not", eqOp1);
        Map<String, Object> petMap1 = new HashMap<String, Object>();
        petMap1.put("pet", notOp1);
        Map<String, Object> eqOp2 = new HashMap<String, Object>();
        eqOp2.put("$eq", "cat");
        Map<String, Object> petMap2 = new HashMap<String, Object>();
        petMap2.put("pet", eqOp2);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$and", Arrays.<Object>asList(petMap1, petMap2));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void queryMatchWithORCompoundOperatorUsingNOT() throws Exception {
        setUpBasicQueryData();
        // query - { "$or: [ { "pet" : { "$not" : { "$eq" : "cat" } } },
        //                   { "pet" : { "$not" : { "$eq" : "dog" } } }
        //                 ]
        //         }
        Map<String, Object> eqOp1 = new HashMap<String, Object>();
        eqOp1.put("$eq", "cat");
        Map<String, Object> notOp1 = new HashMap<String, Object>();
        notOp1.put("$not", eqOp1);
        Map<String, Object> petMap1 = new HashMap<String, Object>();
        petMap1.put("pet", notOp1);
        Map<String, Object> eqOp2 = new HashMap<String, Object>();
        eqOp2.put("$eq", "dog");
        Map<String, Object> notOp2 = new HashMap<String, Object>();
        notOp2.put("$not", eqOp2);
        Map<String, Object> petMap2 = new HashMap<String, Object>();
        petMap2.put("pet", notOp2);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(petMap1, petMap2));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike34",
                                                                 "mike72",
                                                                 "fred34",
                                                                 "fred12"));
    }

    // When indexing array fields

    @Test
    public void canFindDocumentsWithArray() throws Exception {
        setUpArrayIndexingData();
        // query - { "pet" : { "$eq" : "dog" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "dog");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", operator);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34"));
    }

    @Test
    public void canFindDocumentWithoutArray() throws Exception {
        setUpArrayIndexingData();
        // query - { "pet" : { "$eq" : "parrot" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "parrot");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", operator);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred34"));
    }

    @Test
    public void canFindDocumentsWithAndWithoutArray() throws Exception {
        setUpArrayIndexingData();
        // query - { "pet" : { "$eq" : "cat" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "cat");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", operator);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "john22"));
    }

    @Test
    public void canFindDocumentsWithAndWithoutArrayUsingNotNE() throws Exception {
        setUpArrayIndexingData();
        // query - { "pet" : { "$not" : { "$ne" : "cat" } } }
        Map<String, Object> neCat = new HashMap<String, Object>();
        neCat.put("$ne", "cat");
        Map<String, Object> notNeCat = new HashMap<String, Object>();
        notNeCat.put("$not", neCat);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", notNeCat);
        System.out.println("QUERY:" + query);
        QueryResult queryResult = idxMgr.find(query);
        System.out.println("DOCUMENTS: " + queryResult.documentIds());
        // Should be same as $eq
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "john22"));
    }

    // Queries like { "pet" : { "$not" : { "$eq" : "dog" } } }
    //     and      { "pet" : { "$ne" : "dog" } } }
    // Should yield the same result set.  Evidenced in the next two tests.

    @Test
    public void canFindDocumentsWithAndWithoutArrayUsingNOT() throws Exception {
        setUpArrayIndexingData();
        // query - { "pet" : { "$not" : { "$eq" : "dog" } } }
        Map<String, Object> eqOp = new HashMap<String, Object>();
        eqOp.put("$eq", "dog");
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", eqOp);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", notOp);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred34",
                                                                 "john44",
                                                                 "john22",
                                                                 "fred12"));
    }

    @Test
    public void canFindDocumentsWithAndWithoutArrayUsingNE() throws Exception {
        setUpArrayIndexingData();
        // query - { "pet" : { "$ne" : "dog" } } }
        Map<String, Object> neOp = new HashMap<String, Object>();
        neOp.put("$ne", "dog");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", neOp);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred34",
                                                                 "john44",
                                                                 "john22",
                                                                 "fred12"));
    }

    // The $gt and $lte operators are logically opposite.  Consequently querying
    // with those operators and for that matter $gte/$lt will yield result sets
    // that are logically opposite.  Whereas using $not..$gt will yield a result
    // set that consists of documents that do NOT satisfy the "greater than"
    // condition.  This can be a result set that differs from the logical
    // opposite as is evidenced in the following three tests.

    @Test
    public void canFindDocumentsWithAndWithoutArrayUsingGT() throws Exception {
        setUpArrayIndexingData();
        // query - { "pet" : { "$gt" : "dog" } } }
        Map<String, Object> gtOp = new HashMap<String, Object>();
        gtOp.put("$gt", "dog");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", gtOp);
        QueryResult queryResult = idxMgr.find(query);

        // mike34 appears in the results because of the "fish" entry in his
        // list of pets { "pet" : [ "cat", "dog", "fish"] }
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred34", "john44", "mike34"));
    }

    @Test
    public void canFindDocumentsWithAndWithoutArrayUsingLTE() throws Exception {
        setUpArrayIndexingData();
        // query - { "pet" : { "$lte" : "dog" } } }
        Map<String, Object> lteOp = new HashMap<String, Object>();
        lteOp.put("$lte", "dog");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", lteOp);
        QueryResult queryResult = idxMgr.find(query);

        // mike34 appears in the results because of the "cat" entry in his
        // list of pets { "pet" : [ "cat", "dog", "fish"] }
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "john22", "mike34"));
    }

    @Test
    public void canFindDocumentsWithAndWithoutArrayUsingNotGT() throws Exception {
        setUpArrayIndexingData();
        // query - { "pet" : { "$not" : { "$gt" : "dog" } } }
        Map<String, Object> gtOp = new HashMap<String, Object>();
        gtOp.put("$gt", "dog");
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", gtOp);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", notOp);
        QueryResult queryResult = idxMgr.find(query);

        // mike34 does NOT appear in the results here because this result set is strictly
        // a set of documents that are NOT in the set of documents that satisfy the
        // "$gt" : "dog" condition which the Mike34 document is a member of. 
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred12", "john22"));
    }

    @Test
    public void canFindDocumentsWithAndWithoutArrayUsingMultipleNot() throws Exception {
        setUpArrayIndexingData();
        // query - { "$and" : [ { "pet" : { "$not" : { "$eq" : "cat" } } },
        //                      { "pet" : { "$not" : { "$eq" : "dog" } } }
        //                    ] }
        Map<String, Object> eqCat = new HashMap<String, Object>();
        eqCat.put("$eq", "cat");
        Map<String, Object> notCat = new HashMap<String, Object>();
        notCat.put("$not", eqCat);
        Map<String, Object> eqDog = new HashMap<String, Object>();
        eqDog.put("$eq", "dog");
        Map<String, Object> notDog = new HashMap<String, Object>();
        notDog.put("$not", eqDog);
        Map<String, Object> petNotCat = new HashMap<String, Object>();
        petNotCat.put("pet", notCat);
        Map<String, Object> petNotDog = new HashMap<String, Object>();
        petNotDog.put("pet", notDog);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$and", Arrays.<Object>asList(petNotCat, petNotDog));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred34", "fred12", "john44"));
    }

    @Test
    public void returnsNullWhenUsingArrayInQuery() throws Exception {
        setUpArrayIndexingData();
        // query - { "pet" : { "$not" : { "$eq" : [ "dog" ] } } }
        Map<String, Object> eqDog = new HashMap<String, Object>();
        eqDog.put("$eq", Arrays.asList("dog"));
        Map<String, Object> notDog = new HashMap<String, Object>();
        notDog.put("$not", eqDog);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", notDog);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult, is(nullValue()));
    }

    // When querying using $exists operator

    @Test
    public void canFindDocumentWhereFieldDoesNotExist() throws Exception {
        setUpBasicQueryData();
        // query - { "pet" : { "$exists" : false } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$exists", false);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", operator);
        System.out.println(query);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("fred12"));
    }

    @Test
    public void canFindDocumentWhereFieldDoesExist() throws Exception {
        setUpBasicQueryData();
        // query - { "pet" : { "$exists" : true } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$exists", true);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", operator);
        System.out.println(query);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike34",
                                                                 "mike72",
                                                                 "fred34"));
    }

    @Test
    public void canFindDocumentWhereFieldDoesExistUsingNOT() throws Exception {
        setUpBasicQueryData();
        // query - { "pet" : { "$not" : { "$exists" : false } } }
        Map<String, Object> existsOp = new HashMap<String, Object>();
        existsOp.put("$exists", false);
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", existsOp);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", notOp);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike34",
                                                                 "mike72",
                                                                 "fred34"));
    }

    @Test
    public void canFindDocumentWhereFieldDoesNotExistUsingNOT() throws Exception {
        setUpBasicQueryData();
        // query - { "pet" : { "$not" : { "$exists" : true } } }
        Map<String, Object> existsOp = new HashMap<String, Object>();
        existsOp.put("$exists", true);
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", existsOp);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", notOp);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("fred12"));
    }

    // When querying using $in operator

    @Test
    public void canFindDocumentsWithArraysUsingIN() throws Exception {
        setUpArrayIndexingData();
        // query - { "pet" : { "$in" : [ "fish", "hamster" ] } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$in", Arrays.<String>asList("fish", "hamster"));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", op);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike34", "john44"));
    }

    @Test
    public void canFindDocumentWithoutArraysUsingIN() throws Exception {
        setUpArrayIndexingData();
        // query - { "pet" : { "$in" : [ "parrot", "turtle" ] } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$in", Arrays.<String>asList("parrot", "turtle"));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", op);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("fred34"));
    }

    @Test
    public void canFindDocumentsWithAndWithoutArraysUsingIN() throws Exception {
        setUpArrayIndexingData();
        // query - { "pet" : { "$in" : [ "cat", "dog" ] } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$in", Arrays.<String>asList("cat", "dog"));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", op);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "john22"));
    }

    @Test
    public void returnsEmptyResultWhenNoMatchUsingIN() throws Exception {
        setUpArrayIndexingData();
        // query - { "pet" : { "$in" : [ "turtle", "pig" ] } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$in", Arrays.<String>asList("turtle", "pig"));
        op.put("$eq", "turtle");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", op);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.size(), is(0));
    }

    @Test
    public void canFindDocumentsWithAndWithoutArraysUsingNOTIN() throws Exception {
        setUpArrayIndexingData();
        // query - { "pet" : { "$not" : { "$in" : [ "cat", "dog" ] } } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$in", Arrays.<String>asList("cat", "dog"));
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", op);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", notOp);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred12", "fred34", "john44"));
    }

    // When there is a large result set

    @Test
     public void limitsCorrectlyWithLargeResultSet() throws Exception {
        setUpLargeResultSetQueryData();
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("large_field", "cat");
        QueryResult queryResult = idxMgr.find(query, 0, 60, null, null);
        assertThat(queryResult.documentIds().size(), is(60));
    }

    @Test
    public void skipsAndLimitsAcrossBatchBorder() throws Exception {
        setUpLargeResultSetQueryData();
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("large_field", "cat");
        QueryResult queryResult = idxMgr.find(query, 90, 20, null, Arrays.<FieldSort>asList(new FieldSort("idx", FieldSort.Direction.ASCENDING)));
        List<String> expected = new ArrayList<String>();
        for (int i = 90; i < 110; i++) {
            expected.add(String.format("d%d", i));
        }
        assertThat(queryResult.documentIds(), contains(expected.toArray()));
    }

    // When querying using $mod operator

    @Test
    public void worksWhenUsingPositiveDivisorWithMOD() throws Exception {
        setUpNumericOperationsQueryData();
        // query - { "score": { "$mod": [ 10, 1 ] } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$mod", Arrays.<Object>asList(10, 1));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("score", op);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike31", "fred11"));
    }

    @Test
    public void worksWhenUsingNegativeDivisorWithMOD() throws Exception {
        setUpNumericOperationsQueryData();
        // query - { "score": { "$mod": [ -10, 1 ] } }
        //
        // This test should generate the same result as when
        // using @{ @"score" : @{@"$mod" : @[ @10, @1 ] } }.
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$mod", Arrays.<Object>asList(-10, 1));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("score", op);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike31", "fred11"));
    }

    @Test
    public void worksWhenUsingMODWithOthers() throws Exception {
        setUpNumericOperationsQueryData();
        // query - { "name" : "mike", "score": { "$mod": [ 10, 1 ] } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$mod", Arrays.<Object>asList(10, 1));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        query.put("score", op);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike31"));
    }

    @Test
    public void returnsEmptyResultSetWhenMODAppliedToNonNumericField() throws Exception {
        setUpNumericOperationsQueryData();
        // query - { "name": { "$mod": [ 10, 1 ] } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$mod", Arrays.<Object>asList(10, 1));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", op);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void worksWhenRemainderIs0UsingMOD() throws Exception {
        setUpNumericOperationsQueryData();
        // query - { "score": { "$mod": [ 5, 0 ] } }
        //
        // The score field value will be truncated to the
        // whole number before modulo arithmetic is applied.
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$mod", Arrays.<Object>asList(5, 0));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("score", op);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("john15",
                                                                 "john-15",
                                                                 "john15.2",
                                                                 "john15.6",
                                                                 "john0",
                                                                 "john0.0",
                                                                 "john0.6",
                                                                 "john-0.6"));
    }

    // The following two tests ensure that we are using truncated division in the mod operator.
    
    @Test
    public void worksWhenRemainderIsNegativeUsingMOD() throws Exception {
        setUpNumericOperationsQueryData();
        // query - { "score": { "$mod": [ 10, -5 ] } }
        //
        // A negative remainder can only be achieved if the dividend
        // is negative, which in this case is the value of the score field.
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$mod", Arrays.<Object>asList(10, -5));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("score", op);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("john-15"));
    }

    @Test
    public void worksWhenDivisorAndRemainderAreNegativeUsingMOD() throws Exception {
        setUpNumericOperationsQueryData();
        // query - { "score": { "$mod": [ -10, -5 ] } }
        //
        // This test should generate the same result as when
        // using { "score": { "$mod": [ 10, -5 ] } }.
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$mod", Arrays.<Object>asList(-10, -5));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("score", op);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("john-15"));
    }

    @Test
    public void worksWhenRemainderIsNotAWholeNumberUsingMOD() throws Exception {
        setUpNumericOperationsQueryData();
        // query - { "score": { "$mod": [ 10, 1.6 ] } }
        //
        // The remainder is truncated to the whole number prior to the operation
        // being performed.  This test should generate the same result as when
        // using { "score": { "$mod": [ 10, 1 ] } }.
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$mod", Arrays.<Object>asList(10, 1.6));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("score", op);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike31", "fred11"));
    }

    @Test
    public void worksWhenDivisorIsNotAWholeNumberUsingMOD() throws Exception {
        setUpNumericOperationsQueryData();
        // query - { "score": { "$mod": [ 5.4, 0 ] } }
        //
        // The divisor is truncated to the whole number prior
        // to the operation being performed.
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$mod", Arrays.<Object>asList(5.4, 0));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("score", op);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("john15",
                                                                 "john-15",
                                                                 "john15.2",
                                                                 "john15.6",
                                                                 "john0",
                                                                 "john0.0",
                                                                 "john0.6",
                                                                 "john-0.6"));
    }
    
}
