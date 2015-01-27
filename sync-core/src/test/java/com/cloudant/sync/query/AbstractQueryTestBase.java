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

import com.cloudant.sync.datastore.DocumentRevision;

import org.junit.Test;

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
 *  within this class using {@link com.cloudant.sync.query.QueryExecutorSQLOnlyTest}
 *  which exercises the SQL query engine matching functionality.  We then test
 *  the same test execution pipeline contained within this class using
 *  {@link com.cloudant.sync.query.QueryExecutorMatcherTest} which in turn
 *  exercises the post hoc matcher matching functionality.  Finally we test the
 *  execution pipeline with {@link com.cloudant.sync.query.QueryExecutorStandardTest}
 *  which tests query functionality under standard "production" conditions.
 *
 *  @see com.cloudant.sync.query.QueryExecutorSQLOnlyTest
 *  @see com.cloudant.sync.query.QueryExecutorMatcherTest
 *  @see com.cloudant.sync.query.QueryExecutorStandardTest
 */
public abstract class AbstractQueryTestBase extends AbstractQueryTestSetUp {

    // When executing AND queries

    @Test
    public void returnsNullForNoQuery() {
        setUpBasicQueryData();
        assertThat(im.find(null), is(nullValue()));
    }

    @Test
    // Since Floats are not allowed, the query is rejected when a Float is encountered as a value.
    public void returnsNullWhenQueryContainsInvalidValue() {
        setUpBasicQueryData();
        // query - { "name" : { "eq" : "mike" }, "age" : { "$eq" : 12.0f } }
        Map<String, Object> nameOperator = new HashMap<String, Object>();
        nameOperator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", nameOperator);
        Map<String, Object> ageOperator = new HashMap<String, Object>();
        ageOperator.put("$eq", 12.0f);
        query.put("age", ageOperator);
        assertThat(im.find(query), is(nullValue()));
    }

    @Test
    public void validateIteratorContentWithDocumentIdsList() {
        setUpBasicQueryData();
        // query - { "name" : "mike" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        QueryResult queryResult = im.find(query);
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
    public void returnsAllDocsForEmptyQuery() {
        setUpBasicQueryData();
        Map<String, Object> query = new HashMap<String, Object>();
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike34",
                                                                 "mike72",
                                                                 "fred34",
                                                                 "fred12"));
    }

    @Test
    public void canQueryOverOneStringField() {
        setUpBasicQueryData();
        // query - { "name" : "mike" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "mike72"));
    }

    @Test
    public void canQueryOverOneStringFieldNormalized() {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "mike72"));
    }

    @Test
    public void canQueryOverOneNumberField() {
        setUpBasicQueryData();
        // query - { "age" : 12 }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", 12);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred12"));
    }

    @Test
    public void canQueryOverOneNumberFieldNormalized() {
        setUpBasicQueryData();
        // query - { "age" : { "$eq" : 12 } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", operator);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred12"));
    }

    @Test
    public void canQueryOverTwoStringFields() {
        setUpBasicQueryData();
        // query - { "name" : "mike", "pet" : "cat" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        query.put("pet", "cat");
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike72"));
    }

    @Test
    public void canQueryOverTwoStringFieldsNormalized() {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" }, "pet" : { "$eq" : "cat" } }
        Map<String, Object> nameOperator = new HashMap<String, Object>();
        nameOperator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", nameOperator);
        Map<String, Object> petOperator = new HashMap<String, Object>();
        petOperator.put("$eq", "cat");
        query.put("pet", petOperator);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike72"));
    }

    @Test
    public void canQueryOverTwoMixedFields() {
        setUpBasicQueryData();
        // query - { "name" : "mike", "age" : "12" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        query.put("age", 12);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    @Test
    public void canQueryOverTwoMixedFieldsNormalized() {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" }, "age" : { "$eq" : 12 } }
        Map<String, Object> nameOperator = new HashMap<String, Object>();
        nameOperator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", nameOperator);
        Map<String, Object> ageOperator = new HashMap<String, Object>();
        ageOperator.put("$eq", 12);
        query.put("age", ageOperator);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    @Test
    public void noResultsWhenQueryOverOneFieldIsMismatched() {
        setUpBasicQueryData();
        // query - { "name" : "bill" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "bill");
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.size(), is(0));
    }

    @Test
    public void noResultsWhenQueryOverTwoFieldsOneIsMismatched() {
        setUpBasicQueryData();
        // query - { "name" : "bill", "age" : 12 }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "bill");
        query.put("age", 12);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.size(), is(0));
    }

    @Test
    public void noResultsWhenQueryOverTwoFieldsBothAreMismatched() {
        setUpBasicQueryData();
        // query - { "name" : "bill", "age" : 17 }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "bill");
        query.put("age", 17);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.size(), is(0));
    }

    @Test
    public void failsWhenUsingUnsupportedOperator() {
        setUpBasicQueryData();
        // query - { "age" : { "$blah" : 12 } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$blah", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", operator);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult, is(nullValue()));
    }

    @Test
    public void worksWhenUsingGTOperatorAlone() {
        setUpBasicQueryData();
        // query - { "age" : { "$gt" : 12 } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$gt", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", operator);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike34", "mike72", "fred34"));
    }

    @Test
    public void worksWhenUsingGTOperatorWithOthers() {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" }, "age" : { "$gt" : 12 } }
        Map<String, Object> eqOp = new HashMap<String, Object>();
        eqOp.put("$eq", "mike");
        Map<String, Object> gtOp = new HashMap<String, Object>();
        gtOp.put("$gt", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", eqOp);
        query.put("age", gtOp);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike34", "mike72"));
    }

    @Test
    public void canCompareStringsWhenUsingGTOperator() {
        setUpBasicQueryData();
        // query - { "name" : { "$gt" : "fred" } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gt", "fred");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", op);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "mike72"));
    }

    @Test
    public void canCompareStringsAsPartOfANDQueryWhenUsingGTOperator() {
        setUpBasicQueryData();
        // query - { "name" : { "$gt" : "fred" }, "age" : 34 }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gt", "fred");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", op);
        query.put("age", 34);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("mike34"));
    }

    @Test
    public void worksWhenUsingGTEOperatorAlone() {
        setUpBasicQueryData();
        // query - { "age" : { "$gte" : 12 } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$gte", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", operator);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike34",
                                                                 "mike72",
                                                                 "fred12",
                                                                 "fred34"));
    }

    @Test
    public void worksWhenUsingGTEOperatorWithOthers() {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" }, "age" : { "$gte" : 12 } }
        Map<String, Object> eqOp = new HashMap<String, Object>();
        eqOp.put("$eq", "mike");
        Map<String, Object> gteOp = new HashMap<String, Object>();
        gteOp.put("$gte", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", eqOp);
        query.put("age", gteOp);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "mike72"));
    }

    @Test
    public void worksWhenUsingLTOperatorAlone() {
        setUpBasicQueryData();
        // query - { "age" : { "$lt" : 12 } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$lt", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", operator);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.size(), is(0));
    }

    @Test
    public void worksWhenUsingLTOperatorWithOthers() {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" }, "age" : { "$lt" : 12 } }
        Map<String, Object> eqOp = new HashMap<String, Object>();
        eqOp.put("$eq", "mike");
        Map<String, Object> ltOp = new HashMap<String, Object>();
        ltOp.put("$lt", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", eqOp);
        query.put("age", ltOp);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.size(), is(0));
    }

    @Test
    public void canCompareStringsWhenUsingLTOperator() {
        setUpBasicQueryData();
        // query - { "name" : { "$lt" : "mike" } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lt", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", op);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred12", "fred34"));
    }

    @Test
    public void canCompareStringsAsPartOfANDQueryWhenUsingLTOperator() {
        setUpBasicQueryData();
        // query - { "name" : { "$lt" : "mike" }, "age" : 34 }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lt", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", op);
        query.put("age", 34);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("fred34"));
    }

    @Test
    public void worksWhenUsingLTEOperatorAlone() {
        setUpBasicQueryData();
        // query - { "age" : { "$lte" : 12 } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$lte", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", operator);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred12"));
    }

    @Test
    public void worksWhenUsingLTEOperatorWithOthers() {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" }, "age" : { "$lte" : 12 } }
        Map<String, Object> eqOp = new HashMap<String, Object>();
        eqOp.put("$eq", "mike");
        Map<String, Object> lteOp = new HashMap<String, Object>();
        lteOp.put("$lte", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", eqOp);
        query.put("age", lteOp);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    // When limiting and offsetting results

    @Test
    public void returnsAllForSkip0Limit0() {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = im.find(query, 0, 0, null, null);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "mike72"));
    }

    @Test
    public void limitsQueryResults() {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = im.find(query, 0, 1, null, null);
        assertThat(queryResult.size(), is(1));
    }

    @Test
    public void limitsQueryAndOffsetsStartingPoint() {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult offsetResults = im.find(query, 1, 1, null, null);
        QueryResult results = im.find(query, 0, 2, null, null);
        assertThat(results.size(), is(2));
        assertThat(results.documentIds().get(1), is(offsetResults.documentIds().get(0)));
    }

    @Test
    public void disablesLimitFor0() {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = im.find(query, 1, 0, null, null);
        assertThat(queryResult.size(), is(2));
    }

    @Test
    public void returnsAllWhenLimitOverResultBounds() {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = im.find(query, 0, 4, null, null);
        assertThat(queryResult.size(), is(3));
    }

    @Test
    public void returnsAllWhenLimitVeryLarge() {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = im.find(query, 0, 1000, null, null);
        assertThat(queryResult.size(), is(3));
    }

    @Test
    public void returnsEmptyResultSetWhenRangeOutOfBounds() {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = im.find(query, 4, 4, null, null);
        assertThat(queryResult.size(), is(0));
    }

    @Test
    public void returnsAppropriateResultsSkipAndLargeLimitUsed() {
        setUpBasicQueryData();
        // query - { "name" : { "$eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = im.find(query, 1000, 1000, null, null);
        assertThat(queryResult.size(), is(0));
    }

    // When using dotted notation

    @Test
    public void noResultsTwoLevelDottedQuery() {
        setUpDottedQueryData();
        // query - { "pet.name" : { "$eq" : "fred" }, "age" : { "$eq" : 12 } }
        Map<String, Object> op1 = new HashMap<String, Object>();
        op1.put("$eq", "fred");
        Map<String, Object> op2 = new HashMap<String, Object>();
        op2.put("$eq", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet.name", op1);
        query.put("age", op2);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult, is(notNullValue()));
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void oneResultTwoLevelDottedQuery() {
        setUpDottedQueryData();
        // query - { "pet.name" : { "$eq" : "mike" }, "age" : { "$eq" : 12 } }
        Map<String, Object> op1 = new HashMap<String, Object>();
        op1.put("$eq", "mike");
        Map<String, Object> op2 = new HashMap<String, Object>();
        op2.put("$eq", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet.name", op1);
        query.put("age", op2);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    @Test
    public void multiResultTwoLevelDottedQuery() {
        setUpDottedQueryData();
        // query - { "pet.species" : { "$eq" : "cat" } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$eq", "cat");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet.species", op);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike23", "mike34"));
    }

    @Test
    public void resultThreeLevelDottedQuery() {
        setUpDottedQueryData();
        // query - { "pet.name.first" : { "$eq" : "mike" } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet.name.first", op);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("mike23"));
    }

    // When using non-ascii text

    @Test
    public void canQueryForNonAsciiValues() {
        setUpNonAsciiQueryData();
        assertThat(im.ensureIndexed(Arrays.<Object>asList("name"), "nonascii"), is("nonascii"));
        // query - { "name" : { "$eq" : "اسم" } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$eq", "اسم");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", op);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("اسم34"));
    }

    @Test
    public void canQueryUsingFieldsWithOddNames() {
        setUpNonAsciiQueryData();
        assertThat(im.ensureIndexed(Arrays.<Object>asList("اسم", "datatype", "age"), "nonascii"),
                is("nonascii"));
        // query - { "اسم" : { "$eq" : "fred" }, "age" : { "$eq" : 12 } }
        Map<String, Object> op1 = new HashMap<String, Object>();
        op1.put("$eq", "fred");
        Map<String, Object> op2 = new HashMap<String, Object>();
        op2.put("$eq", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("اسم", op1);
        query.put("age", op2);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("fredarabic"));

        // query - { "datatype" : { "$eq" : "fred" }, "age" : { "$eq" : 12 } }
        query.clear();
        query.put("datatype", op1);
        query.put("age", op2);
        queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("freddatatype"));
    }

    // When using OR queries

    @Test
    public void supportsUsingOR() {
        setUpOrQueryData();
        // query - { "$or" : [ { "name" : "mike" }, { "pet" : "cat" } ] }
        Map<String, Object> op1 = new HashMap<String, Object>();
        op1.put("name", "mike");
        Map<String, Object> op2 = new HashMap<String, Object>();
        op2.put("pet", "cat");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(op1, op2));
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike23",
                                                                 "mike34",
                                                                 "mike72",
                                                                 "fred34"));
    }

    @Test
    public void supportsUsingORWithSpecifiedOperator() {
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
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike23",
                                                                 "mike34",
                                                                 "mike72",
                                                                 "fred34"));
    }

    @Test
    public void supportsUsingORInSubTrees() {
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
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred12",
                                                                 "fred34",
                                                                 "mike12",
                                                                 "mike72"));
    }

    @Test
    public void supportsUsingORWithSingleOperand() {
        setUpOrQueryData();
        // query - { "$or" : [ { "name" : "mike" } ] }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("name", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(op));
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike23",
                                                                 "mike34",
                                                                 "mike72"));
    }

    // When using nested queries

    @Test
    public void queryWithTwoLevels() {
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
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike23",
                                                                 "mike34",
                                                                 "john34",
                                                                 "fred43"));
    }

    @Test
    public void queryORWithSubOR() {
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
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike23",
                                                                 "mike34",
                                                                 "john34",
                                                                 "fred43",
                                                                 "fred12"));
    }

    @Test
    public void queryANDWithSubAND() {
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
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void queryORWithSubAND() {
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
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike23",
                                                                 "mike34",
                                                                 "john34"));
    }

    @Test
    public void queryANDWithSubOR() {
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
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("mike34"));
    }

    @Test
    public void queryMatchANDWithSubANDWithSubAND() {
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
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    @Test
    public void queryNoMatchANDWithSubANDWithSubAND() {
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
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void queryORWithSubORAndSubAND() {
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
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike23",
                                                                 "mike34",
                                                                 "fred43"));
    }

    // When querying using _id

    @Test
    public void queryUsing_idWorksAsASingleClause() {
        setUpBasicQueryData();
        // query - { "_id" : "mike12" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("_id", "mike12");
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    @Test
    public void queryUsing_idWorksWithOtherClauses() {
        setUpBasicQueryData();
        // query - { "_id" : "mike12", "name" : "mike" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("_id", "mike12");
        query.put("name", "mike");
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    // When querying using _rev

    @Test
    public void queryUsing_revWorksAsASingleClause() {
        setUpBasicQueryData();
        String docRev = ds.getDocument("mike12").getRevision();
        // query - { "_rev" : <docRev> }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("_rev", docRev);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    @Test
    public void queryUsing_revWorksWithOtherClauses() {
        setUpBasicQueryData();
        String docRev = ds.getDocument("mike12").getRevision();
        // query - { "_rev" : <docRev>, "name" : "mike" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("_rev", docRev);
        query.put("name", "mike");
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    // When querying using $not operator

    @Test
    public void canQueryOverOneStringFieldUsingNOT() {
        setUpBasicQueryData();
        // query - { "name" : { "$not" : { "$eq" : "mike" } } }
        Map<String, Object> eqOp = new HashMap<String, Object>();
        eqOp.put("$eq", "mike");
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", eqOp);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", notOp);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred12", "fred34"));
    }

    @Test
    public void canQueryOverOneIntegerFieldUsingNOT() {
        setUpBasicQueryData();
        // query - { "age" : { "$not" : { "$gt" : 34 } } }
        Map<String, Object> gtOp = new HashMap<String, Object>();
        gtOp.put("$gt", 34);
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", gtOp);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", notOp);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred12",
                                                                 "fred34",
                                                                 "mike12",
                                                                 "mike34"));
    }

    @Test
    public void includesDocumentsWithoutFieldIndexedUsingNOT() {
        setUpBasicQueryData();
        // query - { "pet" : { "$not" : { "$eq" : "cat" } } }
        Map<String, Object> eqOp = new HashMap<String, Object>();
        eqOp.put("$eq", "cat");
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", eqOp);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", notOp);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred12", "mike34"));
    }

    @Test
    public void queryMatchWithANDCompoundOperatorUsingNOT() {
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
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred12"));
    }

    @Test
    public void queryNoMatchWithANDCompoundOperatorUsingNOT() {
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
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void queryMatchWithORCompoundOperatorUsingNOT() {
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
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike34",
                                                                 "mike72",
                                                                 "fred34",
                                                                 "fred12"));
    }

    // When indexing array fields

    @Test
    public void canFindDocumentWithArray() {
        setUpArrayIndexingData();
        // query - { "pet" : { "$eq" : "dog" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "dog");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", operator);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12"));
    }

    @Test
    public void canFindDocumentWithoutArray() {
        setUpArrayIndexingData();
        // query - { "pet" : { "$eq" : "parrot" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "parrot");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", operator);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred34"));
    }

    @Test
    public void canFindDocumentsWithAndWithoutArraysUsingNOT() {
        setUpArrayIndexingData();
        // query - { "pet" : { "$not" : { "$eq" : "dog" } } }
        Map<String, Object> eqOp = new HashMap<String, Object>();
        eqOp.put("$eq", "dog");
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", eqOp);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", notOp);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred34"));
    }

    // When querying using $exists operator

    @Test
    public void canFindDocumentWhereFieldDoesNotExist() {
        setUpBasicQueryData();
        // query - { "pet" : { "$exists" : false } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$exists", false);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", operator);
        System.out.println(query);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("fred12"));
    }

    @Test
    public void canFindDocumentWhereFieldDoesExist() {
        setUpBasicQueryData();
        // query - { "pet" : { "$exists" : true } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$exists", true);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", operator);
        System.out.println(query);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                "mike34",
                "mike72",
                "fred34"));
    }

    @Test
    public void canFindDocumentWhereFieldDoesExistUsingNOT() {
        setUpBasicQueryData();
        // query - { "pet" : { "$not" : { "$exists" : false } } }
        Map<String, Object> existsOp = new HashMap<String, Object>();
        existsOp.put("$exists", false);
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", existsOp);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", notOp);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike34",
                                                                 "mike72",
                                                                 "fred34"));
    }

    @Test
    public void canFindDocumentWhereFieldDoesNotExistUsingNOT() {
        setUpBasicQueryData();
        // query - { "pet" : { "$not" : { "$exists" : true } } }
        Map<String, Object> existsOp = new HashMap<String, Object>();
        existsOp.put("$exists", true);
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", existsOp);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", notOp);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("fred12"));
    }

    // When there is a large result set

    @Test
     public void limitsCorrectlyWithLargeResultSet() {
        setUpLargeResultSetQueryData();
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("large_field", "cat");
        QueryResult queryResult = im.find(query, 0, 60, null, null);
        assertThat(queryResult.documentIds().size(), is(60));
    }

    @Test
    public void skipsAndLimitsAcrossBatchBorder() {
        setUpLargeResultSetQueryData();
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("large_field", "cat");
        QueryResult queryResult = im.find(query, 90, 20, null, null);
        assertThat(queryResult.size(), is(20));
        // Uncomment remainder of test after query sorting is implemented
        //List<String> expected = new ArrayList<String>();
        //for (int i = 90; i < 110; i++) {
        //    expected.add(String.format("d%d", i));
        //}
        //assertThat(queryResult, containsInAnyOrder(expected.toArray()));
    }

}