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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.datastore.MutableDocumentRevision;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.TestUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
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
 *  exercises the post hoc matcher matching functionality.
 */
public abstract class AbstractQueryTestBase {

    String factoryPath = null;
    DatastoreManager factory = null;
    DatastoreExtended ds = null;
    IndexManager im = null;
    SQLDatabase db = null;

    @Before
    public void setUp() throws SQLException {
        factoryPath = TestUtils.createTempTestingDir(AbstractQueryTestBase.class.getName());
        assertThat(factoryPath, is(notNullValue()));
        factory = new DatastoreManager(factoryPath);
        assertThat(factory, is(notNullValue()));
        ds = (DatastoreExtended) factory.openDatastore(AbstractQueryTestBase.class.getSimpleName());
        assertThat(ds, is(notNullValue()));

        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "mike12";
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "mike34";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "dog");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "mike72";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 72);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "fred34";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "fred12";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

    }

    @After
    public void tearDown() {
        im.close();
        assertThat(im.getQueue().isShutdown(), is(true));
        ds.close();
        TestUtils.deleteDatabaseQuietly(db);
        TestUtils.deleteTempTestingDir(factoryPath);

        im = null;
        ds = null;
        factory = null;
        factoryPath = null;
    }

    @Test
    public void returnsNullForNoQuery() {
        assertThat(im.find(null), is(nullValue()));
    }

    @Test
    // Since Floats are not allowed, the query is rejected when a Float is encountered as a value.
    public void returnsNullWhenQueryContainsInvalidValue() {
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
        // query - { "name" : "mike" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.size(), is((long) queryResult.documentIds().size()));
        List<String> docCheckList = new ArrayList<String>();
        for (DocumentRevision rev: queryResult) {
            assertThat(rev.getId(), is(notNullValue()));
            assertThat(rev.getBody(), is(notNullValue()));
            docCheckList.add(rev.getId());
        }
        assertThat(queryResult.size(), is((long) docCheckList.size()));
        assertThat(queryResult.documentIds(), containsInAnyOrder(docCheckList.toArray()));
    }

    @Test
    public void canQueryOverOneStringField() {
        // query - { "name" : "mike" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.size(), is(3l));
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "mike72"));
    }

    @Test
    public void canQueryOverOneStringFieldNormalized() {
        // query - { "name" : { "eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.size(), is(3l));
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "mike72"));
    }

    @Test
    public void canQueryOverOneNumberField() {
        // query - { "age" : 12 }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", 12);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.size(), is(2l));
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred12"));
    }

    @Test
    public void canQueryOverOneNumberFieldNormalized() {
        // query - { "age" : { "eq" : 12 } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", operator);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.size(), is(2l));
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred12"));
    }

    @Test
    public void canQueryOverTwoStringFields() {
        // query - { "name" : "mike", "pet" : "cat" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        query.put("pet", "cat");
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.size(), is(2l));
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike72"));
    }

    @Test
    public void canQueryOverTwoStringFieldsNormalized() {
        // query - { "name" : { "eq" : "mike" }, "pet" : { "$eq" : "cat" } }
        Map<String, Object> nameOperator = new HashMap<String, Object>();
        nameOperator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", nameOperator);
        Map<String, Object> petOperator = new HashMap<String, Object>();
        petOperator.put("$eq", "cat");
        query.put("pet", petOperator);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.size(), is(2l));
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike72"));
    }

    @Test
    public void canQueryOverTwoMixedFields() {
        // query - { "name" : "mike", "age" : "12" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        query.put("age", 12);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.size(), is(1l));
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    @Test
    public void canQueryOverTwoMixedFieldsNormalized() {
        // query - { "name" : { "eq" : "mike" }, "age" : { "$eq" : 12 } }
        Map<String, Object> nameOperator = new HashMap<String, Object>();
        nameOperator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", nameOperator);
        Map<String, Object> ageOperator = new HashMap<String, Object>();
        ageOperator.put("$eq", 12);
        query.put("age", ageOperator);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.size(), is(1l));
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    @Test
    public void noResultsWhenQueryOverOneFieldIsMismatched() {
        // query - { "name" : "bill" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "bill");
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.size(), is(0l));
    }

    @Test
    public void noResultsWhenQueryOverTwoFieldsOneIsMismatched() {
        // query - { "name" : "bill", "age" : 12 }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "bill");
        query.put("age", 12);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.size(), is(0l));
    }

    @Test
    public void noResultsWhenQueryOverTwoFieldsBothAreMismatched() {
        // query - { "name" : "bill", "age" : 17 }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "bill");
        query.put("age", 17);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.size(), is(0l));
    }

    // When limiting and offsetting results

    @Test
    public void returnsAllForSkip0Limit0() {
        // query - { "name" : { "eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = im.find(query, 0, 0, null, null);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "mike72"));
    }

    @Test
    public void limitsQueryResults() {
        // query - { "name" : { "eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = im.find(query, 0, 1, null, null);
        assertThat(queryResult.size(), is(1l));
    }

    @Test
    public void limitsQueryAndOffsetsStartingPoint() {
        // query - { "name" : { "eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult offsetResults = im.find(query, 1, 1, null, null);
        QueryResult results = im.find(query, 0, 2, null, null);
        assertThat(results.size(), is(2l));
        assertThat(results.documentIds().get(1), is(offsetResults.documentIds().get(0)));
    }

    @Test
    public void disablesLimitFor0() {
        // query - { "name" : { "eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = im.find(query, 1, 0, null, null);
        assertThat(queryResult.size(), is(2l));
    }

    @Test
    public void returnsAllWhenLimitOverResultBounds() {
        // query - { "name" : { "eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = im.find(query, 0, 4, null, null);
        assertThat(queryResult.size(), is(3l));
    }

    @Test
    public void returnsAllWhenLimitVeryLarge() {
        // query - { "name" : { "eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = im.find(query, 0, 1000, null, null);
        assertThat(queryResult.size(), is(3l));
    }

    @Test
    public void returnsEmptyResultSetWhenRangeOutOfBounds() {
        // query - { "name" : { "eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = im.find(query, 4, 4, null, null);
        assertThat(queryResult.size(), is(0l));
    }

    @Test
    public void returnsAppropriateResultsSkipAndLargeLimitUsed() {
        // query - { "name" : { "eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = im.find(query, 1000, 1000, null, null);
        assertThat(queryResult.size(), is(0l));
    }

}