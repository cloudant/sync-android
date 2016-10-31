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

import static com.cloudant.sync.query.IndexMatcherHelpers.getIndexNameMatcher;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *  The aim is to make sure that the post hoc matcher class behaves the
 *  same as the SQL query engine.
 *
 *  To accomplish this, we test the entire test execution pipeline contained
 *  within this class using {@link com.cloudant.sync.query.MockMatcherIndexManager}
 *  which exercises the post hoc matcher matching functionality.  We then test the
 *  execution pipeline with {@link IndexManagerImpl}
 *  which tests query functionality under standard "production" conditions.
 *
 *  Note: We do not execute the tests contained within this class against the
 *  {@link com.cloudant.sync.query.MockSQLOnlyIndexManager} since the queries contained
 *  within this class specifically only test queries without covering indexes.  Therefore
 *  these tests would all fail.
 *
 *  @see com.cloudant.sync.query.MockMatcherIndexManager
 *  @see IndexManagerImpl
 *  @see com.cloudant.sync.query.MockSQLOnlyIndexManager
 */
@RunWith(Parameterized.class)
public class QueryWithoutCoveringIndexesTest extends AbstractQueryTestBase {

    private static final String QUERY_TYPE = "Queries without covering indexes";
    private static final String STANDARD_EXECUTION = String.format("Standard - %s", QUERY_TYPE);
    private static final String MATCHER_EXECUTION = String.format("Matcher - %s", QUERY_TYPE);

    private String testType = null;

    private IndexManager idxMgr = null;
    
    @Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{ {MATCHER_EXECUTION},
                                             {STANDARD_EXECUTION} });
    }

    public QueryWithoutCoveringIndexesTest(String testType) {
        this.testType = testType;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (testType.equals(MATCHER_EXECUTION)) {
            idxMgr = new MockMatcherIndexManager(im);
        } else if (testType.equals(STANDARD_EXECUTION)) {
            idxMgr = im;
        }
        indexManagerDatabaseQueue = TestUtils.getDBQueue(im);
        assertThat(im, is(notNullValue()));
        assertThat(indexManagerDatabaseQueue, is(notNullValue()));
        String[] metadataTableList = new String[] { IndexManagerImpl.INDEX_METADATA_TABLE_NAME };
        SQLDatabaseTestUtils.assertTablesExist(indexManagerDatabaseQueue, metadataTableList);
    }

    // When executing AND queries

    @Test
    public void canQueryWithoutIndexSingleClause() throws Exception {
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
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike72", "fred12"));
    }

    @Test
    public void canIterateOverDocumentsWithPostHocMatcher() throws Exception {
        setUpWithoutCoveringIndexesQueryData();

        for (int i = 0; i < 51; i++){
            DocumentRevision rev = new DocumentRevision("rhys"+i);
            Map<String, Object> bodyMap = new HashMap<String, Object>();
            bodyMap.put("name", "rhys");
            bodyMap.put("age", 12);
            bodyMap.put("pet", "cat");
            bodyMap.put("town", "cardiff");
            rev.setBody(DocumentBodyFactory.create(bodyMap));
            ds.createDocumentFromRevision(rev);
        }

        Map<String, Object> query = new HashMap<String, Object>();
        query.put("town", "bristol");
        QueryResult queryResult = idxMgr.find(query);

        int count = 0;
        for(DocumentRevision rev: queryResult){

            assertThat("Rev is not null", rev,is(notNullValue()));
            count++;
        }
        assertThat(count, is(2)); // Should have called .next() twice.

    }

    @Test
    public void canIterateOverDocumentsWithPostHocMatcher101() throws Exception {
        setUpWithoutCoveringIndexesQueryData();

        for (int i = 0; i < 101; i++){
            DocumentRevision rev = new DocumentRevision("rhys"+i);
            Map<String, Object> bodyMap = new HashMap<String, Object>();
            bodyMap.put("name", "rhys");
            bodyMap.put("age", 12);
            bodyMap.put("pet", "cat");
            if ( i % 2 == 0) {
                bodyMap.put("town", "cardiff");
            } else {
                bodyMap.put("town", "bristol"); //+2
            }
            rev.setBody(DocumentBodyFactory.create(bodyMap));
            ds.createDocumentFromRevision(rev);
        }

        Map<String, Object> query = new HashMap<String, Object>();
        query.put("town", "bristol");
        QueryResult queryResult = idxMgr.find(query);
        int count = 0;
        for(DocumentRevision rev: queryResult){

            assertThat("Rev is not null", rev,is(notNullValue()));
            assertThat((String) rev.getBody().asMap().get("town"), is("bristol"));
            count++;
        }
        assertThat(count, is(52)); // should have called the iterator.next 52 times

    }

    @Test
    public void canQueryWithoutIndexMultiClause() throws Exception {
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
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    @Test
    public void postHocMatchesProjectingOverNonQueriedFields() throws Exception {
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
        QueryResult queryResult = idxMgr.find(query, 0, 0, Arrays.asList("name"), null);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike72", "fred12"));
    }

    // When executing OR queries

    @Test
    public void canQueryORWithAMissingIndex() throws Exception {
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
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike72",
                                                                 "fred34",
                                                                 "fred12"));
    }

    @Test
    public void canQueryORWithoutAnyIndexes() throws Exception {
        setUpWithoutCoveringIndexesQueryData();
        idxMgr.deleteIndex("pet");
        assertThat(idxMgr.listIndexes(), contains(getIndexNameMatcher("basic")));
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
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike72",
                                                                 "fred34",
                                                                 "fred12"));
    }

// When executing queries containing $size operator

    @Test
    public void worksWhenArrayCountMatchesUsingSIZE() throws Exception {
        setUpSizeOperatorQueryData();
        // query - { "pet" : { "$size" : 2 } }
        Map<String, Object> sizeOp = new HashMap<String, Object>();
        sizeOp.put("$size", 2);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", sizeOp);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred34", "bill34"));
    }

    @Test
    public void returnsEmptyResultSetWhenArrayCountDoesNotMatchUsingSIZE() throws Exception {
        setUpSizeOperatorQueryData();
        // query - { "pet" : { "$size" : 3 } }
        Map<String, Object> sizeOp = new HashMap<String, Object>();
        sizeOp.put("$size", 3);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", sizeOp);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void matchesOnArrayExistenceAndArrayCountEqualOneUsingSIZE() throws Exception {
        setUpSizeOperatorQueryData();
        // query - { "pet" : { "$size" : 1 } }
        Map<String, Object> sizeOp = new HashMap<String, Object>();
        sizeOp.put("$size", 1);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", sizeOp);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike24", "fred72"));
    }

    @Test
    public void matchesOnArrayExistenceAndArrayCountEqualZeroUsingSIZE() throws Exception {
        setUpSizeOperatorQueryData();
        // query - { "pet" : { "$size" : 0 } }
        Map<String, Object> sizeOp = new HashMap<String, Object>();
        sizeOp.put("$size", 0);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", sizeOp);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("fred11"));
    }

    @Test
    public void returnsEmptyResultSetWhenArgumentIsNegativeUsingSIZE() throws Exception {
        setUpSizeOperatorQueryData();
        // query - { "pet" : { "$size" : -1 } }
        Map<String, Object> sizeOp = new HashMap<String, Object>();
        sizeOp.put("$size", -1);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", sizeOp);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void returnsEmptyResultSetWhenArgumentIsNotAWholeNumberUsingSIZE() throws Exception {
        setUpSizeOperatorQueryData();
        // query - { "pet" : { "$size" : 1.6 } }
        Map<String, Object> sizeOp = new HashMap<String, Object>();
        sizeOp.put("$size", 1.6);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", sizeOp);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void returnsEmptyResultSetWhenArgumentIsAStringUsingSIZE() throws Exception {
        setUpSizeOperatorQueryData();
        // query - { "pet" : { "$size" : "dog" } }
        Map<String, Object> sizeOp = new HashMap<String, Object>();
        sizeOp.put("$size", "dog");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", sizeOp);
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void returnsNullWhenArgumentIsInvalidUsingSIZE() throws Exception {
        setUpSizeOperatorQueryData();
        // query - { "pet" : { "$size" : [1] } }
        Map<String, Object> sizeOp = new HashMap<String, Object>();
        sizeOp.put("$size", Collections.singletonList(1));
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", sizeOp);
        assertThat(idxMgr.find(query), is(nullValue()));
    }

    @Test
    public void performsCompoundANDQueryWithSingleSIZEClause() throws Exception {
        setUpSizeOperatorQueryData();
        // query - { "$and" : [ { "pet" : { "$size" : 2 } }, { "name" : "mike" } ] }
        Map<String, Object> sizeOp = new HashMap<String, Object>();
        sizeOp.put("$size", 2);
        Map<String, Object> pet = new HashMap<String, Object>();
        pet.put("pet", sizeOp);
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$and", Arrays.<Object>asList(pet, name));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    @Test
    public void performsCompoundANDQueryOnSameFieldWithSingleSIZEClause() throws Exception {
        setUpSizeOperatorQueryData();
        // query - { "$and" : [ { "pet" : { "$size" : 2 } }, { "pet" : "dog" } ] }
        Map<String, Object> sizeOp = new HashMap<String, Object>();
        sizeOp.put("$size", 2);
        Map<String, Object> pet = new HashMap<String, Object>();
        pet.put("pet", sizeOp);
        Map<String, Object> petEq = new HashMap<String, Object>();
        petEq.put("pet", "dog");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$and", Arrays.<Object>asList(pet, petEq));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred34"));
    }

    @Test
    public void performsCompoundORQueryWithSingleSIZEClause() throws Exception {
        setUpSizeOperatorQueryData();
        // query - { "$or" : [ { "pet" : { "$size" : 1 } }, { "name" : "john" } ] }
        Map<String, Object> sizeOp = new HashMap<String, Object>();
        sizeOp.put("$size", 1);
        Map<String, Object> pet = new HashMap<String, Object>();
        pet.put("pet", sizeOp);
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", "john");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(pet, name));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike24",
                                                                 "fred72",
                                                                 "john12",
                                                                 "john44"));
    }

    @Test
    public void performsCompoundORQueryWithMultipleSIZEClausesOnSameField() throws Exception {
        setUpSizeOperatorQueryData();
        // query - { "$or" : [ { "pet" : { "$size" : 1 } }, { "pet" : { "$size" : 2 } } ] }
        Map<String, Object> sizeOp1 = new HashMap<String, Object>();
        sizeOp1.put("$size", 1);
        Map<String, Object> petOp1 = new HashMap<String, Object>();
        petOp1.put("pet", sizeOp1);
        Map<String, Object> sizeOp2 = new HashMap<String, Object>();
        sizeOp2.put("$size", 2);
        Map<String, Object> petOp2 = new HashMap<String, Object>();
        petOp2.put("pet", sizeOp2);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(petOp1, petOp2));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike24",
                                                                 "mike12",
                                                                 "fred34",
                                                                 "fred72",
                                                                 "bill34"));
    }

    @Test
    public void returnsEmptyForCompoundANDWithMultipleSIZEClausesOnSameField() throws Exception {
        setUpSizeOperatorQueryData();
        // query - { "$and" : [ { "pet" : { "$size" : 1 } }, { "pet" : { "$size" : 2 } } ] }
        Map<String, Object> sizeOp1 = new HashMap<String, Object>();
        sizeOp1.put("$size", 1);
        Map<String, Object> petOp1 = new HashMap<String, Object>();
        petOp1.put("pet", sizeOp1);
        Map<String, Object> sizeOp2 = new HashMap<String, Object>();
        sizeOp2.put("$size", 2);
        Map<String, Object> petOp2 = new HashMap<String, Object>();
        petOp2.put("pet", sizeOp2);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$and", Arrays.<Object>asList(petOp1, petOp2));
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void canQueryWithoutAnyUserDefinedIndexes() throws Exception {
        setUpWithoutCoveringIndexesQueryData();
        // query - { "town" : "bristol" }
        // indexes - No user defined indexes found.  Retrieves
        //           document ids directly from the datastore.
        idxMgr.deleteIndex("basic");
        idxMgr.deleteIndex("pet");
        assertThat(idxMgr.listIndexes(), is(empty()));

        Map<String, Object> query = new HashMap<String, Object>();
        query.put("town", "bristol");
        QueryResult queryResult = idxMgr.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike72", "fred12"));
    }

}
