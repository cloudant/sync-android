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

import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.datastore.MutableDocumentRevision;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryExecutorTest extends AbstractIndexTestBase {

    @Override
    public void setUp() throws SQLException {
        super.setUp();

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

        List<Object> fieldNames = Arrays.<Object>asList("name", "age");
        Assert.assertEquals("basic", im.ensureIndexed(fieldNames, "basic"));

        fieldNames = Arrays.<Object>asList("name", "pet");
        Assert.assertEquals("pet", im.ensureIndexed(fieldNames, "pet"));
    }

    @Test
    public void returnsNullForNoQuery() {
        Assert.assertNull(im.find(null));
    }

    @Test
    public void canQueryOverOneStringField() {
        // query - { "name" : "mike" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        QueryResult queryResult = im.find(query);
        Assert.assertNotNull(queryResult);
        Assert.assertEquals(3, queryResult.size());
        List<String> checkList = Arrays.asList("mike12", "mike34", "mike72");
        Assert.assertEquals(3, queryResult.documentIds().size());
        Assert.assertTrue(queryResult.documentIds().containsAll(checkList));
    }

    @Test
    public void canQueryOverOneStringFieldNormalized() {
        // query - { "name" : { "eq" : "mike" } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", operator);
        QueryResult queryResult = im.find(query);
        Assert.assertNotNull(queryResult);
        Assert.assertEquals(3, queryResult.size());
        List<String> checkList = Arrays.asList("mike12", "mike34", "mike72");
        Assert.assertEquals(3, queryResult.documentIds().size());
        Assert.assertTrue(queryResult.documentIds().containsAll(checkList));
    }

    @Test
    public void canQueryOverOneNumberField() {
        // query - { "age" : 12 }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", 12);
        QueryResult queryResult = im.find(query);
        Assert.assertNotNull(queryResult);
        Assert.assertEquals(2, queryResult.size());
        List<String> checkList = Arrays.asList("mike12", "fred12");
        Assert.assertEquals(2, queryResult.documentIds().size());
        Assert.assertTrue(queryResult.documentIds().containsAll(checkList));
    }

    @Test
    public void canQueryOverOneNumberFieldNormalized() {
        // query - { "age" : { "eq" : 12 } }
        Map<String, Object> operator = new HashMap<String, Object>();
        operator.put("$eq", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("age", operator);
        QueryResult queryResult = im.find(query);
        Assert.assertNotNull(queryResult);
        Assert.assertEquals(2, queryResult.size());
        List<String> checkList = Arrays.asList("mike12", "fred12");
        Assert.assertEquals(2, queryResult.documentIds().size());
        Assert.assertTrue(queryResult.documentIds().containsAll(checkList));
    }

    @Test
    public void canQueryOverTwoStringFields() {
        // query - { "name" : "mike", "pet" : "cat" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        query.put("pet", "cat");
        QueryResult queryResult = im.find(query);
        Assert.assertNotNull(queryResult);
        Assert.assertEquals(2, queryResult.size());
        List<String> checkList = Arrays.asList("mike12", "mike72");
        Assert.assertEquals(2, queryResult.documentIds().size());
        Assert.assertTrue(queryResult.documentIds().containsAll(checkList));
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
        Assert.assertNotNull(queryResult);
        Assert.assertEquals(2, queryResult.size());
        List<String> checkList = Arrays.asList("mike12", "mike72");
        Assert.assertEquals(2, queryResult.documentIds().size());
        Assert.assertTrue(queryResult.documentIds().containsAll(checkList));
    }

    @Test
    public void canQueryOverTwoMixedFields() {
        // query - { "name" : "mike", "age" : "12" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        query.put("age", 12);
        QueryResult queryResult = im.find(query);
        Assert.assertNotNull(queryResult);
        Assert.assertEquals(1, queryResult.size());
        List<String> checkList = Arrays.asList("mike12");
        Assert.assertEquals(1, queryResult.documentIds().size());
        Assert.assertTrue(queryResult.documentIds().containsAll(checkList));
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
        Assert.assertNotNull(queryResult);
        Assert.assertEquals(1, queryResult.size());
        List<String> checkList = Arrays.asList("mike12");
        Assert.assertEquals(1, queryResult.documentIds().size());
        Assert.assertTrue(queryResult.documentIds().containsAll(checkList));
    }

    @Test
    public void noResultsWhenQueryOverOneFieldIsMismatched() {
        // query - { "name" : "bill" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "bill");
        QueryResult queryResult = im.find(query);
        Assert.assertNotNull(queryResult);
        Assert.assertEquals(0, queryResult.size());
    }

    @Test
    public void noResultsWhenQueryOverTwoFieldsOneIsMismatched() {
        // query - { "name" : "bill", "age" : 12 }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "bill");
        query.put("age", 12);
        QueryResult queryResult = im.find(query);
        Assert.assertNotNull(queryResult);
        Assert.assertEquals(0, queryResult.size());
    }

    @Test
    public void noResultsWhenQueryOverTwoFieldsBothAreMismatched() {
        // query - { "name" : "bill", "age" : 17 }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "bill");
        query.put("age", 17);
        QueryResult queryResult = im.find(query);
        Assert.assertNotNull(queryResult);
        Assert.assertEquals(0, queryResult.size());
    }

    @Test
    public void validateIteratorContentWithDocumentIdsList() {
        // query - { "name" : "mike" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        QueryResult queryResult = im.find(query);
        Assert.assertNotNull(queryResult);
        Assert.assertTrue(queryResult.size() == queryResult.documentIds().size());
        List<String> docCheckList = new ArrayList<String>();
        for (DocumentRevision rev: queryResult) {
            Assert.assertNotNull(rev.getId());
            Assert.assertNotNull(rev.getBody());
            docCheckList.add(rev.getId());
        }
        Assert.assertTrue(queryResult.size() == docCheckList.size());
        Assert.assertTrue(docCheckList.containsAll(queryResult.documentIds()));
    }

}