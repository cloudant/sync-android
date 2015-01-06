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

import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class QuerySqlTranslatorTest extends AbstractIndexTestBase {

    Map<String, Object> indexes;
    Boolean[] indexesCoverQuery;

    @Override
    public void setUp() throws SQLException {
        super.setUp();
        ArrayList<Object> fieldNames = new ArrayList<Object>();
        fieldNames.add("name");
        fieldNames.add("age");
        fieldNames.add("pet");
        Assert.assertEquals("basic", im.ensureIndexed(fieldNames, "basic"));
        indexes = im.listIndexes();
        Assert.assertNotNull(indexes);
        indexesCoverQuery = new Boolean[]{ false };
    }

    @Test
    public void copesWithSingleFieldQuery() {
        // query - { "name" : "mike" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        query = QueryValidator.normaliseAndValidateQuery(query);
        QueryNode node = QuerySqlTranslator.translateQuery(query, indexes, indexesCoverQuery);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof AndQueryNode);
        Assert.assertTrue(indexesCoverQuery[0]);
        AndQueryNode andNode = (AndQueryNode) node;
        Assert.assertEquals(1, andNode.children.size());
        Assert.assertTrue(andNode.children.get(0) instanceof SqlQueryNode);
        SqlQueryNode sqlNode = (SqlQueryNode) andNode.children.get(0);
        Assert.assertTrue(sqlNode.sql.equals("SELECT _id FROM _t_cloudant_sync_query_index_basic " +
                                             "WHERE \"name\" = \"mike\""));
    }

    @Test
    public void copesWithSingleLevelANDedQuery() {
        // query - { "name" : "mike", "pet" : "cat" }
        Map<String, Object> query = new LinkedHashMap<String, Object>();
        query.put("name", "mike");
        query.put("pet", "cat");
        query = QueryValidator.normaliseAndValidateQuery(query);
        QueryNode node = QuerySqlTranslator.translateQuery(query, indexes, indexesCoverQuery);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof AndQueryNode);
        Assert.assertTrue(indexesCoverQuery[0]);
        AndQueryNode andNode = (AndQueryNode) node;
        Assert.assertEquals(1, andNode.children.size());
        Assert.assertTrue(andNode.children.get(0) instanceof SqlQueryNode);
        SqlQueryNode sqlNode = (SqlQueryNode) andNode.children.get(0);
        Assert.assertTrue(sqlNode.sql.equals("SELECT _id FROM _t_cloudant_sync_query_index_basic " +
                                             "WHERE \"name\" = \"mike\" AND \"pet\" = \"cat\""));
    }

    @Test
    public void copesWithLongHandSingleLevelANDedQuery() {
        // query - { "and" : [ { "name" : "mike" }, { "pet" : "cat" } ] }
        Map<String, Object> nameMap = new HashMap<String, Object>();
        nameMap.put("name", "mike");
        Map<String, Object> petMap = new HashMap<String, Object>();
        petMap.put("pet", "cat");
        List<Object> fields = new ArrayList<Object>();
        fields.add(nameMap);
        fields.add(petMap);
        Map<String, Object> query = new LinkedHashMap<String, Object>();
        query.put("$and", fields);
        query = QueryValidator.normaliseAndValidateQuery(query);
        QueryNode node = QuerySqlTranslator.translateQuery(query, indexes, indexesCoverQuery);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof AndQueryNode);
        Assert.assertTrue(indexesCoverQuery[0]);
        AndQueryNode andNode = (AndQueryNode) node;
        Assert.assertEquals(1, andNode.children.size());
        Assert.assertTrue(andNode.children.get(0) instanceof SqlQueryNode);
        SqlQueryNode sqlNode = (SqlQueryNode) andNode.children.get(0);
        Assert.assertTrue(sqlNode.sql.equals("SELECT _id FROM _t_cloudant_sync_query_index_basic " +
                                             "WHERE \"name\" = \"mike\" AND \"pet\" = \"cat\""));
    }

}