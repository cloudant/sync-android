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
import java.util.Arrays;
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
        List<Object> fieldNames = Arrays.<Object>asList("name", "age", "pet");
        Assert.assertEquals("basic", im.ensureIndexed(fieldNames, "basic"));
        indexes = im.listIndexes();
        Assert.assertNotNull(indexes);
        indexesCoverQuery = new Boolean[]{ false };
    }

    // When creating a tree

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
        String sql = sqlNode.sql.sqlWithPlaceHolders;
        String[] valuesArray = sqlNode.sql.placeHolderValues;
        Assert.assertTrue(sql.equals("SELECT _id FROM _t_cloudant_sync_query_index_basic " +
                                     "WHERE \"name\" = ?"));
        List<String> placeHolderValues = Arrays.asList("mike");
        Assert.assertTrue(placeHolderValues.containsAll(Arrays.asList(valuesArray)));
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
        String sql = sqlNode.sql.sqlWithPlaceHolders;
        String[] valuesArray = sqlNode.sql.placeHolderValues;
        Assert.assertTrue(sql.equals("SELECT _id FROM _t_cloudant_sync_query_index_basic " +
                                     "WHERE \"name\" = ? AND \"pet\" = ?"));
        List<String> placeHolderValues = Arrays.asList("mike", "cat");
        Assert.assertTrue(placeHolderValues.containsAll(Arrays.asList(valuesArray)));
    }

    @Test
    public void copesWithLongHandSingleLevelANDedQuery() {
        // query - { "and" : [ { "name" : "mike" }, { "pet" : "cat" } ] }
        Map<String, Object> nameMap = new HashMap<String, Object>();
        nameMap.put("name", "mike");
        Map<String, Object> petMap = new HashMap<String, Object>();
        petMap.put("pet", "cat");
        List<Object> fields = Arrays.<Object>asList(nameMap, petMap);
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
        String sql = sqlNode.sql.sqlWithPlaceHolders;
        String[] valuesArray = sqlNode.sql.placeHolderValues;
        Assert.assertTrue(sql.equals("SELECT _id FROM _t_cloudant_sync_query_index_basic " +
                                     "WHERE \"name\" = ? AND \"pet\" = ?"));
        List<String> placeHolderValues = Arrays.asList("mike", "cat");
        Assert.assertTrue(placeHolderValues.containsAll(Arrays.asList(valuesArray)));
    }

    // When selecting an index to use

    @Test
    public void indexSelectionFailsWhenNoIndexes() {
        Map<String, Object> eq = new HashMap<String, Object>();
        eq.put("$eq", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", eq);
        List<Object> clause = Arrays.<Object>asList(name);
        Assert.assertNull(QuerySqlTranslator.chooseIndexForAndClause(clause, null));
    }

    @Test
    public void indexSelectionFailsWhenNoQueryKeys() {
        Map<String, Object> indexes = new HashMap<String, Object>();
        List<Object> fields = Arrays.<Object>asList("name", "age", "pet");
        indexes.put("named", fields);
        Assert.assertNull(QuerySqlTranslator.chooseIndexForAndClause(null, indexes));
        Assert.assertNull(QuerySqlTranslator.chooseIndexForAndClause(new ArrayList<Object>(),
                                                                     indexes));
    }

    @Test
    public void selectsIndexForSingleFieldQuery() {
        Map<String, Object> indexes = new HashMap<String, Object>();
        Map<String, Object> index = new HashMap<String, Object>();
        index.put("name", "named");
        index.put("type", "json");
        List<Object> fields = Arrays.<Object>asList("name");
        index.put("fields", fields);
        indexes.put("named", index);

        Map<String, Object> eq = new HashMap<String, Object>();
        eq.put("$eq", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", eq);
        List<Object> clause = Arrays.<Object>asList(name);

        String idx = QuerySqlTranslator.chooseIndexForAndClause(clause, indexes);
        Assert.assertTrue(idx.equals("named"));
    }

    @Test
    public void selectsIndexForMultiFieldQuery() {
        Map<String, Object> indexes = new HashMap<String, Object>();
        Map<String, Object> index = new HashMap<String, Object>();
        index.put("name", "named");
        index.put("type", "json");
        List<Object> fields = Arrays.<Object>asList("name", "age", "pet");
        index.put("fields", fields);
        indexes.put("named", index);

        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", "mike");
        Map<String, Object> pet = new HashMap<String, Object>();
        pet.put("pet", "cat");
        List<Object> clause = Arrays.<Object>asList(name, pet);

        String idx = QuerySqlTranslator.chooseIndexForAndClause(clause, indexes);
        Assert.assertTrue(idx.equals("named"));
    }

    @Test
    public void selectsIndexFromMultipleIndexesForMultiFieldQuery() {
        Map<String, Object> indexes = new HashMap<String, Object>();

        Map<String, Object> named = new HashMap<String, Object>();
        named.put("name", "named");
        named.put("type", "json");
        List<Object> namedFields = Arrays.<Object>asList("name", "age", "pet");
        named.put("fields", namedFields);

        Map<String, Object> bopped = new HashMap<String, Object>();
        bopped.put("name", "bopped");
        bopped.put("type", "json");
        List<Object> boppedFields = Arrays.<Object>asList("house_number", "pet");
        bopped.put("fields", boppedFields);

        Map<String, Object> unsuitable = new HashMap<String, Object>();
        unsuitable.put("name", "unsuitable");
        unsuitable.put("type", "json");
        List<Object> unsuitableFields = Arrays.<Object>asList("name");
        unsuitable.put("fields", unsuitableFields);

        indexes.put("named", named);
        indexes.put("bopped", bopped);
        indexes.put("unsuitable", unsuitable);

        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", "mike");
        Map<String, Object> pet = new HashMap<String, Object>();
        pet.put("pet", "cat");
        List<Object> clause = Arrays.<Object>asList(name, pet);

        String idx = QuerySqlTranslator.chooseIndexForAndClause(clause, indexes);
        Assert.assertTrue(idx.equals("named"));
    }

    @Test
    public void selectsCorrectIndexWhenSeveralMatch() {
        Map<String, Object> indexes = new HashMap<String, Object>();

        Map<String, Object> named = new HashMap<String, Object>();
        named.put("name", "named");
        named.put("type", "json");
        List<Object> namedFields = Arrays.<Object>asList("name", "age", "pet");
        named.put("fields", namedFields);

        Map<String, Object> bopped = new HashMap<String, Object>();
        bopped.put("name", "bopped");
        bopped.put("type", "json");
        List<Object> boppedFields = Arrays.<Object>asList("name", "age", "pet");
        bopped.put("fields", boppedFields);

        Map<String, Object> manyField = new HashMap<String, Object>();
        manyField.put("name", "manyField");
        manyField.put("type", "json");
        List<Object> manyFieldFields = Arrays.<Object>asList("name", "age", "pet");
        manyField.put("fields", manyFieldFields);

        Map<String, Object> unsuitable = new HashMap<String, Object>();
        unsuitable.put("name", "unsuitable");
        unsuitable.put("type", "json");
        List<Object> unsuitableFields = Arrays.<Object>asList("name");
        unsuitable.put("fields", unsuitableFields);

        indexes.put("named", named);
        indexes.put("bopped", bopped);
        indexes.put("manyField", manyField);
        indexes.put("unsuitable", unsuitable);

        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", "mike");
        Map<String, Object> pet = new HashMap<String, Object>();
        pet.put("pet", "cat");
        List<Object> clause = Arrays.<Object>asList(name, pet);

        String idx = QuerySqlTranslator.chooseIndexForAndClause(clause, indexes);
        Assert.assertTrue(idx.equals("named") || idx.equals("bopped"));
    }

    @Test
    public void nullWhenNoSuitableIndexAvailable() {
        Map<String, Object> indexes = new HashMap<String, Object>();

        Map<String, Object> named = new HashMap<String, Object>();
        named.put("name", "named");
        named.put("type", "json");
        List<Object> namedFields = Arrays.<Object>asList("name", "age");
        named.put("fields", namedFields);

        Map<String, Object> unsuitable = new HashMap<String, Object>();
        unsuitable.put("name", "unsuitable");
        unsuitable.put("type", "json");
        List<Object> unsuitableFields = Arrays.<Object>asList("name");
        unsuitable.put("fields", unsuitableFields);

        indexes.put("named", named);
        indexes.put("unsuitable", unsuitable);

        Map<String, Object> pet = new HashMap<String, Object>();
        pet.put("pet", "cat");
        List<Object> clause = Arrays.<Object>asList(pet);

        Assert.assertNull(QuerySqlTranslator.chooseIndexForAndClause(clause, indexes));
    }

    // When generating query WHERE clauses

    @Test
    public void nullWhereClauseWhenQueryEmpty() {
        Assert.assertNull(QuerySqlTranslator.whereSqlForAndClause(null));
        Assert.assertNull(QuerySqlTranslator.whereSqlForAndClause(new ArrayList<Object>()));
    }

    @Test
    public void validWhereClauseForSingleTermEQ() {
        Map<String, Object> eq = new HashMap<String, Object>();
        eq.put("$eq", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", eq);
        List<Object> clause = Arrays.<Object>asList(name);
        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(clause);
        Assert.assertNotNull(where);
        Assert.assertTrue(where.sqlWithPlaceHolders.equals("\"name\" = ?"));
        List<String> placeHolderValues = Arrays.asList("mike");
        Assert.assertTrue(placeHolderValues.containsAll(Arrays.asList(where.placeHolderValues)));
    }

    @Test
    public void validWhereClauseForMultiTermEQ() {
        Map<String, Object> nameEq = new HashMap<String, Object>();
        nameEq.put("$eq", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", nameEq);

        Map<String, Object> ageEq = new HashMap<String, Object>();
        ageEq.put("$eq", 12);
        Map<String, Object> age = new HashMap<String, Object>();
        age.put("age", ageEq);

        Map<String, Object> petEq = new HashMap<String, Object>();
        petEq.put("$eq", "cat");
        Map<String, Object> pet = new HashMap<String, Object>();
        pet.put("pet", petEq);

        List<Object> clause = Arrays.<Object>asList(name, age, pet);
        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(clause);
        Assert.assertNotNull(where);
        String expected = "\"name\" = ? AND \"age\" = ? AND \"pet\" = ?";
        Assert.assertTrue(where.sqlWithPlaceHolders.equals(expected));
        List<String> placeHolderValues = Arrays.asList("mike", "12", "cat");
        Assert.assertTrue(placeHolderValues.containsAll(Arrays.asList(where.placeHolderValues)));
    }

    @Test
    public void nullWhereClauseForUnsupportedOperator() {
        Map<String, Object> eq = new HashMap<String, Object>();
        eq.put("$blah", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", eq);
        List<Object> clause = Arrays.<Object>asList(name);
        Assert.assertNull(QuerySqlTranslator.whereSqlForAndClause(clause));
    }

    // When generating query SELECT clauses

    @Test
    public void nullSelectClauseWhenQueryEmpty() {
        Assert.assertNull(QuerySqlTranslator.selectStatementForAndClause(null, "named"));
        Assert.assertNull(QuerySqlTranslator.selectStatementForAndClause(new ArrayList<Object>(),
                                                                         "named"));
    }

    @Test
    public void nullSelectClauseWhenIndexEmpty() {
        Map<String, Object> eq = new HashMap<String, Object>();
        eq.put("$eq", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", eq);
        List<Object> clause = Arrays.<Object>asList(name);
        Assert.assertNull(QuerySqlTranslator.selectStatementForAndClause(clause, null));
        Assert.assertNull(QuerySqlTranslator.selectStatementForAndClause(clause, ""));
    }

    @Test
    public void validSelectClauseForSingleTermEQ() {
        Map<String, Object> eq = new HashMap<String, Object>();
        eq.put("$eq", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", eq);
        List<Object> clause = Arrays.<Object>asList(name);
        SqlParts sql = QuerySqlTranslator.selectStatementForAndClause(clause, "anIndex");
        Assert.assertNotNull(sql);
        String expected = "SELECT _id FROM _t_cloudant_sync_query_index_anIndex WHERE \"name\" = ?";
        Assert.assertTrue(sql.sqlWithPlaceHolders.equals(expected));
        List<String> placeHolderValues = Arrays.asList("mike");
        Assert.assertTrue(placeHolderValues.containsAll(Arrays.asList(sql.placeHolderValues)));
    }

    @Test
    public void validSelectClauseForMultiTermEQ() {
        Map<String, Object> nameEq = new HashMap<String, Object>();
        nameEq.put("$eq", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", nameEq);

        Map<String, Object> ageEq = new HashMap<String, Object>();
        ageEq.put("$eq", 12);
        Map<String, Object> age = new HashMap<String, Object>();
        age.put("age", ageEq);

        Map<String, Object> petEq = new HashMap<String, Object>();
        petEq.put("$eq", "cat");
        Map<String, Object> pet = new HashMap<String, Object>();
        pet.put("pet", petEq);

        List<Object> clause = Arrays.<Object>asList(name, age, pet);
        SqlParts sql = QuerySqlTranslator.selectStatementForAndClause(clause, "anIndex");
        Assert.assertNotNull(sql);
        String expected = "SELECT _id FROM _t_cloudant_sync_query_index_anIndex WHERE \"name\" = " +
                          "? AND \"age\" = ? AND \"pet\" = ?";
        Assert.assertTrue(sql.sqlWithPlaceHolders.equals(expected));
        List<String> placeHolderValues = Arrays.asList("mike", "12", "cat");
        Assert.assertTrue(placeHolderValues.containsAll(Arrays.asList(sql.placeHolderValues)));
    }

}