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
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

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
        String indexName = im.ensureIndexed(Arrays.<Object>asList("name", "age", "pet"), "basic");
        assertThat(indexName, is("basic"));
        indexes = im.listIndexes();
        assertThat(indexes, is(notNullValue()));
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
        assertThat(node, is(instanceOf(AndQueryNode.class)));
        AndQueryNode andNode = (AndQueryNode) node;
        assertThat(indexesCoverQuery[0], is(true));
        assertThat(andNode.children.size(), is(1));
        assertThat(andNode.children.get(0), is(instanceOf(SqlQueryNode.class)));
        SqlQueryNode sqlNode = (SqlQueryNode) andNode.children.get(0);
        String sql = sqlNode.sql.sqlWithPlaceHolders;
        String[] placeHolderValues = sqlNode.sql.placeHolderValues;
        String select = "SELECT _id FROM _t_cloudant_sync_query_index_basic";
        String where = " WHERE \"name\" = ?";
        assertThat(sql, is(String.format("%s%s", select, where)));
        assertThat(placeHolderValues, is(arrayContaining("mike")));
    }

    @Test
    public void copesWithSingleLevelANDedQuery() {
        // query - { "name" : "mike", "pet" : "cat" }
        Map<String, Object> query = new LinkedHashMap<String, Object>();
        query.put("name", "mike");
        query.put("pet", "cat");
        query = QueryValidator.normaliseAndValidateQuery(query);
        QueryNode node = QuerySqlTranslator.translateQuery(query, indexes, indexesCoverQuery);
        assertThat(node, is(instanceOf(AndQueryNode.class)));
        AndQueryNode andNode = (AndQueryNode) node;
        assertThat(indexesCoverQuery[0], is(true));
        assertThat(andNode.children.size(), is(1));
        assertThat(andNode.children.get(0), is(instanceOf(SqlQueryNode.class)));
        SqlQueryNode sqlNode = (SqlQueryNode) andNode.children.get(0);
        String sql = sqlNode.sql.sqlWithPlaceHolders;
        String[] placeHolderValues = sqlNode.sql.placeHolderValues;
        String select = "SELECT _id FROM _t_cloudant_sync_query_index_basic";
        String where = " WHERE \"name\" = ? AND \"pet\" = ?";
        assertThat(sql, is(String.format("%s%s", select, where)));
        assertThat(placeHolderValues, is(arrayContainingInAnyOrder("mike", "cat")));
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
        assertThat(node, is(instanceOf(AndQueryNode.class)));
        AndQueryNode andNode = (AndQueryNode) node;
        assertThat(indexesCoverQuery[0], is(true));
        assertThat(andNode.children.size(), is(1));
        assertThat(andNode.children.get(0), is(instanceOf(SqlQueryNode.class)));
        SqlQueryNode sqlNode = (SqlQueryNode) andNode.children.get(0);
        String sql = sqlNode.sql.sqlWithPlaceHolders;
        String[] placeHolderValues = sqlNode.sql.placeHolderValues;
        String select = "SELECT _id FROM _t_cloudant_sync_query_index_basic";
        String where = " WHERE \"name\" = ? AND \"pet\" = ?";
        assertThat(sql, is(String.format("%s%s", select, where)));
        assertThat(placeHolderValues, is(arrayContainingInAnyOrder("mike", "cat")));
    }

    // When selecting an index to use

    @Test
    public void indexSelectionFailsWhenNoIndexes() {
        Map<String, Object> eq = new HashMap<String, Object>();
        eq.put("$eq", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", eq);
        assertThat(QuerySqlTranslator.chooseIndexForAndClause(Arrays.<Object>asList(name), null),
                   is(nullValue()));
    }

    @Test
    public void indexSelectionFailsWhenNoQueryKeys() {
        Map<String, Object> indexes = new HashMap<String, Object>();
        indexes.put("named", Arrays.<Object>asList("name", "age", "pet"));
        assertThat(QuerySqlTranslator.chooseIndexForAndClause(null, indexes), is(nullValue()));
        assertThat(QuerySqlTranslator.chooseIndexForAndClause(new ArrayList<Object>(), null),
                   is(nullValue()));
    }

    @Test
    public void selectsIndexForSingleFieldQuery() {
        Map<String, Object> indexes = new HashMap<String, Object>();
        Map<String, Object> index = new HashMap<String, Object>();
        index.put("name", "named");
        index.put("type", "json");
        index.put("fields", Arrays.<Object>asList("name"));
        indexes.put("named", index);

        Map<String, Object> eq = new HashMap<String, Object>();
        eq.put("$eq", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", eq);

        String idx = QuerySqlTranslator.chooseIndexForAndClause(Arrays.<Object>asList(name),
                                                                indexes);
        assertThat(idx, is("named"));
    }

    @Test
    public void selectsIndexForMultiFieldQuery() {
        Map<String, Object> indexes = new HashMap<String, Object>();
        Map<String, Object> index = new HashMap<String, Object>();
        index.put("name", "named");
        index.put("type", "json");
        index.put("fields", Arrays.<Object>asList("name", "age", "pet"));
        indexes.put("named", index);

        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", "mike");
        Map<String, Object> pet = new HashMap<String, Object>();
        pet.put("pet", "cat");

        String idx = QuerySqlTranslator.chooseIndexForAndClause(Arrays.<Object>asList(name, pet),
                                                                indexes);
        assertThat(idx, is("named"));
    }

    @Test
    public void selectsIndexFromMultipleIndexesForMultiFieldQuery() {
        Map<String, Object> indexes = new HashMap<String, Object>();

        Map<String, Object> named = new HashMap<String, Object>();
        named.put("name", "named");
        named.put("type", "json");
        named.put("fields", Arrays.<Object>asList("name", "age", "pet"));

        Map<String, Object> bopped = new HashMap<String, Object>();
        bopped.put("name", "bopped");
        bopped.put("type", "json");
        bopped.put("fields", Arrays.<Object>asList("house_number", "pet"));

        Map<String, Object> unsuitable = new HashMap<String, Object>();
        unsuitable.put("name", "unsuitable");
        unsuitable.put("type", "json");
        unsuitable.put("fields", Arrays.<Object>asList("name"));

        indexes.put("named", named);
        indexes.put("bopped", bopped);
        indexes.put("unsuitable", unsuitable);

        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", "mike");
        Map<String, Object> pet = new HashMap<String, Object>();
        pet.put("pet", "cat");

        String idx = QuerySqlTranslator.chooseIndexForAndClause(Arrays.<Object>asList(name, pet),
                                                                indexes);
        assertThat(idx, is("named"));
    }

    @Test
    public void selectsCorrectIndexWhenSeveralMatch() {
        Map<String, Object> indexes = new HashMap<String, Object>();

        Map<String, Object> named = new HashMap<String, Object>();
        named.put("name", "named");
        named.put("type", "json");
        named.put("fields", Arrays.<Object>asList("name", "age", "pet"));

        Map<String, Object> bopped = new HashMap<String, Object>();
        bopped.put("name", "bopped");
        bopped.put("type", "json");
        bopped.put("fields", Arrays.<Object>asList("name", "age", "pet"));

        Map<String, Object> manyField = new HashMap<String, Object>();
        manyField.put("name", "manyField");
        manyField.put("type", "json");
        manyField.put("fields", Arrays.<Object>asList("name", "age", "pet"));

        Map<String, Object> unsuitable = new HashMap<String, Object>();
        unsuitable.put("name", "unsuitable");
        unsuitable.put("type", "json");
        unsuitable.put("fields", Arrays.<Object>asList("name"));

        indexes.put("named", named);
        indexes.put("bopped", bopped);
        indexes.put("manyField", manyField);
        indexes.put("unsuitable", unsuitable);

        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", "mike");
        Map<String, Object> pet = new HashMap<String, Object>();
        pet.put("pet", "cat");

        String idx = QuerySqlTranslator.chooseIndexForAndClause(Arrays.<Object>asList(name, pet),
                                                                indexes);
        assertThat(idx, either(is("named")).or(is("bopped")));
    }

    @Test
    public void nullWhenNoSuitableIndexAvailable() {
        Map<String, Object> indexes = new HashMap<String, Object>();

        Map<String, Object> named = new HashMap<String, Object>();
        named.put("name", "named");
        named.put("type", "json");
        named.put("fields", Arrays.<Object>asList("name", "age"));

        Map<String, Object> unsuitable = new HashMap<String, Object>();
        unsuitable.put("name", "unsuitable");
        unsuitable.put("type", "json");
        unsuitable.put("fields", Arrays.<Object>asList("name"));

        indexes.put("named", named);
        indexes.put("unsuitable", unsuitable);

        Map<String, Object> pet = new HashMap<String, Object>();
        pet.put("pet", "cat");

        assertThat(QuerySqlTranslator.chooseIndexForAndClause(Arrays.<Object>asList(pet), indexes),
                   is(nullValue()));
    }

    // When generating query WHERE clauses

    @Test
    public void nullWhereClauseWhenQueryEmpty() {
        assertThat(QuerySqlTranslator.whereSqlForAndClause(null), is(nullValue()));
        assertThat(QuerySqlTranslator.whereSqlForAndClause(new ArrayList<Object>()),
                   is(nullValue()));
    }

    @Test
    public void validWhereClauseForSingleTermEQ() {
        Map<String, Object> eq = new HashMap<String, Object>();
        eq.put("$eq", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", eq);
        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name));
        assertThat(where.sqlWithPlaceHolders, is("\"name\" = ?"));
        assertThat(where.placeHolderValues, is(arrayContaining("mike")));
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

        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name,
                                                                                       age,
                                                                                       pet));
        String expected = "\"name\" = ? AND \"age\" = ? AND \"pet\" = ?";
        assertThat(where.sqlWithPlaceHolders, is(expected));
        assertThat(where.placeHolderValues, is(arrayContainingInAnyOrder("mike", "12", "cat")));
    }

    @Test
    public void nullWhereClauseForUnsupportedOperator() {
        Map<String, Object> eq = new HashMap<String, Object>();
        eq.put("$blah", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", eq);
        assertThat(QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name)),
                   is(nullValue()));
    }

    // When generating query SELECT clauses

    @Test
    public void nullSelectClauseWhenQueryEmpty() {
        assertThat(QuerySqlTranslator.selectStatementForAndClause(null, "named"), is(nullValue()));
        assertThat(QuerySqlTranslator.selectStatementForAndClause(new ArrayList<Object>(), "named"),
                   is(nullValue()));
    }

    @Test
    public void nullSelectClauseWhenIndexEmpty() {
        Map<String, Object> eq = new HashMap<String, Object>();
        eq.put("$eq", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", eq);
        List<Object> clause = Arrays.<Object>asList(name);
        assertThat(QuerySqlTranslator.selectStatementForAndClause(clause, null), is(nullValue()));
        assertThat(QuerySqlTranslator.selectStatementForAndClause(clause, ""), is(nullValue()));
    }

    @Test
    public void validSelectClauseForSingleTermEQ() {
        Map<String, Object> eq = new HashMap<String, Object>();
        eq.put("$eq", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", eq);
        List<Object> clause = Arrays.<Object>asList(name);
        SqlParts sql = QuerySqlTranslator.selectStatementForAndClause(clause, "anIndex");
        String expected = "SELECT _id FROM _t_cloudant_sync_query_index_anIndex WHERE \"name\" = ?";
        assertThat(sql.sqlWithPlaceHolders, is(expected));
        assertThat(sql.placeHolderValues, is(arrayContaining("mike")));
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
        String select = "SELECT _id FROM _t_cloudant_sync_query_index_anIndex";
        String where = " WHERE \"name\" = ? AND \"age\" = ? AND \"pet\" = ?";
        String expected = String.format("%s%s", select, where);
        assertThat(sql.sqlWithPlaceHolders, is(expected));
        assertThat(sql.placeHolderValues, is(arrayContainingInAnyOrder("mike", "12", "cat")));
    }

}