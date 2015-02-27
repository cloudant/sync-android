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
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class QuerySqlTranslatorTest extends AbstractIndexTestBase {

    Map<String, Object> indexes;
    Boolean[] indexesCoverQuery;
    String indexName;
    String indexTable;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexName = im.ensureIndexed(Arrays.<Object>asList("name", "age", "pet"), "basic");
        assertThat(indexName, is("basic"));
        indexes = im.listIndexes();
        assertThat(indexes, is(notNullValue()));
        indexesCoverQuery = new Boolean[]{ false };
        indexTable = String.format("_t_cloudant_sync_query_index_%s", indexName);
    }

    // When creating a tree

    @Test
    public void copesWhenNoMatchingIndex() {
        // query - { "firstname" : "mike" }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("firstname", "mike");
        query = QueryValidator.normaliseAndValidateQuery(query);
        QueryNode node = QuerySqlTranslator.translateQuery(query, indexes, indexesCoverQuery);
        assertThat(node, is(instanceOf(AndQueryNode.class)));
        AndQueryNode andNode = (AndQueryNode) node;
        assertThat(andNode, is(notNullValue()));
        assertThat(indexesCoverQuery[0], is(false));
        assertThat(andNode.children.size(), is(1));
        assertThat(andNode.children.get(0), is(instanceOf(SqlQueryNode.class)));
        SqlQueryNode sqlNode = (SqlQueryNode) andNode.children.get(0);
        String sql = sqlNode.sql.sqlWithPlaceHolders;
        String[] placeHolderValues = sqlNode.sql.placeHolderValues;
        String select = "SELECT _id FROM _t_cloudant_sync_query_index_basic";
        assertThat(sql, is(select));
        assertThat(placeHolderValues, is(emptyArray()));
    }

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
        // query - { "$and" : [ { "name" : "mike" }, { "pet" : "cat" } ] }
        Map<String, Object> nameMap = new HashMap<String, Object>();
        nameMap.put("name", "mike");
        Map<String, Object> petMap = new HashMap<String, Object>();
        petMap.put("pet", "cat");
        Map<String, Object> query = new LinkedHashMap<String, Object>();
        query.put("$and", Arrays.<Object>asList(nameMap, petMap));
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
    public void copesWithLongHandTwoLevelANDedQuery() {
        // query - { "$and" : [ { "name" : "mike" },
        //                      { "pet" : "cat" },
        //                      { "$and" : [ { "name" : "mike" }, { "pet" : "cat" } ] } ] }
        Map<String, Object> nameMap = new HashMap<String, Object>();
        nameMap.put("name", "mike");
        Map<String, Object> petMap = new HashMap<String, Object>();
        petMap.put("pet", "cat");
        Map<String, Object> lvl2 = new HashMap<String, Object>();
        lvl2.put("$and", Arrays.<Object>asList(nameMap, petMap));
        Map<String, Object> query = new LinkedHashMap<String, Object>();
        query.put("$and", Arrays.<Object>asList(nameMap, petMap, lvl2));
        query = QueryValidator.normaliseAndValidateQuery(query);

        QueryNode node = QuerySqlTranslator.translateQuery(query, indexes, indexesCoverQuery);
        assertThat(node, is(instanceOf(AndQueryNode.class)));
        assertThat(indexesCoverQuery[0], is(true));

        //        AND
        //       /  \
        //     sql  AND
        //           \
        //           sql

        AndQueryNode andNode = (AndQueryNode) node;
        assertThat(andNode.children.size(), is(2));

        String select = "SELECT _id FROM _t_cloudant_sync_query_index_basic";
        String where = " WHERE \"name\" = ? AND \"pet\" = ?";
        String sql = String.format("%s%s", select, where);

        // As the embedded AND is the same as the top-level AND, both
        // children should have the same embedded SQL.

        SqlQueryNode sqlNode = (SqlQueryNode) andNode.children.get(0);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sql));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContainingInAnyOrder("mike", "cat")));

        sqlNode = (SqlQueryNode) ((AndQueryNode) andNode.children.get(1)).children.get(0);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sql));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContainingInAnyOrder("mike", "cat")));
    }

    @Test
    public void ordersANDNodesLastInTrees() {
        // query - { "$and" : [ { "$and" : [ { "name" : "mike" }, { "pet" : "cat" } ] },
        //                      { "name" : "mike" },
        //                      { "pet" : "cat" } ] }
        Map<String, Object> nameMap = new HashMap<String, Object>();
        nameMap.put("name", "mike");
        Map<String, Object> petMap = new HashMap<String, Object>();
        petMap.put("pet", "cat");
        Map<String, Object> lvl2 = new HashMap<String, Object>();
        lvl2.put("$and", Arrays.<Object>asList(nameMap, petMap));
        Map<String, Object> query = new LinkedHashMap<String, Object>();
        query.put("$and", Arrays.<Object>asList(lvl2, nameMap, petMap));
        query = QueryValidator.normaliseAndValidateQuery(query);

        QueryNode node = QuerySqlTranslator.translateQuery(query, indexes, indexesCoverQuery);
        assertThat(node, is(instanceOf(AndQueryNode.class)));
        assertThat(indexesCoverQuery[0], is(true));

        //        AND
        //       /  \
        //     sql  AND
        //           \
        //           sql

        AndQueryNode andNode = (AndQueryNode) node;
        assertThat(andNode.children.size(), is(2));

        String select = "SELECT _id FROM _t_cloudant_sync_query_index_basic";
        String where = " WHERE \"name\" = ? AND \"pet\" = ?";
        String sql = String.format("%s%s", select, where);

        // As the embedded AND is the same as the top-level AND, both
        // children should have the same embedded SQL.

        SqlQueryNode sqlNode = (SqlQueryNode) andNode.children.get(0);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sql));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContainingInAnyOrder("mike", "cat")));

        sqlNode = (SqlQueryNode) ((AndQueryNode) andNode.children.get(1)).children.get(0);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sql));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContainingInAnyOrder("mike", "cat")));
    }

    @Test
    public void supportsOR() {
        // query - { "$or" : [ { "name" : "mike" }, { "pet" : "cat" } ] }
        Map<String, Object> nameMap = new HashMap<String, Object>();
        nameMap.put("name", "mike");
        Map<String, Object> petMap = new HashMap<String, Object>();
        petMap.put("pet", "cat");
        Map<String, Object> query = new LinkedHashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(nameMap, petMap));
        query = QueryValidator.normaliseAndValidateQuery(query);

        QueryNode node = QuerySqlTranslator.translateQuery(query, indexes, indexesCoverQuery);
        assertThat(node, is(instanceOf(OrQueryNode.class)));
        assertThat(indexesCoverQuery[0], is(true));

        //        _OR_
        //       /    \
        //     sql    sql

        OrQueryNode orNode = (OrQueryNode) node;
        assertThat(orNode.children.size(), is(2));

        String select = "SELECT _id FROM _t_cloudant_sync_query_index_basic";
        String whereLeft = " WHERE \"name\" = ?";
        String whereRight = " WHERE \"pet\" = ?";
        String sqlLeft = String.format("%s%s", select, whereLeft);
        String sqlRight = String.format("%s%s", select, whereRight);

        SqlQueryNode sqlNode = (SqlQueryNode) orNode.children.get(0);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sqlLeft));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContaining("mike")));

        sqlNode = (SqlQueryNode) orNode.children.get(1);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sqlRight));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContaining("cat")));
    }

    @Test
    public void supportsORInSubTrees() {
        // query - { "$or" : [ { "name" : "mike" },
        //                      { "pet" : "cat" },
        //                      { "$or" : [ { "name" : "mike" }, { "pet" : "cat" } ] } ] }
        Map<String, Object> nameMap = new HashMap<String, Object>();
        nameMap.put("name", "mike");
        Map<String, Object> petMap = new HashMap<String, Object>();
        petMap.put("pet", "cat");
        Map<String, Object> lvl2 = new HashMap<String, Object>();
        lvl2.put("$or", Arrays.<Object>asList(nameMap, petMap));
        Map<String, Object> query = new LinkedHashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(nameMap, petMap, lvl2));
        query = QueryValidator.normaliseAndValidateQuery(query);

        QueryNode node = QuerySqlTranslator.translateQuery(query, indexes, indexesCoverQuery);
        assertThat(node, is(instanceOf(OrQueryNode.class)));
        assertThat(indexesCoverQuery[0], is(true));

        //        OR______
        //       /   \    \
        //      sql  sql  OR
        //               /  \
        //             sql  sql

        OrQueryNode orNode = (OrQueryNode) node;
        assertThat(orNode.children.size(), is(3));

        String select = "SELECT _id FROM _t_cloudant_sync_query_index_basic";
        String whereLeft = " WHERE \"name\" = ?";
        String whereRight = " WHERE \"pet\" = ?";
        String sqlLeft = String.format("%s%s", select, whereLeft);
        String sqlRight = String.format("%s%s", select, whereRight);

        SqlQueryNode sqlNode = (SqlQueryNode) orNode.children.get(0);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sqlLeft));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContaining("mike")));

        sqlNode = (SqlQueryNode) orNode.children.get(1);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sqlRight));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContaining("cat")));

        QueryNode subOrNode = ((OrQueryNode) node).children.get(2);
        assertThat(subOrNode, is(instanceOf(OrQueryNode.class)));

        sqlNode = (SqlQueryNode) ((OrQueryNode) subOrNode).children.get(0);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sqlLeft));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContaining("mike")));

        sqlNode = (SqlQueryNode) ((OrQueryNode) subOrNode).children.get(1);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sqlRight));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContaining("cat")));

    }

    @Test
    public void supportsANDAndORInSubTreesTwoLevel() {
        // query - { "$or" : [ { "name" : "mike" },
        //                      { "pet" : "cat" },
        //                      { "$or" : [ { "name" : "mike" }, { "pet" : "cat" } ] },
        //                      { "$and" : [ { "name" : "mike" }, { "pet" : "cat" } ] } ] }
        Map<String, Object> nameMap = new HashMap<String, Object>();
        nameMap.put("name", "mike");
        Map<String, Object> petMap = new HashMap<String, Object>();
        petMap.put("pet", "cat");
        Map<String, Object> lvl2or = new HashMap<String, Object>();
        lvl2or.put("$or", Arrays.<Object>asList(nameMap, petMap));
        Map<String, Object> lvl2and = new HashMap<String, Object>();
        lvl2and.put("$and", Arrays.<Object>asList(nameMap, petMap));
        Map<String, Object> query = new LinkedHashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(nameMap, petMap, lvl2or, lvl2and));
        query = QueryValidator.normaliseAndValidateQuery(query);

        QueryNode node = QuerySqlTranslator.translateQuery(query, indexes, indexesCoverQuery);
        assertThat(node, is(instanceOf(OrQueryNode.class)));
        assertThat(indexesCoverQuery[0], is(true));

        //        OR____________
        //       /   \    \     \
        //      sql  sql  OR    AND
        //               /  \     \
        //             sql  sql   sql

        OrQueryNode orNode = (OrQueryNode) node;
        assertThat(orNode.children.size(), is(4));

        String select = "SELECT _id FROM _t_cloudant_sync_query_index_basic";
        String whereLeft = " WHERE \"name\" = ?";
        String whereRight = " WHERE \"pet\" = ?";
        String whereAnd = " WHERE \"name\" = ? AND \"pet\" = ?";
        String sqlLeft = String.format("%s%s", select, whereLeft);
        String sqlRight = String.format("%s%s", select, whereRight);
        String sqlAnd = String.format("%s%s", select, whereAnd);

        SqlQueryNode sqlNode = (SqlQueryNode) orNode.children.get(0);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sqlLeft));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContaining("mike")));

        sqlNode = (SqlQueryNode) orNode.children.get(1);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sqlRight));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContaining("cat")));

        QueryNode subOrNode = ((OrQueryNode) node).children.get(2);
        assertThat(subOrNode, is(instanceOf(OrQueryNode.class)));

        sqlNode = (SqlQueryNode) ((OrQueryNode) subOrNode).children.get(0);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sqlLeft));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContaining("mike")));

        sqlNode = (SqlQueryNode) ((OrQueryNode) subOrNode).children.get(1);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sqlRight));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContaining("cat")));

        QueryNode subAndNode = orNode.children.get(3);
        assertThat(subAndNode, is(instanceOf(AndQueryNode.class)));
        assertThat(((AndQueryNode) subAndNode).children.size(), is(1));

        sqlNode = (SqlQueryNode) ((AndQueryNode) subAndNode).children.get(0);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sqlAnd));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContainingInAnyOrder("mike", "cat")));
    }

    @Test
    public void supportsANDAndORInSubTreesThreeLevel() {
        // query - { "$or" : [ { "name" : "mike" },
        //                      { "pet" : "cat" },
        //                      { "$or" : [ { "name" : "mike" },
        //                                  { "pet" : "cat" },
        //                                  { "$and" : [ { "name" : "mike" }, { "pet" : "cat" } ] }
        //                                ]
        //                      }
        //                   ]
        //         }
        Map<String, Object> nameMap = new HashMap<String, Object>();
        nameMap.put("name", "mike");
        Map<String, Object> petMap = new HashMap<String, Object>();
        petMap.put("pet", "cat");
        Map<String, Object> lvl2or = new HashMap<String, Object>();
        Map<String, Object> lvl3and = new HashMap<String, Object>();
        lvl3and.put("$and", Arrays.<Object>asList(nameMap, petMap));
        lvl2or.put("$or", Arrays.<Object>asList(nameMap, petMap, lvl3and));
        Map<String, Object> query = new LinkedHashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(nameMap, petMap, lvl2or));
        query = QueryValidator.normaliseAndValidateQuery(query);

        QueryNode node = QuerySqlTranslator.translateQuery(query, indexes, indexesCoverQuery);
        assertThat(node, is(instanceOf(OrQueryNode.class)));
        assertThat(indexesCoverQuery[0], is(true));

        //        OR______
        //       /   \    \
        //      sql  sql  OR______
        //               /  \     \
        //             sql  sql   AND
        //                         |
        //                        sql

        OrQueryNode orNode = (OrQueryNode) node;
        assertThat(orNode.children.size(), is(3));

        String select = "SELECT _id FROM _t_cloudant_sync_query_index_basic";
        String whereLeft = " WHERE \"name\" = ?";
        String whereRight = " WHERE \"pet\" = ?";
        String whereAnd = " WHERE \"name\" = ? AND \"pet\" = ?";
        String sqlLeft = String.format("%s%s", select, whereLeft);
        String sqlRight = String.format("%s%s", select, whereRight);
        String sqlAnd = String.format("%s%s", select, whereAnd);

        SqlQueryNode sqlNode = (SqlQueryNode) orNode.children.get(0);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sqlLeft));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContaining("mike")));

        sqlNode = (SqlQueryNode) orNode.children.get(1);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sqlRight));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContaining("cat")));

        QueryNode subOrNode = ((OrQueryNode) node).children.get(2);
        assertThat(subOrNode, is(instanceOf(OrQueryNode.class)));
        assertThat(((OrQueryNode) subOrNode).children.size(), is(3));

        sqlNode = (SqlQueryNode) ((OrQueryNode) subOrNode).children.get(0);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sqlLeft));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContaining("mike")));

        sqlNode = (SqlQueryNode) ((OrQueryNode) subOrNode).children.get(1);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sqlRight));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContaining("cat")));

        QueryNode subAndNode = ((OrQueryNode) subOrNode).children.get(2);
        assertThat(subAndNode, is(instanceOf(AndQueryNode.class)));
        assertThat(((AndQueryNode) subAndNode).children.size(), is(1));

        sqlNode = (SqlQueryNode) ((AndQueryNode) subAndNode).children.get(0);
        assertThat(sqlNode.sql.sqlWithPlaceHolders, is(sqlAnd));
        assertThat(sqlNode.sql.placeHolderValues, is(arrayContainingInAnyOrder("mike", "cat")));
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

        String idx = QuerySqlTranslator.chooseIndexForAndClause(Arrays.<Object>asList(name), indexes);
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
        assertThat(Arrays.asList("named", "bopped").contains(idx), is(true));
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

        String idx = QuerySqlTranslator.chooseIndexForAndClause(Arrays.<Object>asList(pet), indexes);

        assertThat(idx, is(nullValue()));
    }

    // When generating query WHERE clauses

    @Test
    public void nullWhereClauseWhenQueryEmpty() {
        assertThat(QuerySqlTranslator.whereSqlForAndClause(null, indexName), is(nullValue()));
        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(new ArrayList<Object>(),
                                                                 indexName);
                assertThat(where, is(nullValue()));
    }

    @Test
    public void validWhereClauseForSingleTermEQ() {
        Map<String, Object> eq = new HashMap<String, Object>();
        eq.put("$eq", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", eq);
        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name),
                                                                 indexName);
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

        List<Object> clauses = Arrays.<Object>asList(name, age, pet);
        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(clauses, indexName);
        String expected = "\"name\" = ? AND \"age\" = ? AND \"pet\" = ?";
        assertThat(where.sqlWithPlaceHolders, is(expected));
        assertThat(where.placeHolderValues, is(arrayContainingInAnyOrder("mike", "12", "cat")));
    }

    @Test
    public void usesCorrectSQLOperatorWhenUsingGT() {
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gt", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", op);

        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name),
                                                                 indexName);
        String expected = "\"name\" > ?";
        assertThat(where.sqlWithPlaceHolders, is(expected));
        assertThat(where.placeHolderValues, is(arrayContaining("mike")));
    }

    @Test
    public void usesCorrectSQLOperatorWhenUsingGTE() {
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gte", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", op);

        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name),
                                                                 indexName);
        String expected = "\"name\" >= ?";
        assertThat(where.sqlWithPlaceHolders, is(expected));
        assertThat(where.placeHolderValues, is(arrayContaining("mike")));
    }

    @Test
    public void usesCorrectSQLOperatorWhenUsingLT() {
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lt", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", op);

        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name),
                                                                 indexName);
        String expected = "\"name\" < ?";
        assertThat(where.sqlWithPlaceHolders, is(expected));
        assertThat(where.placeHolderValues, is(arrayContaining("mike")));
    }

    @Test
    public void usesCorrectSQLOperatorWhenUsingLTE() {
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lte", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", op);

        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name),
                                                                 indexName);
        String expected = "\"name\" <= ?";
        assertThat(where.sqlWithPlaceHolders, is(expected));
        assertThat(where.placeHolderValues, is(arrayContaining("mike")));
    }

    @Test
    public void usesCorrectSQLOperatorForEXISTSTrue() {
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$exists", true);
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", op);

        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name),
                                                                 indexName);
        String expected = "(\"name\" IS NOT NULL)";
        assertThat(where.sqlWithPlaceHolders, is(expected));
    }

    @Test
    public void usesCorrectSQLOperatorForEXISTSFalse() {
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$exists", false);
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", op);

        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name),
                                                                 indexName);
        String expected = "(\"name\" IS NULL)";
        assertThat(where.sqlWithPlaceHolders, is(expected));
    }

    @Test
    public void usesCorrectSQLOperatorForNOTEXISTSFalse() {
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$exists", false);
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", op);
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", notOp);

        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name),
                                                                 indexName);
        String expected = "(\"name\" IS NOT NULL)";
        assertThat(where.sqlWithPlaceHolders, is(expected));
    }

    @Test
    public void usesCorrectSQLOperatorForNOTEXISTSTrue() {
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$exists", true);
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", op);
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", notOp);

        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name),
                                                                 indexName);
        String expected = "(\"name\" IS NULL)";
        assertThat(where.sqlWithPlaceHolders, is(expected));
    }

    @Test
    public void constructsValidWhereClauseForINUsingSingleElement() {
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$in", Arrays.<Object>asList("mike"));
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", op);

        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name),
                         indexName);
        String expected = "\"name\" = ?";
        assertThat(where.sqlWithPlaceHolders, is(expected));
        assertThat(where.placeHolderValues, is(arrayContaining("mike")));
    }

    @Test
    public void constructsValidWhereClauseForINUsingMultipleElements() {
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$in", Arrays.<Object>asList("mike", "fred"));
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", op);

        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name),
                                                                 indexName);
        String expected = "\"name\" IN ( ?, ? )";
        assertThat(where.sqlWithPlaceHolders, is(expected));
        assertThat(where.placeHolderValues, is(arrayContaining("mike", "fred")));
    }

    // When generating query WHERE clauses with $not

    @Test
    public void validWhereNOTClauseForSingleTermEQ() {
        Map<String, Object> eq = new HashMap<String, Object>();
        eq.put("$eq", "mike");
        Map<String, Object> not = new HashMap<String, Object>();
        not.put("$not", eq);
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", not);
        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name),
                                                                 indexName);
        String expected = String.format("_id NOT IN (SELECT _id FROM %s WHERE \"name\" = ?)",
                                        indexTable);
        assertThat(where.sqlWithPlaceHolders, is(expected));
        assertThat(where.placeHolderValues, is(arrayContaining("mike")));
    }

    @Test
    public void validWhereNOTClauseForMultiTermEQ() {
        Map<String, Object> eq = new HashMap<String, Object>();
        eq.put("$eq", "mike");
        Map<String, Object> not = new HashMap<String, Object>();
        not.put("$not", eq);
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", not);

        Map<String, Object> ageEq = new HashMap<String, Object>();
        ageEq.put("$eq", 12);
        Map<String, Object> age = new HashMap<String, Object>();
        age.put("age", ageEq);

        Map<String, Object> petEq = new HashMap<String, Object>();
        petEq.put("$eq", "cat");
        Map<String, Object> notPet = new HashMap<String, Object>();
        notPet.put("$not", petEq);
        Map<String, Object> pet = new HashMap<String, Object>();
        pet.put("pet", notPet);

        List<Object> clauses = Arrays.<Object>asList(name, age, pet);
        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(clauses, indexName);

        String expectedName = String.format("_id NOT IN (SELECT _id FROM %s WHERE \"name\" = ?)",
                                            indexTable);
        String expectedAge = "\"age\" = ?";
        String expectedPet = String.format("_id NOT IN (SELECT _id FROM %s WHERE \"pet\" = ?)",
                                           indexTable);
        String expected = String.format("%s AND %s AND %s", expectedName, expectedAge, expectedPet);

        assertThat(where.sqlWithPlaceHolders, is(expected));
        assertThat(where.placeHolderValues, is(arrayContainingInAnyOrder("mike", "12", "cat")));
    }

    @Test
    public void usesCorrectSQLOperatorWhenUsingNOTGT() {
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gt", "mike");
        Map<String, Object> not = new HashMap<String, Object>();
        not.put("$not", op);
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", not);

        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name),
                                                                 indexName);
        String expected = String.format("_id NOT IN (SELECT _id FROM %s WHERE \"name\" > ?)",
                                        indexTable);
        assertThat(where.sqlWithPlaceHolders, is(expected));
        assertThat(where.placeHolderValues, is(arrayContaining("mike")));
    }

    @Test
    public void usesCorrectSQLOperatorWhenUsingNOTGTE() {
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gte", "mike");
        Map<String, Object> not = new HashMap<String, Object>();
        not.put("$not", op);
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", not);

        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name),
                                                                 indexName);
        String expected = String.format("_id NOT IN (SELECT _id FROM %s WHERE \"name\" >= ?)",
                                        indexTable);
        assertThat(where.sqlWithPlaceHolders, is(expected));
        assertThat(where.placeHolderValues, is(arrayContaining("mike")));
    }

    @Test
    public void usesCorrectSQLOperatorWhenUsingNOTLT() {
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lt", "mike");
        Map<String, Object> not = new HashMap<String, Object>();
        not.put("$not", op);
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", not);

        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name),
                                                                 indexName);
        String expected = String.format("_id NOT IN (SELECT _id FROM %s WHERE \"name\" < ?)",
                                        indexTable);
        assertThat(where.sqlWithPlaceHolders, is(expected));
        assertThat(where.placeHolderValues, is(arrayContaining("mike")));
    }

    @Test
    public void usesCorrectSQLOperatorWhenUsingNOTLTE() {
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lte", "mike");
        Map<String, Object> not = new HashMap<String, Object>();
        not.put("$not", op);
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", not);

        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name),
                                                                 indexName);
        String expected = String.format("_id NOT IN (SELECT _id FROM %s WHERE \"name\" <= ?)",
                                        indexTable);
        assertThat(where.sqlWithPlaceHolders, is(expected));
        assertThat(where.placeHolderValues, is(arrayContaining("mike")));
    }

    @Test
    public void constructsValidWhereClauseForNOTIN() {
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$in", Arrays.<Object>asList("mike", "fred"));
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", op);
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", notOp);

        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(name),
                                                                 indexName);
        String expectedSubWhere = "WHERE \"name\" IN ( ?, ? ))";
        String expected = String.format("_id NOT IN (SELECT _id FROM %s %s",
                                        indexTable,
                                        expectedSubWhere);
        assertThat(where.sqlWithPlaceHolders, is(expected));
        assertThat(where.placeHolderValues, is(arrayContaining("mike", "fred")));
    }

    @Test
    public void returnsCorrectWhenTwoConditionsOneField() {
        Map<String, Object> c1op = new HashMap<String, Object>();
        c1op.put("$eq", "mike");
        Map<String, Object> c1not = new HashMap<String, Object>();
        c1not.put("$not", c1op);
        Map<String, Object> c1 = new HashMap<String, Object>();
        c1.put("name", c1not);
        Map<String, Object> c2op = new HashMap<String, Object>();
        c2op.put("$eq", "john");
        Map<String, Object> c2 = new HashMap<String, Object>();
        c2.put("name", c2op);

        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(Arrays.<Object>asList(c1, c2),
                                                                 indexName);
        String notEqName = String.format("_id NOT IN (SELECT _id FROM %s WHERE \"name\" = ?)",
                                         indexTable);
        String eqName = "\"name\" = ?";
        String expected = String.format("%s AND %s", notEqName, eqName);
        assertThat(where.sqlWithPlaceHolders, is(expected));
        assertThat(where.placeHolderValues, is(arrayContaining("mike", "john")));
    }

    @Test
    public void returnsCorrectWhenMultiConditionsMultiFields() {
        Map<String, Object> c1op = new HashMap<String, Object>();
        c1op.put("$gt", 12);
        Map<String, Object> c1 = new HashMap<String, Object>();
        c1.put("age", c1op);
        Map<String, Object> c2op = new HashMap<String, Object>();
        c2op.put("$lte", 54);
        Map<String, Object> c2 = new HashMap<String, Object>();
        c2.put("age", c2op);
        Map<String, Object> c3op = new HashMap<String, Object>();
        c3op.put("$eq", "mike");
        Map<String, Object> c3 = new HashMap<String, Object>();
        c3.put("name", c3op);
        Map<String, Object> c4op = new HashMap<String, Object>();
        c4op.put("$eq", 30);
        Map<String, Object> c4NotOp = new HashMap<String, Object>();
        c4NotOp.put("$not", c4op);
        Map<String, Object> c4 = new HashMap<String, Object>();
        c4.put("age", c4NotOp);
        Map<String, Object> c5op = new HashMap<String, Object>();
        c5op.put("$eq", 42);
        Map<String, Object> c5 = new HashMap<String, Object>();
        c5.put("age", c5op);

        List<Object> clauses = Arrays.<Object>asList(c1, c2, c3, c4, c5);
        SqlParts where = QuerySqlTranslator.whereSqlForAndClause(clauses, indexName);
        String ageGt = "\"age\" > ?";
        String ageLte = "\"age\" <= ?";
        String nameEq = "\"name\" = ?";
        String ageNe = String.format("_id NOT IN (SELECT _id FROM %s WHERE \"age\" = ?)",
                                     indexTable);
        String ageEq = "\"age\" = ?";
        String expected = String.format("%s AND %s AND %s AND %s AND %s",
                                        ageGt,
                                        ageLte,
                                        nameEq,
                                        ageNe,
                                        ageEq);
        assertThat(where.sqlWithPlaceHolders, is(expected));
        assertThat(where.placeHolderValues, is(arrayContaining("12", "54", "mike", "30", "42")));
    }

    // When generating query SELECT clauses

    @Test
    public void nullSelectClauseWhenQueryEmpty() {
        assertThat(QuerySqlTranslator.selectStatementForAndClause(null, "named"), is(nullValue()));
        SqlParts sql = QuerySqlTranslator.selectStatementForAndClause(new ArrayList<Object>(),
                                                                      "named");
        assertThat(sql, is(nullValue()));
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
