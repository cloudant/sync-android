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

import static com.cloudant.sync.query.QueryExecutor.sqlToSortIds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.cloudant.sync.util.SQLDatabaseTestUtils;

import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class QuerySortTest extends AbstractQueryTestSetUp {

    Map<String, Object> indexes;
    Set<String> smallDocIdSet;
    Set<String> largeDocIdSet;

    @Override
    public void setUp() throws SQLException {
        super.setUp();
        im = new IndexManager(ds);
        assertThat(im, is(notNullValue()));
        db = im.getDatabase();
        assertThat(db, is(notNullValue()));
        assertThat(im.getQueue(), is(notNullValue()));
        String[] metadataTableList = new String[] { IndexManager.INDEX_METADATA_TABLE_NAME };
        SQLDatabaseTestUtils.assertTablesExist(db, metadataTableList);

        Map<String, Object> indexA = new HashMap<String, Object>();
        indexA.put("name", "a");
        indexA.put("type", "json");
        indexA.put("fields", Arrays.<Object>asList("name", "age", "pet"));
        Map<String, Object> indexB = new HashMap<String, Object>();
        indexB.put("name", "b");
        indexB.put("type", "json");
        indexB.put("fields", Arrays.<Object>asList("x", "y", "z"));
        indexes = new HashMap<String, Object>();
        indexes.put("a", indexA);
        indexes.put("b", indexB);
        smallDocIdSet = new HashSet<String>(Arrays.asList("mike", "john"));
        largeDocIdSet = new HashSet<String>();
        for (int i = 0; i < 501; i++) {  // 500 max id set for placeholders
            largeDocIdSet.add(String.format("doc-%d", i));
        }
    }

    // When sorting

    @Test
    public void sortsOnName() {
        setUpSortingQueryData();
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("same", "all");
        Map<String, String> sortName = new HashMap<String, String>();
        sortName.put("name", "asc");
        List<Map<String, String>> order = new ArrayList<Map<String, String>>();
        order.add(sortName);
        QueryResult queryResult = im.find(query, 0, Long.MAX_VALUE, null, order);
        assertThat(queryResult.documentIds(), contains("fred11", "fred34", "mike12"));
    }

    @Test
    public void sortsOnNameAge() {
        setUpSortingQueryData();
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("same", "all");
        Map<String, String> sortName = new HashMap<String, String>();
        sortName.put("name", "asc");
        Map<String, String> sortAge = new HashMap<String, String>();
        sortAge.put("age", "desc");
        List<Map<String, String>> order = new ArrayList<Map<String, String>>();
        order.add(sortName);
        order.add(sortAge);
        QueryResult queryResult = im.find(query, 0, Long.MAX_VALUE, null, order);
        assertThat(queryResult.documentIds(), contains("fred34", "fred11", "mike12"));
    }

    @Test
    public void sortsOnArrayField() {
        setUpSortingQueryData();
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("same", "all");
        Map<String, String> sortPet = new HashMap<String, String>();
        sortPet.put("pet", "asc");
        List<Map<String, String>> order = new ArrayList<Map<String, String>>();
        order.add(sortPet);
        QueryResult queryResult = im.find(query, 0, Long.MAX_VALUE, null, order);
        assertThat(queryResult.documentIds(), contains("mike12", "fred11", "fred34"));
    }

    @Test
    public void returnsNullWhenNotUsingAscOrDesc() {
        setUpSortingQueryData();
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("same", "all");
        Map<String, String> sortName = new HashMap<String, String>();
        sortName.put("name", "blah");
        Map<String, String> sortAge = new HashMap<String, String>();
        sortAge.put("age", "desc");
        List<Map<String, String>> order = new ArrayList<Map<String, String>>();
        order.add(sortName);
        order.add(sortAge);
        QueryResult queryResult = im.find(query, 0, Long.MAX_VALUE, null, order);
        assertThat(queryResult, is(nullValue()));
    }

    @Test
    public void returnsNullWhenTooManyClauses() {
        setUpSortingQueryData();
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("same", "all");
        Map<String, String> sort = new HashMap<String, String>();
        sort.put("name", "asc");
        sort.put("age", "desc");
        List<Map<String, String>> order = new ArrayList<Map<String, String>>();
        order.add(sort);
        QueryResult queryResult = im.find(query, 0, Long.MAX_VALUE, null, order);
        assertThat(queryResult, is(nullValue()));
    }

    // When generating ordering SQL

    @Test
    public void smallDocSetForSingleFieldUsingAsc() {
        Map<String, String> sortName = new HashMap<String, String>();
        sortName.put("name", "asc");
        List<Map<String, String>> order = new ArrayList<Map<String, String>>();
        order.add(sortName);
        SqlParts parts = sqlToSortIds(smallDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_a";
        String where = "WHERE _id IN (?, ?) ORDER BY \"name\" ASC";
        String sql = String.format("%s %s", select, where);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues,
                is(smallDocIdSet.toArray(new String[smallDocIdSet.size()])));
    }

    @Test
    public void smallDocSetForSingleFieldUsingDesc() {
        Map<String, String> sortY = new HashMap<String, String>();
        sortY.put("y", "desc");
        List<Map<String, String>> order = new ArrayList<Map<String, String>>();
        order.add(sortY);
        SqlParts parts = sqlToSortIds(smallDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_b";
        String where = "WHERE _id IN (?, ?) ORDER BY \"y\" DESC";
        String sql = String.format("%s %s", select, where);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues,
                is(smallDocIdSet.toArray(new String[smallDocIdSet.size()])));
    }

    @Test
    public void smallDocSetForMultipleFieldUsingAsc() {
        Map<String, String> sortY = new HashMap<String, String>();
        sortY.put("y", "asc");
        Map<String, String> sortX = new HashMap<String, String>();
        sortX.put("x", "asc");
        List<Map<String, String>> order = new ArrayList<Map<String, String>>();
        order.add(sortY);
        order.add(sortX);
        SqlParts parts = sqlToSortIds(smallDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_b";
        String where = "WHERE _id IN (?, ?) ORDER BY \"y\" ASC, \"x\" ASC";
        String sql = String.format("%s %s", select, where);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues,
                is(smallDocIdSet.toArray(new String[smallDocIdSet.size()])));
    }

    @Test
    public void smallDocSetForMultipleFieldUsingDesc() {
        Map<String, String> sortY = new HashMap<String, String>();
        sortY.put("y", "desc");
        Map<String, String> sortX = new HashMap<String, String>();
        sortX.put("x", "desc");
        List<Map<String, String>> order = new ArrayList<Map<String, String>>();
        order.add(sortY);
        order.add(sortX);
        SqlParts parts = sqlToSortIds(smallDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_b";
        String where = "WHERE _id IN (?, ?) ORDER BY \"y\" DESC, \"x\" DESC";
        String sql = String.format("%s %s", select, where);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues,
                is(smallDocIdSet.toArray(new String[smallDocIdSet.size()])));
    }

    @Test
    public void smallDocSetForMultipleFieldUsingMixed() {
        Map<String, String> sortY = new HashMap<String, String>();
        sortY.put("y", "desc");
        Map<String, String> sortX = new HashMap<String, String>();
        sortX.put("x", "asc");
        List<Map<String, String>> order = new ArrayList<Map<String, String>>();
        order.add(sortY);
        order.add(sortX);
        SqlParts parts = sqlToSortIds(smallDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_b";
        String where = "WHERE _id IN (?, ?) ORDER BY \"y\" DESC, \"x\" ASC";
        String sql = String.format("%s %s", select, where);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues,
                is(smallDocIdSet.toArray(new String[smallDocIdSet.size()])));
    }

    @Test
    public void largeDocSetForSingleFieldUsingAsc() {
        Map<String, String> sortName = new HashMap<String, String>();
        sortName.put("name", "asc");
        List<Map<String, String>> order = new ArrayList<Map<String, String>>();
        order.add(sortName);
        SqlParts parts = sqlToSortIds(largeDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_a";
        String orderBy = "ORDER BY \"name\" ASC";
        String sql = String.format("%s  %s", select, orderBy);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues, is(new String[]{}));
    }

    @Test
    public void largeDocSetForSingleFieldUsingDesc() {
        Map<String, String> sortY = new HashMap<String, String>();
        sortY.put("y", "desc");
        List<Map<String, String>> order = new ArrayList<Map<String, String>>();
        order.add(sortY);
        SqlParts parts = sqlToSortIds(largeDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_b";
        String orderBy = "ORDER BY \"y\" DESC";
        String sql = String.format("%s  %s", select, orderBy);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues, is(new String[]{}));
    }

    @Test
    public void largeDocSetForMultipleFieldUsingAsc() {
        Map<String, String> sortY = new HashMap<String, String>();
        sortY.put("y", "asc");
        Map<String, String> sortX = new HashMap<String, String>();
        sortX.put("x", "asc");
        List<Map<String, String>> order = new ArrayList<Map<String, String>>();
        order.add(sortY);
        order.add(sortX);
        SqlParts parts = sqlToSortIds(largeDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_b";
        String orderBy = "ORDER BY \"y\" ASC, \"x\" ASC";
        String sql = String.format("%s  %s", select, orderBy);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues, is(new String[]{}));
    }

    @Test
    public void largeDocSetForMultipleFieldUsingDesc() {
        Map<String, String> sortY = new HashMap<String, String>();
        sortY.put("y", "desc");
        Map<String, String> sortX = new HashMap<String, String>();
        sortX.put("x", "desc");
        List<Map<String, String>> order = new ArrayList<Map<String, String>>();
        order.add(sortY);
        order.add(sortX);
        SqlParts parts = sqlToSortIds(largeDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_b";
        String orderBy = "ORDER BY \"y\" DESC, \"x\" DESC";
        String sql = String.format("%s  %s", select, orderBy);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues, is(new String[]{}));
    }

    @Test
    public void largeDocSetForMultipleFieldUsingMixed() {
        Map<String, String> sortY = new HashMap<String, String>();
        sortY.put("y", "desc");
        Map<String, String> sortX = new HashMap<String, String>();
        sortX.put("x", "asc");
        List<Map<String, String>> order = new ArrayList<Map<String, String>>();
        order.add(sortY);
        order.add(sortX);
        SqlParts parts = sqlToSortIds(largeDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_b";
        String orderBy = "ORDER BY \"y\" DESC, \"x\" ASC";
        String sql = String.format("%s  %s", select, orderBy);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues, is(new String[]{}));
    }

    @Test
    public void failsWhenUsingUnindexedField() {
        Map<String, String> sortApples = new HashMap<String, String>();
        sortApples.put("apples", "asc");
        List<Map<String, String>> order = new ArrayList<Map<String, String>>();
        order.add(sortApples);
        assertThat(sqlToSortIds(smallDocIdSet, order, indexes), is(nullValue()));
    }

    @Test
    public void failsWhenFieldsNotInSingleIndex() {
        Map<String, String> sortX = new HashMap<String, String>();
        sortX.put("x", "asc");
        Map<String, String> sortAge = new HashMap<String, String>();
        sortAge.put("age", "asc");
        List<Map<String, String>> order = new ArrayList<Map<String, String>>();
        order.add(sortX);
        order.add(sortAge);
        assertThat(sqlToSortIds(smallDocIdSet, order, indexes), is(nullValue()));
    }

    @Test
    public void returnsNullWhenNoIndexes() {
        Map<String, String> sortY = new HashMap<String, String>();
        sortY.put("y", "desc");
        List<Map<String, String>> order = new ArrayList<Map<String, String>>();
        order.add(sortY);
        assertThat(sqlToSortIds(smallDocIdSet, order, new HashMap<String, Object>()),
                is(nullValue()));
        assertThat(sqlToSortIds(smallDocIdSet, order, null), is(nullValue()));
    }

}