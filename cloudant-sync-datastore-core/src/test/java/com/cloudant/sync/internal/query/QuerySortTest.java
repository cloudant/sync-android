/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2015 Cloudant, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.internal.query;

import static com.cloudant.sync.internal.query.QueryExecutor.sqlToSortIds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.cloudant.sync.query.FieldSort;
import com.cloudant.sync.query.Index;
import com.cloudant.sync.query.IndexType;
import com.cloudant.sync.query.QueryException;
import com.cloudant.sync.query.QueryResult;
import com.cloudant.sync.util.SQLDatabaseTestUtils;
import com.cloudant.sync.util.TestUtils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class QuerySortTest extends AbstractQueryTestBase {

    List<Index> indexes;
    Set<String> smallDocIdSet;
    Set<String> largeDocIdSet;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexManagerDatabaseQueue = TestUtils.getDBQueue(im);
        assertThat(im, is(notNullValue()));
        assertThat(TestUtils.getDBQueue(im), is(notNullValue()));
        String[] metadataTableList = new String[] { QueryConstants.INDEX_METADATA_TABLE_NAME };
        SQLDatabaseTestUtils.assertTablesExist(indexManagerDatabaseQueue, metadataTableList);

        Index indexA = new Index(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age"), new FieldSort("pet")), "a", IndexType.JSON, null);
        Index indexB = new Index(Arrays.<FieldSort>asList(new FieldSort("x"), new FieldSort("y"), new FieldSort("z")), "b", IndexType.JSON, null);
        indexes = new ArrayList<Index>();
        indexes.add(indexA);
        indexes.add(indexB);
        smallDocIdSet = new HashSet<String>(Arrays.asList("mike", "john"));
        largeDocIdSet = new HashSet<String>();
        for (int i = 0; i < 501; i++) {  // 500 max ID set for placeholders
            largeDocIdSet.add(String.format("doc-%d", i));
        }
    }

    // When sorting

    @Test
    public void sortsOnName() throws Exception {
        setUpSortingQueryData();
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("same", "all");
        List<FieldSort> order = Arrays.<FieldSort>asList(new FieldSort("name", FieldSort.Direction.ASCENDING));
        QueryResult queryResult = im.find(query, 0, Long.MAX_VALUE, null, order);
        assertThat(queryResult.documentIds(), contains("fred11", "fred34", "mike12"));
    }

    @Test
    public void sortsOnNameAge() throws Exception {
        setUpSortingQueryData();
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("same", "all");
        List<FieldSort> order = Arrays.<FieldSort>asList(new FieldSort("name", FieldSort.Direction.ASCENDING), new FieldSort("age", FieldSort.Direction.DESCENDING));
        QueryResult queryResult = im.find(query, 0, Long.MAX_VALUE, null, order);
        assertThat(queryResult.documentIds(), contains("fred34", "fred11", "mike12"));
    }

    @Test
    public void sortsOnArrayField() throws Exception {
        setUpSortingQueryData();
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("same", "all");
        List<FieldSort> order = Arrays.<FieldSort>asList(new FieldSort("pet", FieldSort.Direction.ASCENDING));
        QueryResult queryResult = im.find(query, 0, Long.MAX_VALUE, null, order);
        assertThat(queryResult.documentIds(), contains("mike12", "fred11", "fred34"));
    }

    // TODO check test can be deleted - i think it relates to the way the sort document is built up which is no longer relevant
    //@Test
    public void returnsNullWhenTooManyClauses() throws Exception{
        setUpSortingQueryData();
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("same", "all");
        List<FieldSort> order = Arrays.<FieldSort>asList(new FieldSort("name", FieldSort.Direction.ASCENDING), new FieldSort("age", FieldSort.Direction.DESCENDING));
        QueryResult queryResult = im.find(query, 0, Long.MAX_VALUE, null, order);
        assertThat(queryResult, is(nullValue()));
    }

    // When generating ordering SQL

    @Test
    public void smallDocSetForSingleFieldUsingAsc() throws QueryException {
        List<FieldSort> order = Arrays.<FieldSort>asList(new FieldSort("name", FieldSort.Direction.ASCENDING));
        SqlParts parts = sqlToSortIds(smallDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_a";
        String where = "WHERE _id IN (?, ?) ORDER BY \"name\" ASC";
        String sql = String.format("%s %s", select, where);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues,
                is(smallDocIdSet.toArray(new String[smallDocIdSet.size()])));
    }

    @Test
    public void smallDocSetForSingleFieldUsingDesc() throws QueryException {
        List<FieldSort> order = Arrays.<FieldSort>asList(new FieldSort("y", FieldSort.Direction.DESCENDING));
        SqlParts parts = sqlToSortIds(smallDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_b";
        String where = "WHERE _id IN (?, ?) ORDER BY \"y\" DESC";
        String sql = String.format("%s %s", select, where);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues,
                is(smallDocIdSet.toArray(new String[smallDocIdSet.size()])));
    }

    @Test
    public void smallDocSetForMultipleFieldUsingAsc() throws QueryException {
        List<FieldSort> order = Arrays.<FieldSort>asList(new FieldSort("y", FieldSort.Direction.ASCENDING), new FieldSort("x", FieldSort.Direction.ASCENDING));
        SqlParts parts = sqlToSortIds(smallDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_b";
        String where = "WHERE _id IN (?, ?) ORDER BY \"y\" ASC, \"x\" ASC";
        String sql = String.format("%s %s", select, where);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues,
                is(smallDocIdSet.toArray(new String[smallDocIdSet.size()])));
    }

    @Test
    public void smallDocSetForMultipleFieldUsingDesc() throws QueryException {
        List<FieldSort> order = Arrays.<FieldSort>asList(new FieldSort("y", FieldSort.Direction.DESCENDING), new FieldSort("x", FieldSort.Direction.DESCENDING));
        SqlParts parts = sqlToSortIds(smallDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_b";
        String where = "WHERE _id IN (?, ?) ORDER BY \"y\" DESC, \"x\" DESC";
        String sql = String.format("%s %s", select, where);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues,
                is(smallDocIdSet.toArray(new String[smallDocIdSet.size()])));
    }

    @Test
    public void smallDocSetForMultipleFieldUsingMixed() throws QueryException {
        List<FieldSort> order = Arrays.<FieldSort>asList(new FieldSort("y", FieldSort.Direction.DESCENDING), new FieldSort("x", FieldSort.Direction.ASCENDING));
        SqlParts parts = sqlToSortIds(smallDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_b";
        String where = "WHERE _id IN (?, ?) ORDER BY \"y\" DESC, \"x\" ASC";
        String sql = String.format("%s %s", select, where);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues,
                is(smallDocIdSet.toArray(new String[smallDocIdSet.size()])));
    }

    @Test
    public void largeDocSetForSingleFieldUsingAsc() throws QueryException {
        List<FieldSort> order = Arrays.<FieldSort>asList(new FieldSort("name", FieldSort.Direction.ASCENDING));
        SqlParts parts = sqlToSortIds(largeDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_a";
        String orderBy = "ORDER BY \"name\" ASC";
        String sql = String.format("%s  %s", select, orderBy);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues, is(new String[]{}));
    }

    @Test
    public void largeDocSetForSingleFieldUsingDesc() throws QueryException {
        List<FieldSort> order = Arrays.<FieldSort>asList(new FieldSort("y", FieldSort.Direction.DESCENDING));
        SqlParts parts = sqlToSortIds(largeDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_b";
        String orderBy = "ORDER BY \"y\" DESC";
        String sql = String.format("%s  %s", select, orderBy);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues, is(new String[]{}));
    }

    @Test
    public void largeDocSetForMultipleFieldUsingAsc() throws QueryException {
        List<FieldSort> order = Arrays.<FieldSort>asList(new FieldSort("y", FieldSort.Direction.ASCENDING), new FieldSort("x", FieldSort.Direction.ASCENDING));
        SqlParts parts = sqlToSortIds(largeDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_b";
        String orderBy = "ORDER BY \"y\" ASC, \"x\" ASC";
        String sql = String.format("%s  %s", select, orderBy);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues, is(new String[]{}));
    }

    @Test
    public void largeDocSetForMultipleFieldUsingDesc() throws QueryException {
        List<FieldSort> order = Arrays.<FieldSort>asList(new FieldSort("y", FieldSort.Direction.DESCENDING), new FieldSort("x", FieldSort.Direction.DESCENDING));
        SqlParts parts = sqlToSortIds(largeDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_b";
        String orderBy = "ORDER BY \"y\" DESC, \"x\" DESC";
        String sql = String.format("%s  %s", select, orderBy);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues, is(new String[]{}));
    }

    @Test
    public void largeDocSetForMultipleFieldUsingMixed() throws QueryException {
        List<FieldSort> order = Arrays.<FieldSort>asList(new FieldSort("y", FieldSort.Direction.DESCENDING), new FieldSort("x", FieldSort.Direction.ASCENDING));
        SqlParts parts = sqlToSortIds(largeDocIdSet, order, indexes);
        String select = "SELECT DISTINCT _id FROM _t_cloudant_sync_query_index_b";
        String orderBy = "ORDER BY \"y\" DESC, \"x\" ASC";
        String sql = String.format("%s  %s", select, orderBy);
        assertThat(parts.sqlWithPlaceHolders, is(sql));
        assertThat(parts.placeHolderValues, is(new String[]{}));
    }

    @Test(expected = QueryException.class)
    public void failsWhenUsingUnindexedField() throws QueryException {
        List<FieldSort> order = Arrays.<FieldSort>asList(new FieldSort("apples", FieldSort.Direction.ASCENDING));
        sqlToSortIds(smallDocIdSet, order, indexes);
    }

    @Test(expected = QueryException.class)
    public void failsWhenFieldsNotInSingleIndex() throws QueryException {
        List<FieldSort> order = Arrays.<FieldSort>asList(new FieldSort("x", FieldSort.Direction.ASCENDING), new FieldSort("age", FieldSort.Direction.ASCENDING));
        sqlToSortIds(smallDocIdSet, order, indexes);
    }

    @Test(expected = QueryException.class)
    public void returnsNullWhenNoIndexes() throws QueryException {
        List<FieldSort> order = Arrays.<FieldSort>asList(new FieldSort("y", FieldSort.Direction.DESCENDING));
        sqlToSortIds(smallDocIdSet, order, new ArrayList<Index>());
    }

    @Test(expected = QueryException.class)
    public void returnsNullWhenNullIndexes() throws QueryException {
        List<FieldSort> order = Arrays.<FieldSort>asList(new FieldSort("y", FieldSort.Direction.DESCENDING));
        sqlToSortIds(smallDocIdSet, order, null);
    }


}
