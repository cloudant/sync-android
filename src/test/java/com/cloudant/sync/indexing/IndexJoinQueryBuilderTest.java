/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.indexing;

import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

public class IndexJoinQueryBuilderTest {

    IndexJoinQueryBuilder builder;

    @Before
    public void setUp() {
        builder = new IndexJoinQueryBuilder();
    }

    @Test
    public void integerIndex_buildQuerySQL_simpleInteger() {
        QueryBuilder qb = new QueryBuilder();
        qb.index("year").equalTo(2013l);
        String expectSQL = "SELECT DISTINCT album.docid FROM album WHERE album.value = 2013";
        builder.addQueryCriterion("album", qb.build().get("query").get("year"), IndexType.INTEGER);
        Assert.assertEquals(expectSQL, builder.toSQL());
    }

    @Test
    public void stringIndex_buildQuerySQL_simpleString() {
        QueryBuilder qb = new QueryBuilder();
        qb.index("year").equalTo("2013");
        String expectSQL = "SELECT DISTINCT album.docid FROM album WHERE album.value = '2013'";
        builder.addQueryCriterion("album", qb.build().get("query").get("year"), IndexType.STRING);
        Assert.assertEquals(expectSQL, builder.toSQL());
    }


    @Test
    public void integerIndex_buildQuerySQL_listOfIntegers() {
        QueryBuilder qb = new QueryBuilder();
        qb.index("year").oneOf(new Long[]{2013l, 2014l, 2015l});
        String expectSQL = "SELECT DISTINCT album.docid FROM album WHERE album.value IN (2013,2014,2015)";
        builder.addQueryCriterion("album", qb.build().get("query").get("year"), IndexType.INTEGER);
        Assert.assertEquals(expectSQL, builder.toSQL());
    }

    @Test
    public void stringIndex_buildQuerySQL_listOfStrings() {
        QueryBuilder qb = new QueryBuilder();
        qb.index("year").oneOf(new String[]{"2013", "2014", "2015"});
        String expectSQL = "SELECT DISTINCT album.docid FROM album WHERE album.value IN ('2013','2014','2015')";
        builder.addQueryCriterion("album", qb.build().get("query").get("year"), IndexType.STRING);
        Assert.assertEquals(expectSQL, builder.toSQL());
    }


    @Test
    public void integerIndex_buildQuerySQL_RangeQuery() {
        QueryBuilder qb = new QueryBuilder();
        qb.index("year").greaterThan(2012l).index("year").lessThan(2016l);
        String expectSQL = "SELECT DISTINCT album.docid FROM album WHERE album.value < 2016 AND album.value > 2012";
        builder.addQueryCriterion("album", qb.build().get("query").get("year"), IndexType.INTEGER);
        Assert.assertEquals(expectSQL, builder.toSQL());
    }

    @Test
    public void stringIndex_buildQuerySQL_RangeQuery() {
        QueryBuilder qb = new QueryBuilder();
        qb.index("year").greaterThan("2012").index("year").lessThan("2016");
        String expectSQL = "SELECT DISTINCT album.docid FROM album WHERE album.value < '2016' AND album.value > '2012'";
        builder.addQueryCriterion("album", qb.build().get("query").get("year"), IndexType.STRING);
        Assert.assertEquals(expectSQL, builder.toSQL());
    }


    @Test
    public void integerIndex_buildQuerySQL_RangeQueryWithMinIntegerOnly() {
        QueryBuilder qb = new QueryBuilder();
        qb.index("year").greaterThan(2012l);
        String expectSQL = "SELECT DISTINCT album.docid FROM album WHERE album.value > 2012";
        builder.addQueryCriterion("album", qb.build().get("query").get("year"), IndexType.INTEGER);
        Assert.assertEquals(expectSQL, builder.toSQL());
    }

    @Test
    public void stringIndex_buildQuerySQL_RangeQueryWithMinStringOnly() {
        QueryBuilder qb = new QueryBuilder();
        qb.index("year").lessThan("2016");
        String expectSQL = "SELECT DISTINCT album.docid FROM album WHERE album.value < '2016'";
        builder.addQueryCriterion("album", qb.build().get("query").get("year"), IndexType.STRING);
        Assert.assertEquals(expectSQL, builder.toSQL());
    }


    @Test
    public void integerIndex_buildQuerySQL_RangeQueryWithMaxIntegerOnly() {
        QueryBuilder qb = new QueryBuilder();
        qb.index("year").lessThan(2012l);
        String expectSQL = "SELECT DISTINCT album.docid FROM album WHERE album.value < 2012";
        builder.addQueryCriterion("album", qb.build().get("query").get("year"), IndexType.INTEGER);
        Assert.assertEquals(expectSQL, builder.toSQL());
    }


    @Test
    public void stringIndex_buildQuerySQL_RangeQueryWithMaxStringOnly() {
        QueryBuilder qb = new QueryBuilder();
        qb.index("year").greaterThan("2012");
        String expectSQL = "SELECT DISTINCT album.docid FROM album WHERE album.value > '2012'";
        builder.addQueryCriterion("album", qb.build().get("query").get("year"), IndexType.STRING);
        Assert.assertEquals(expectSQL, builder.toSQL());
    }

    @Test
    public void stringIndex_buildQuerySQL_singleQuoteShouldBeEscaped() {
        QueryBuilder qb = new QueryBuilder();
        qb.index("name").equalTo("tom's nick name");
        String expectSQL = "SELECT DISTINCT album.docid FROM album WHERE album.value = 'tom''s nick name'";
        builder.addQueryCriterion("album", qb.build().get("query").get("name"), IndexType.STRING);
        Assert.assertEquals(expectSQL, builder.toSQL());
    }

    @Test
    public void multipleIndexes_buildQuerySQL_simpleIntegerPlusSimpleString() {
        QueryBuilder qb = new QueryBuilder();
        qb.index("year").equalTo(2013l);
        qb.index("name").equalTo("tom's nick name");
        String expectSQL = "SELECT DISTINCT artistAlbum.docid" +
                " FROM artistAlbum" +
                " JOIN artistName ON artistAlbum.docid = artistName.docid" +
                " WHERE artistAlbum.value = 2013 AND artistName.value = 'tom''s nick name'";
        builder.addQueryCriterion("artistAlbum", qb.build().get("query").get("year"), IndexType.INTEGER);
        builder.addQueryCriterion("artistName", qb.build().get("query").get("name"), IndexType.STRING);
        Assert.assertEquals(expectSQL, builder.toSQL());
    }

    @Test
    public void multipleIndexes_buildQuerySQL_simpleIntegerSimpleListOfString() {
        QueryBuilder qb = new QueryBuilder();
        qb.index("year").equalTo(2013l);
        qb.index("name").oneOf("Tommy Jones", "Harrison Ford");
        String expectSQL = "SELECT DISTINCT artistAlbum.docid" +
                " FROM artistAlbum" +
                " JOIN artistName ON artistAlbum.docid = artistName.docid" +
                " WHERE artistAlbum.value = 2013 AND artistName.value IN ('Tommy Jones','Harrison Ford')";
        builder.addQueryCriterion("artistAlbum", qb.build().get("query").get("year"), IndexType.INTEGER);
        builder.addQueryCriterion("artistName", qb.build().get("query").get("name"), IndexType.STRING);
        Assert.assertEquals(expectSQL, builder.toSQL());
    }

    @Test
    public void multipleIndexes_buildQuerySQL_simpleIntegerSimpleListOfStringPlusRangQuery() {
        QueryBuilder qb = new QueryBuilder();
        qb.index("year").equalTo(2013l);
        qb.index("name").oneOf("Tommy Jones", "Harrison Ford");
        qb.index("age").greaterThan(23l);
        qb.index("age").lessThan(99l);
        String expectSQL = "SELECT DISTINCT artistAlbum.docid" +
                " FROM artistAlbum" +
                " JOIN artistName ON artistAlbum.docid = artistName.docid" +
                " JOIN artistAge ON artistAlbum.docid = artistAge.docid" +
                " WHERE artistAlbum.value = 2013" +
                " AND artistName.value IN ('Tommy Jones','Harrison Ford')" +
                " AND artistAge.value < 99 AND artistAge.value > 23";
        builder.addQueryCriterion("artistAlbum", qb.build().get("query").get("year"), IndexType.INTEGER);
        builder.addQueryCriterion("artistName", qb.build().get("query").get("name"), IndexType.STRING);
        builder.addQueryCriterion("artistAge", qb.build().get("query").get("age"), IndexType.INTEGER);
        Assert.assertEquals(expectSQL, builder.toSQL());
    }
}
