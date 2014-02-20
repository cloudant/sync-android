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


import com.cloudant.sync.indexing.QueryBuilder;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *  { company': 'some company', 'age': {'max': 120, 'min': 12}, 'name': ['Tom', 'Jerry']}
 */
public class QueryBuilderTest {

    QueryBuilder queryBuilder = null;

    @Before
    public void setUp() {
        queryBuilder = new QueryBuilder();
        Assert.assertNotNull(queryBuilder);
    }

    @Test
    public void equalsTo_String() {
        queryBuilder.index("album").equalTo("X&Y");
        Map<String, Object> query = queryBuilder.build().get("query");
        Assert.assertEquals(1, query.size());
        Assert.assertTrue(query.containsKey("album"));
        Assert.assertEquals("X&Y", query.get("album"));
    }

    @Test(expected = IllegalStateException.class)
    public void equalsTo_StringButIndexNotCalled_exception() {
        queryBuilder.equalTo("X&Y");
    }

    @Test
    public void equalsTo_Long() {
        queryBuilder.index("album").equalTo(100L);
        Map<String, Object> query = queryBuilder.build().get("query");
        Assert.assertEquals(1, query.size());
        Assert.assertTrue(query.containsKey("album"));
        Assert.assertEquals(100L, query.get("album"));
    }

    @Test(expected = IllegalStateException.class)
    public void equalsTo_LongButIndexNotCalled_exception() {
        queryBuilder.equalTo(100L);
    }

    @Test
    public void oneOf_ListOfString() {
        queryBuilder.index("album").oneOf(new String[]{"value1", "value2"});
        Map<String, Object> query = queryBuilder.build().get("query");
        Assert.assertEquals(1, query.size());
        Assert.assertTrue(query.containsKey("album"));
        Assert.assertTrue(query.get("album") instanceof List);
        Assert.assertTrue(((List) query.get("album")).contains("value1"));
        Assert.assertTrue(((List) query.get("album")).contains("value2"));
    }

    @Test(expected = IllegalStateException.class)
    public void oneOf_ListOfStringButIndexNotCalled() {
        queryBuilder.oneOf(new String[]{"value1", "value2"});
    }

    @Test
    public void oneOf_ListOfLong() {
        queryBuilder.index("album").oneOf(new Long[]{100L, 200L});
        Map<String, Object> query = queryBuilder.build().get("query");
        Assert.assertEquals(1, query.size());
        Assert.assertTrue(query.containsKey("album"));
        Assert.assertTrue(query.get("album") instanceof List);
        Assert.assertTrue(((List) query.get("album")).contains(100L));
        Assert.assertTrue(((List) query.get("album")).contains(200L));
    }

    @Test(expected = IllegalStateException.class)
    public void oneOf_ListOfLongButIndexNotCalled() {
        queryBuilder.oneOf(new Long[]{100L, 200L});
    }

    @Test
    public void greaterThan_String() {
        queryBuilder.index("album").greaterThanOrEqual("value");
        Map<String, Object> query = queryBuilder.build().get("query");
        Assert.assertEquals(1, query.size());
        Assert.assertTrue(query.get("album") instanceof Map);
        Assert.assertTrue(((Map) query.get("album")).containsKey("min"));
        Assert.assertTrue(((Map) query.get("album")).get("min").equals("value"));
    }

    @Test(expected = IllegalStateException.class)
    public void greaterThan_StringButIndexNotCalled() {
        queryBuilder.greaterThanOrEqual(100L);
    }

    @Test
    public void greaterThan_Long() {
        queryBuilder.index("album").greaterThanOrEqual(100L);
        Map<String, Object> query = queryBuilder.build().get("query");
        Assert.assertEquals(1, query.size());
        Assert.assertTrue(query.get("album") instanceof Map);
        Assert.assertTrue(((Map) query.get("album")).containsKey("min"));
        Assert.assertTrue(((Map) query.get("album")).get("min").equals(100L));
    }

    @Test(expected = IllegalStateException.class)
    public void greaterThan_LongButIndexNotCalled() {
        queryBuilder.greaterThanOrEqual(100L);
    }

    @Test
    public void lessThan_String() {
        queryBuilder.index("album").lessThanOrEqual("value");
        Map<String, Object> query = queryBuilder.build().get("query");
        Assert.assertEquals(1, query.size());
        Assert.assertTrue(query.get("album") instanceof Map);
        Assert.assertTrue(((Map) query.get("album")).containsKey("max"));
        Assert.assertTrue(((Map) query.get("album")).get("max").equals("value"));
    }

    @Test(expected = IllegalStateException.class)
    public void lessThan_StringButIndexNotCalled() {
        queryBuilder.lessThanOrEqual("value");
    }

    @Test
    public void lessThan_Long() {
        queryBuilder.index("album").lessThanOrEqual(100L);
        Map<String, Object> query = queryBuilder.build().get("query");
        Assert.assertEquals(1, query.size());
        Assert.assertTrue(query.get("album") instanceof Map);
        Assert.assertTrue(((Map) query.get("album")).containsKey("max"));
        Assert.assertTrue(((Map) query.get("album")).get("max").equals(100L));
    }

    @Test(expected = IllegalStateException.class)
    public void lessThan_LongButIndexNotCalled() {
        queryBuilder.lessThanOrEqual(100L);
    }

    @Test
    public void greaterThanAndLessThan() {
        queryBuilder.index("album").lessThanOrEqual(100L).index("album").greaterThanOrEqual(0L);
        Map<String, Object> query = queryBuilder.build().get("query");
        Assert.assertEquals(1, query.size());
        Assert.assertTrue(query.get("album") instanceof Map);
        Assert.assertTrue(((Map) query.get("album")).size() == 2);
        Assert.assertTrue(((Map) query.get("album")).get("max").equals(100L));
        Assert.assertTrue(((Map) query.get("album")).get("min").equals(0L));
    }

    @Test
    public void withEverything() {
        queryBuilder.index("company").equalTo("some company")
                .index("name").oneOf(new String[]{ "Tom", "Jerry"})
                .index("age").lessThanOrEqual(100L).index("age").greaterThanOrEqual(0L);
        Map<String, Object> query = queryBuilder.build().get("query");
        Assert.assertEquals(3, query.size());

        Assert.assertTrue(query.containsKey("company"));
        Assert.assertEquals("some company", query.get("company"));

        Assert.assertTrue(query.containsKey("name"));
        List<String> nameList = (List<String>)query.get("name");
        Assert.assertTrue(Arrays.equals(new String[]{"Tom", "Jerry"}, nameList.toArray(new String[]{})));

        Assert.assertTrue(query.containsKey("age"));
        Assert.assertEquals(0L, ((Map<String, Object>)query.get("age")).get("min"));
    }

    @Test
    public void build_100queryCriterion() {
        for(int i = 0 ; i < 100 ; i ++) {
            queryBuilder.index("index" + i).equalTo(i);
        }
        Assert.assertNotNull(queryBuilder.build());
    }

    @Test(expected = IllegalStateException.class)
    public void build_lessThanEqualTo_exception() {
        queryBuilder.index("age").lessThanOrEqual(100L).equalTo(10L);
    }
}
