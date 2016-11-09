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

package com.cloudant.sync.internal.replication;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DatabaseAssertTest {

    Map<String, Object> map1;
    Map<String, Object> map2;

    @Before
    public void setUp() {
        map1 = new HashMap<String, Object>();
        map2 = new HashMap<String, Object>();
    }

    @Test
    public void assertSameStringMap_emptyMap_success() {
        DatabaseAssert.assertSameStringMap(map1, map2);
    }

    @Test(expected = AssertionError.class)
    public void assertSameStringMap_differentKey_failure() {
        map1.put("a", "A");
        map2.put("A", "A");
        DatabaseAssert.assertSameStringMap(map1, map2);
    }

    @Test
    public void assertSameStringMap_string_success() {
        map1.put("a", "A");
        map2.put("a", "A");
        DatabaseAssert.assertSameStringMap(map1, map2);
    }

    @Test
    public void assertSameStringMap_null_success() {
        map1.put("a", null);
        map2.put("a", null);
        DatabaseAssert.assertSameStringMap(map1, map2);
    }

    @Test(expected = AssertionError.class)
    public void assertSameStringMap_oneIsNull_failure() {
        map1.put("a", 1);
        map2.put("a", null);
        DatabaseAssert.assertSameStringMap(map1, map2);
    }

    @Test
    public void assertSameStringMap_true_success() {
        map1.put("a", true);
        map2.put("a", true);
        DatabaseAssert.assertSameStringMap(map1, map2);
    }

    @Test
    public void assertSameStringMap_false_success() {
        map1.put("a", false);
        map2.put("a", false);
        DatabaseAssert.assertSameStringMap(map1, map2);
    }

    @Test
    public void assertSameStringMap_int_success() {
        map1.put("a", 1);
        map2.put("a", 1);
        DatabaseAssert.assertSameStringMap(map1, map2);
    }

    @Test
    public void assertSameStringMap_double_success() {
        map1.put("a", 1.0);
        map2.put("a", 1.0);
        DatabaseAssert.assertSameStringMap(map1, map2);
    }

    @Test
    public void assertSameStringMap_array_success() {
        map1.put("a", Arrays.asList("A", "B"));
        map2.put("a", Arrays.asList("A", "B"));
        DatabaseAssert.assertSameStringMap(map1, map2);
    }

    @Test(expected = AssertionError.class)
    public void assertSameStringMap_arrayInDifferentOrder_failure() {
        map1.put("a", Arrays.asList("A", "B"));
        map2.put("a", Arrays.asList("B", "A"));
        DatabaseAssert.assertSameStringMap(map1, map2);
    }

    @Test(expected = AssertionError.class)
    public void assertSameStringMap_differentString_failure() {
        map1.put("a", "A");
        map2.put("a", "B");
        DatabaseAssert.assertSameStringMap(map1, map2);
    }

    @Test(expected = AssertionError.class)
    public void assertSameStringMap_differentSize_failure() {
        map1.put("a", "A");
        map2.put("a", "A");
        map2.put("b", "B");
        DatabaseAssert.assertSameStringMap(map1, map2);
    }

    @Test(expected = AssertionError.class)
    public void assertSameStringMap_oneEmptyMap_failure() {
        map1.put("a", "A");
        DatabaseAssert.assertSameStringMap(map1, map2);
    }

    @Test
    public void assertSameStringMap_sameEmptyMapEntry_success() {
        Map map11 = new HashMap();
        map1.put("m", map11);
        Map map22 = new HashMap();
        map2.put("m", map22);

        DatabaseAssert.assertSameStringMap(map1, map2);
    }

    @Test
    public void assertSameStringMap_sameMapEntry_success() {
        Map map11 = new HashMap();
        map11.put("a", "A");
        map1.put("m", map11);

        Map map22 = new HashMap();
        map22.put("a", "A");
        map2.put("m", map22);

        DatabaseAssert.assertSameStringMap(map1, map2);
    }

    @Test(expected = AssertionError.class)
    public void assertSameStringMap_differentMapEntry_success() {
        Map map11 = new HashMap();
        map11.put("a", "A");
        map1.put("m", map11);

        Map map22 = new HashMap();
        map22.put("a", "B");
        map2.put("m", map22);

        DatabaseAssert.assertSameStringMap(map1, map2);
    }
}
