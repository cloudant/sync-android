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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@RunWith(JUnit4.class)
public class FieldIndexerTest {

    @Test
    public void constructor() {
        FieldIndexer fif = new FieldIndexer("FieldName");
        Assert.assertNotNull(fif);
    }

    @Test
    public void simple_field_name_with_multiple_fields() {
        String fieldName = "FieldName";
        String expected = "an expected value";

        FieldIndexer fif = new FieldIndexer(fieldName);

        Map map = new HashMap<String, String>();
        map.put("hello", "world");
        map.put("frogs", new Number[]{1,2,3});
        map.put(fieldName, expected);
        map.put("cats", "dogs");

        Assert.assertEquals(Arrays.asList(expected), fif.indexedValues("null", map));
    }

    @Test
    public void simple_field_string() {
        String expected = "an expected value";
        assertFieldExtractionWorks(expected);
    }

    @Test
    public void simple_field_name_number() {
        Integer expected = new Integer(2);
        assertFieldExtractionWorks(expected);
    }

    @Test
    public void simple_field_name_object() {
        Object[] expected =new Object[]{1,"world",3};
        assertFieldExtractionWorks(expected);
    }

    private void assertFieldExtractionWorks(Object expected) {
        String fieldName = "simple_field_string";
        FieldIndexer fif = new FieldIndexer(fieldName);
        Map map = new HashMap<String, String>();
        map.put(fieldName, expected);
        Assert.assertEquals(Arrays.asList(expected), fif.indexedValues("null", map));
    }
}