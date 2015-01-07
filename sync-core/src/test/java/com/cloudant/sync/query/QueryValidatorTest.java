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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class QueryValidatorTest {

    @Test
    @SuppressWarnings("unchecked")
    public void normalizeSingleFieldQuery() {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "name" : "mike" }
        query.put("name", "mike");
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "name" : { "$eq" : "mike" } } ]
        Assert.assertNotNull(normalizedQuery);
        Assert.assertEquals(1, normalizedQuery.size());
        Assert.assertTrue(normalizedQuery.keySet().iterator().next().equals("$and"));
        Assert.assertTrue(normalizedQuery.get("$and") instanceof List);
        List<Object> fields = (ArrayList) normalizedQuery.get("$and");
        Assert.assertEquals(1, fields.size());
        Assert.assertTrue(fields.get(0) instanceof Map);
        Map<String, Object> fieldMap = (Map) fields.get(0);
        Assert.assertEquals(1, fieldMap.size());
        Assert.assertTrue(fieldMap.keySet().iterator().next().equals("name"));
        Map<String, Object> predicate = (Map) fieldMap.get("name");
        Assert.assertEquals(1, predicate.size());
        Assert.assertTrue(predicate.keySet().iterator().next().equals("$eq"));
        Assert.assertTrue(predicate.get("$eq").equals("mike"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void normalizeMultiFieldQuery() {
        Map<String, Object> query = new LinkedHashMap<String, Object>();
        // query - { "name" : "mike", "age", 12 }
        query.put("name", "mike");
        query.put("age", 12);
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "name" : { "$eq" : "mike" } },
        //                                 { "age" : { "$eq" : "12" } } ]
        Assert.assertNotNull(normalizedQuery);
        Assert.assertEquals(1, normalizedQuery.size());
        Assert.assertTrue(normalizedQuery.keySet().iterator().next().equals("$and"));
        Assert.assertTrue(normalizedQuery.get("$and") instanceof List);
        List<Object> fields = (ArrayList) normalizedQuery.get("$and");
        Assert.assertEquals(2, fields.size());
        for (Object field: fields) {
            Assert.assertTrue(field instanceof Map);
        }
        Map<String, Object> fieldMap = (Map) fields.get(0);
        Assert.assertEquals(1, fieldMap.size());
        Assert.assertTrue(fieldMap.keySet().iterator().next().equals("name"));
        Map<String, Object> predicate = (Map) fieldMap.get("name");
        Assert.assertEquals(1, predicate.size());
        Assert.assertTrue(predicate.keySet().iterator().next().equals("$eq"));
        Assert.assertTrue(predicate.get("$eq").equals("mike"));

        fieldMap.clear();
        fieldMap = (Map) fields.get(1);
        Assert.assertEquals(1, fieldMap.size());
        Assert.assertTrue(fieldMap.keySet().iterator().next().equals("age"));
        predicate.clear();
        predicate = (Map) fieldMap.get("age");
        Assert.assertEquals(1, predicate.size());
        Assert.assertTrue(predicate.keySet().iterator().next().equals("$eq"));
        Assert.assertEquals(12, predicate.get("$eq"));
    }

}