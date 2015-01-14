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
import static org.hamcrest.Matchers.hasKey;
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

public class QueryValidatorTest {

    @Test
    @SuppressWarnings("unchecked")
    public void normalizeSingleFieldQuery() {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "name" : "mike" }
        query.put("name", "mike");
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "name" : { "$eq" : "mike" } } ]
        assertThat(normalizedQuery.size(), is(1));
        assertThat(normalizedQuery, hasKey("$and"));
        List<Object> fields = (ArrayList) normalizedQuery.get("$and");
        assertThat(fields.size(), is(1));
        Map<String, Object> fieldMap = (Map) fields.get(0);
        assertThat(fieldMap.size(), is(1));
        assertThat(fieldMap, hasKey("name"));
        Map<String, Object> predicate = (Map) fieldMap.get("name");
        assertThat(predicate.size(), is(1));
        assertThat(predicate, hasKey("$eq"));
        assertThat((String) predicate.get("$eq"), is("mike"));
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
        assertThat(normalizedQuery.size(), is(1));
        assertThat(normalizedQuery, hasKey("$and"));

        List<Object> fields = (ArrayList) normalizedQuery.get("$and");
        assertThat(fields.size(), is(2));
        Map<String, Object> fieldMap = (Map) fields.get(0);
        assertThat(fieldMap.size(), is(1));
        assertThat(fieldMap, hasKey("name"));
        Map<String, Object> predicate = (Map) fieldMap.get("name");
        assertThat(predicate.size(), is(1));
        assertThat(predicate, hasKey("$eq"));
        assertThat((String) predicate.get("$eq"), is("mike"));

        fieldMap.clear();
        fieldMap = (Map) fields.get(1);
        assertThat(fieldMap.size(), is(1));
        assertThat(fieldMap, hasKey("age"));
        predicate.clear();
        predicate = (Map) fieldMap.get("age");
        assertThat(predicate.size(), is(1));
        assertThat(predicate, hasKey("$eq"));
        assertThat((Integer) predicate.get("$eq"), is(12));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void checkForInvalidValues() {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "$and" : [ { "name" : { "$eq" : "mike" } },
        //                      { "age" : { "$eq" : 12 } } ] - (VALID)
        Map<String, Object> eqMike = new HashMap<String, Object>();
        eqMike.put("$eq", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", eqMike);
        Map<String, Object> eq12 = new HashMap<String, Object>();
        eq12.put("$eq", 12);
        Map<String, Object> age = new HashMap<String, Object>();
        age.put("age", eq12);
        query.put("$and", Arrays.<Object>asList(name, age));
        assertThat(QueryValidator.normaliseAndValidateQuery(query), is(notNullValue()));
        // query - { "$and" : [ { "name" : { "$eq" : "mike" } },
        //                      { "age" : { "$eq" : 12.0f } } ] - (INVALID)
        eq12.remove("$eq");
        eq12.put("$eq", 12.0f);
        assertThat(QueryValidator.normaliseAndValidateQuery(query), is(nullValue()));
        // query - { "$and" : [ { "name" : { "$eq" : "mike" } },
        //                      { "age" : { "$eq" : 12.345 } } ] - (VALID)
        eq12.remove("$eq");
        eq12.put("$eq", 12.345);
        assertThat(QueryValidator.normaliseAndValidateQuery(query), is(notNullValue()));
        // query - { "$and" : [ { "name" : { "$eq" : "mike" } },
        //                      { "age" : { "$eq" : 12l } } ] - (VALID)
        eq12.remove("$eq");
        eq12.put("$eq", 12l);
        assertThat(QueryValidator.normaliseAndValidateQuery(query), is(notNullValue()));
    }

}