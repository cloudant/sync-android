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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class QueryValidatorTest {

    @Test
    public void normalizeSingleFieldQuery() {
        Map<String, Object> query = new LinkedHashMap<String, Object>();
        // query - { "name" : "mike" }
        query.put("name", "mike");
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "name" : { "$eq" : "mike" } } ] }
        Map<String, Object> c1op = new HashMap<String, Object>();
        c1op.put("$eq", "mike");
        Map<String, Object> c1 = new HashMap<String, Object>();
        c1.put("name", c1op);
        Map<String, Object> expected = new LinkedHashMap<String, Object>();
        expected.put("$and", Arrays.<Object>asList(c1));
        assertThat(normalizedQuery, is(expected));
    }

    @Test
    public void normalizeMultiFieldQuery() {
        Map<String, Object> query = new LinkedHashMap<String, Object>();
        // query - { "name" : "mike", "pet" : "cat", "age", 12 }
        query.put("name", "mike");
        query.put("pet", "cat");
        query.put("age", 12);
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "name" : { "$eq" : "mike" } },
        //                                 { "pet" : { "$eq" : "cat" } },
        //                                 { "age" : { "$eq" : "12" } } ] }
        Map<String, Object> c1op = new HashMap<String, Object>();
        c1op.put("$eq", "mike");
        Map<String, Object> c1 = new HashMap<String, Object>();
        c1.put("name", c1op);
        Map<String, Object> c2op = new HashMap<String, Object>();
        c2op.put("$eq", "cat");
        Map<String, Object> c2 = new HashMap<String, Object>();
        c2.put("pet", c2op);
        Map<String, Object> c3op = new HashMap<String, Object>();
        c3op.put("$eq", 12);
        Map<String, Object> c3 = new HashMap<String, Object>();
        c3.put("age", c3op);
        Map<String, Object> expected = new LinkedHashMap<String, Object>();
        expected.put("$and", Arrays.<Object>asList(c1, c2, c3));
        assertThat(normalizedQuery, is(expected));
    }

    @Test
    public void doesNotChangeAlreadyNormalizedQuery() {
        // query - { "$and" : [ { "name" : { "$eq" : "mike" } },
        //                      { "pet" : { "$eq" : "cat" } },
        //                      { "age" : { "$eq" : "12" } } ] }
        Map<String, Object> c1op = new HashMap<String, Object>();
        c1op.put("$eq", "mike");
        Map<String, Object> c1 = new HashMap<String, Object>();
        c1.put("name", c1op);
        Map<String, Object> c2op = new HashMap<String, Object>();
        c2op.put("$eq", "cat");
        Map<String, Object> c2 = new HashMap<String, Object>();
        c2.put("pet", c2op);
        Map<String, Object> c3op = new HashMap<String, Object>();
        c3op.put("$eq", 12);
        Map<String, Object> c3 = new HashMap<String, Object>();
        c3.put("age", c3op);
        Map<String, Object> query = new LinkedHashMap<String, Object>();
        query.put("$and", Arrays.<Object>asList(c1, c2, c3));
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "name" : { "$eq" : "mike" } },
        //                                 { "pet" : { "$eq" : "cat" } },
        //                                 { "age" : { "$eq" : "12" } } ] }
        assertThat(normalizedQuery, is(query));
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