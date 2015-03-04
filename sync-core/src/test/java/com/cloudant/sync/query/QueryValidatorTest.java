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
    public void normalizesMultipleEvenNOTs() {
        // query - { "pet" : { "$not" : { "$not" : { "$eq" : "cat" } } } }
        Map<String, Object> query = new HashMap<String, Object>();
        Map<String, Object> predicate = new HashMap<String, Object>(){{ put("$eq", "cat"); }};
        for (int i = 0; i < 2; i++) {
            final Map<String, Object> prevPredicate = predicate;
            predicate = new HashMap<String, Object>(){{ put("$not", prevPredicate); }};
        }
        query.put("pet", predicate);
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "pet" : { "$eq" : "cat" } } ] }
        Map<String, Object> c1op = new HashMap<String, Object>();
        c1op.put("$eq", "cat");
        Map<String, Object> c1 = new HashMap<String, Object>();
        c1.put("pet", c1op);
        Map<String, Object> expected = new LinkedHashMap<String, Object>();
        expected.put("$and", Arrays.<Object>asList(c1));
        assertThat(normalizedQuery, is(expected));
    }

    @Test
    public void normalizesSingleNe() {
        // query - { "pet" : { "$ne" : "cat" } }
        Map<String, Object> neCat = new HashMap<String, Object>();
        neCat.put("$ne", "cat");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", neCat);
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "pet" : { "$not" : { "$eq" : "cat" } } } ] }
        Map<String, Object> c1op = new HashMap<String, Object>();
        c1op.put("$eq", "cat");
        Map<String, Object> notC1op = new HashMap<String, Object>();
        notC1op.put("$not", c1op);
        Map<String, Object> c1 = new HashMap<String, Object>();
        c1.put("pet", notC1op);
        Map<String, Object> expected = new LinkedHashMap<String, Object>();
        expected.put("$and", Arrays.<Object>asList(c1));
        assertThat(normalizedQuery, is(expected));
    }

    @Test
    public void normalizesMultipleNOTsWithNe() {
        // query - { "pet" : { "$not" : { "$not" : { "$ne" : "cat" } } } }
        Map<String, Object> predicate = new HashMap<String, Object>(){{ put("$ne", "cat"); }};
        for (int i = 0; i < 2; i++) {
            final Map<String, Object> prevPredicate = predicate;
            predicate = new HashMap<String, Object>(){{ put("$not", prevPredicate); }};
        }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", predicate);
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "pet" : { "$not" : { "$eq" : "cat" } } } ] }
        Map<String, Object> c1op = new HashMap<String, Object>();
        c1op.put("$eq", "cat");
        Map<String, Object> notC1op = new HashMap<String, Object>();
        notC1op.put("$not", c1op);
        Map<String, Object> c1 = new HashMap<String, Object>();
        c1.put("pet", notC1op);
        Map<String, Object> expected = new LinkedHashMap<String, Object>();
        expected.put("$and", Arrays.<Object>asList(c1));
        assertThat(normalizedQuery, is(expected));
    }

    @Test
    public void normalizesMultipleOddNOTs() {
        // query - { "pet" : { "$not" : { "$not" : { "$not" : { "$eq" : "cat" } } } } }
        Map<String, Object> predicate = new HashMap<String, Object>(){{ put("$eq", "cat"); }};
        for (int i = 0; i < 3; i++) {
            final Map<String, Object> prevPredicate = predicate;
            predicate = new HashMap<String, Object>(){{ put("$not", prevPredicate); }};
        }
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("pet", predicate);
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "pet" : { "$not" : { "$eq" : "cat" } } } ] }
        Map<String, Object> c1op = new HashMap<String, Object>();
        c1op.put("$eq", "cat");
        Map<String, Object> notC1op = new HashMap<String, Object>();
        notC1op.put("$not", c1op);
        Map<String, Object> c1 = new HashMap<String, Object>();
        c1.put("pet", notC1op);
        Map<String, Object> expected = new LinkedHashMap<String, Object>();
        expected.put("$and", Arrays.<Object>asList(c1));
        assertThat(normalizedQuery, is(expected));
    }

    @Test
    public void normalizesMultiLevelQueryWithMultipleNOTs() {
        // query - { "$or" : [ { "name" : { "$eq" : "mike" } },
        //                     { "$and" : [ { "pet" : { "$not" : { "$not" :
        //                                  { "$not" : { "$eq" : "cat" } } } } },
        //                                  { "age" : { "$eq" : 12 } } ] } ] }
        Map<String, Object> eqMike = new HashMap<String, Object>();
        eqMike.put("$eq", "mike");
        Map<String, Object> nameOp = new HashMap<String, Object>();
        nameOp.put("name", eqMike);

        Map<String, Object> catOp = new HashMap<String, Object>(){{ put("$eq", "cat"); }};
        for (int i = 0; i < 3; i++) {
            final Map<String, Object> prevCatOp = catOp;
            catOp = new HashMap<String, Object>(){{ put("$not", prevCatOp); }};
        }
        Map<String, Object> petOp = new HashMap<String, Object>();
        petOp.put("pet", catOp);

        Map<String, Object> eq12 = new HashMap<String, Object>();
        eq12.put("$eq", 12);
        Map<String, Object> ageOp = new HashMap<String, Object>();
        ageOp.put("age", eq12);

        Map<String, Object> andOp = new HashMap<String, Object>();
        andOp.put("$and", Arrays.<Object>asList(petOp, ageOp));

        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(nameOp, andOp));
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$or" : [ { "name" : { "$eq" : "mike" } },
        //                                { "$and" : [ { "pet" : { "$not" : { "$eq" : "cat" } } },
        //                                             { "age" : { "$eq" : 12 } } ] } ] }
        Map<String, Object> c1op = new HashMap<String, Object>();
        c1op.put("$eq", "mike");
        Map<String, Object> c1 = new HashMap<String, Object>();
        c1.put("name", c1op);
        Map<String, Object> c2SubOp1 = new HashMap<String, Object>();
        c2SubOp1.put("$eq", "cat");
        Map<String, Object> c2NotSubOp1 = new HashMap<String, Object>();
        c2NotSubOp1.put("$not", c2SubOp1);
        Map<String, Object> c2Sub1 = new HashMap<String, Object>();
        c2Sub1.put("pet", c2NotSubOp1);
        Map<String, Object> c2SubOp2 = new HashMap<String, Object>();
        c2SubOp2.put("$eq", 12);
        Map<String, Object> c2Sub2 = new HashMap<String, Object>();
        c2Sub2.put("age", c2SubOp2);
        Map<String, Object> c2 = new HashMap<String, Object>();
        c2.put("$and", Arrays.<Object>asList(c2Sub1, c2Sub2));

        Map<String, Object> expected = new LinkedHashMap<String, Object>();
        expected.put("$or", Arrays.<Object>asList(c1, c2));
        assertThat(normalizedQuery, is(expected));
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

    @Test
    public void returnsNullForInvalidOperator() {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "name" : { "$blah" : "mike" } }
        Map<String, Object> blah = new HashMap<String, Object>();
        blah.put("$blah", "mike");
        query.put("name", blah);
        assertThat(QueryValidator.normaliseAndValidateQuery(query), is(nullValue()));
    }

    @Test
    public void returnsNullForInvalidOperatorWithNOT() {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "name" : { "$blah" : "mike" } }
        Map<String, Object> blah = new HashMap<String, Object>();
        blah.put("$blah", "mike");
        Map<String, Object> notBlah = new HashMap<String, Object>();
        notBlah.put("$not", blah);
        query.put("name", notBlah);
        assertThat(QueryValidator.normaliseAndValidateQuery(query), is(nullValue()));
    }

}
