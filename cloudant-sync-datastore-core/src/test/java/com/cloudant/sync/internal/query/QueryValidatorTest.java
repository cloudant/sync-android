/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2014 Cloudant, Inc. All rights reserved.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.cloudant.sync.internal.query.QueryValidator;
import com.cloudant.sync.query.QueryException;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class QueryValidatorTest {

    @Test
    public void normalizeSingleFieldQuery() throws QueryException {
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
    public void normalizeMultiFieldQuery() throws QueryException {
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
    public void doesNotChangeAlreadyNormalizedQuery() throws QueryException {
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
    public void normalizesMultipleEvenNOTs() throws QueryException {
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
    public void normalizesSingleNe() throws QueryException {
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
    public void normalizesMultipleNOTsWithNe() throws QueryException {
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
    public void normalizesMultipleOddNOTs() throws QueryException {
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
    public void normalizesMultiLevelQueryWithMultipleNOTs() throws QueryException {
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
    public void normalizesQueryWithIN() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "name" : { "$in" : ["mike", "fred"] } }
        Map<String, Object> inOp = new HashMap<String, Object>();
        inOp.put("$in", Arrays.<Object>asList("mike", "fred"));
        query.put("name", inOp);
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "name" : { "$in" : ["mike", "fred"] } } ] }
        Map<String, Object> c1op = new HashMap<String, Object>();
        c1op.put("$in", Arrays.<Object>asList("mike", "fred"));
        Map<String, Object> c1 = new HashMap<String, Object>();
        c1.put("name", c1op);
        Map<String, Object> expected = new LinkedHashMap<String, Object>();
        expected.put("$and", Arrays.<Object>asList(c1));
        assertThat(normalizedQuery, is(expected));
    }

    @Test
    public void normalizesQueryWithNIN() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "name" : { "$nin" : ["mike", "fred"] } }
        Map<String, Object> inOp = new HashMap<String, Object>();
        inOp.put("$nin", Arrays.<Object>asList("mike", "fred"));
        query.put("name", inOp);
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "name" : { "$not" : { "$in" : ["mike", "fred"] }}}]}
        Map<String, Object> c1op = new HashMap<String, Object>();
        c1op.put("$in", Arrays.<Object>asList("mike", "fred"));
        Map<String, Object> notC1op = new HashMap<String, Object>();
        notC1op.put("$not", c1op);
        Map<String, Object> c1 = new HashMap<String, Object>();
        c1.put("name", notC1op);
        Map<String, Object> expected = new LinkedHashMap<String, Object>();
        expected.put("$and", Arrays.<Object>asList(c1));
        assertThat(normalizedQuery, is(expected));
    }

    @Test
    public void normalizesQueryWithNOTNIN() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "name" : { "$not" : { "$nin" : ["mike", "fred"] } } }
        Map<String, Object> ninOp = new HashMap<String, Object>();
        ninOp.put("$nin", Arrays.<Object>asList("mike", "fred"));
        Map<String, Object> notNinOp = new HashMap<String, Object>();
        notNinOp.put("$not", ninOp);
        query.put("name", notNinOp);
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "name" : { "$in" : ["mike", "fred"] } } ] }
        Map<String, Object> c1op = new HashMap<String, Object>();
        c1op.put("$in", Arrays.<Object>asList("mike", "fred"));
        Map<String, Object> c1 = new HashMap<String, Object>();
        c1.put("name", c1op);
        Map<String, Object> expected = new LinkedHashMap<String, Object>();
        expected.put("$and", Arrays.<Object>asList(c1));
        assertThat(normalizedQuery, is(expected));
    }

    @Test
    public void normalizesQueryWithNOTNOTNIN() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "name" : { "$not" : { "$not" : { "$nin" : ["mike", "fred"] } } } }

        Map<String, Object> predicate = new HashMap<String, Object>()
        {
            { put("$nin", Arrays.<Object>asList("mike", "fred")); }
        };
        for (int i = 0; i < 2; i++) {
            final Map<String, Object> prevPredicate = predicate;
            predicate = new HashMap<String, Object>(){{ put("$not", prevPredicate); }};
        }
        query.put("name", predicate);
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "name" : { "$not" : { "$in" : ["mike", "fred"] }}} ] }
        Map<String, Object> c1op = new HashMap<String, Object>();
        c1op.put("$in", Arrays.<Object>asList("mike", "fred"));
        Map<String, Object> notC1op = new HashMap<String, Object>();
        notC1op.put("$not", c1op);
        Map<String, Object> c1 = new HashMap<String, Object>();
        c1.put("name", notC1op);
        Map<String, Object> expected = new LinkedHashMap<String, Object>();
        expected.put("$and", Arrays.<Object>asList(c1));
        assertThat(normalizedQuery, is(expected));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void checkForInvalidValues() throws QueryException {
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
        try {
            QueryValidator.normaliseAndValidateQuery(query);
            Assert.fail("Didn't catch expected QueryException");
        } catch (QueryException qe) {

        }
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

    @Test(expected = QueryException.class)
    public void returnsNullForInvalidOperator() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "name" : { "$blah" : "mike" } }
        Map<String, Object> blah = new HashMap<String, Object>();
        blah.put("$blah", "mike");
        query.put("name", blah);
        QueryValidator.normaliseAndValidateQuery(query);
    }

    @Test(expected = QueryException.class)
    public void returnsNullForInvalidOperatorWithNOT() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "name" : { "$not" : { "$blah" : "mike" } } }
        Map<String, Object> blah = new HashMap<String, Object>();
        blah.put("$blah", "mike");
        Map<String, Object> notBlah = new HashMap<String, Object>();
        notBlah.put("$not", blah);
        query.put("name", notBlah);
        QueryValidator.normaliseAndValidateQuery(query);
    }

    @Test(expected = QueryException.class)
    public void returnsNullForNOTWithoutOperator() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "name" : { "$not" : "mike" } }
        Map<String, Object> notMike = new HashMap<String, Object>();
        notMike.put("$not", "mike");
        query.put("name", notMike);
        QueryValidator.normaliseAndValidateQuery(query);
    }

    @Test(expected = QueryException.class)
    public void returnsNullForInvalidIN() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "name" : { "$in" : "mike" } }
        Map<String, Object> inMike = new HashMap<String, Object>();
        inMike.put("$in", "mike");
        query.put("name", inMike);
        QueryValidator.normaliseAndValidateQuery(query);
    }

    @Test
    public void normalizesSingleTextSearch() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "$text" : { "$search" : "foo bar baz" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "foo bar baz");
        query.put("$text", search);
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "$text" : { "$search" : "foo bar baz" } } ] }
        Map<String, Object> expected = new LinkedHashMap<String, Object>();
        Map<String, Object> exText = new HashMap<String, Object>();
        Map<String, Object> exSearch = new HashMap<String, Object>();
        exSearch.put("$search", "foo bar baz");
        exText.put("$text", exSearch);
        expected.put("$and", Arrays.<Object>asList(exText));
        assertThat(normalizedQuery, is(expected));
    }

    @Test
    public void normalizesMultiFieldQueryWithTextSearch() throws QueryException {
        Map<String, Object> query = new LinkedHashMap<String, Object>();
        // query - { "name" : "mike", "$text" : { "$search" : "foo bar baz" } }
        query.put("name", "mike");
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "foo bar baz");
        query.put("$text", search);
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "name" : { "$eq" : "mike" } },
        //                                 { "$text" : { "$search" : "foo bar baz" } } ] }
        Map<String, Object> expected = new LinkedHashMap<String, Object>();
        Map<String, Object> exName = new HashMap<String, Object>();
        Map<String, Object> exEq = new HashMap<String, Object>();
        exEq.put("$eq", "mike");
        exName.put("name", exEq);
        Map<String, Object> exText = new HashMap<String, Object>();
        Map<String, Object> exSearch = new HashMap<String, Object>();
        exSearch.put("$search", "foo bar baz");
        exText.put("$text", exSearch);
        expected.put("$and", Arrays.<Object>asList(exName, exText));
        assertThat(normalizedQuery, is(expected));
    }

    @Test(expected = QueryException.class)
    public void returnsNullForInvalidTextSearchContent() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "$text" : { "$search" : 12 } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", 12);
        query.put("$text", search);
        QueryValidator.normaliseAndValidateQuery(query);
    }

    @Test(expected = QueryException.class)
    public void returnsNullForMultipleTextSearchClauses() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "$or" : [ { "$text" : { "$search" : "foo bar" } },
        //                     { "$text" : { "$search" : "baz" } } ] }
        Map<String, Object> text1 = new HashMap<String, Object>();
        Map<String, Object> search1 = new HashMap<String, Object>();
        search1.put("$search", "foo bar");
        text1.put("$text", search1);
        Map<String, Object> text2 = new HashMap<String, Object>();
        Map<String, Object> search2 = new HashMap<String, Object>();
        search2.put("$search", "baz");
        text2.put("$text", search2);
        query.put("$or", Arrays.<Object>asList(text1, text2));
        QueryValidator.normaliseAndValidateQuery(query);
    }

    @Test(expected = QueryException.class)
    public void returnsNullForInvalidTextSearchOperator() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "$text" : { "$eq" : "foo bar baz" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$eq", "foo bar baz");
        query.put("$text", search);
        QueryValidator.normaliseAndValidateQuery(query);
    }

    @Test(expected = QueryException.class)
    public void returnsNullForTextOperatorWithoutSearchOperator() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "$text" : "foo bar baz" }
        query.put("$text", "foo bar baz");
        QueryValidator.normaliseAndValidateQuery(query);
    }

    @Test(expected = QueryException.class)
    public void returnsNullForSearchOperatorWithoutTextOperator() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "$search" : "foo bar baz" }
        query.put("$search", "foo bar baz");
        QueryValidator.normaliseAndValidateQuery(query);
    }

    @Test
    public void normalizesQueryWithMODOperator() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "age" : { "$mod" : [ 2, 1 ] } }
        Map<String, Object> mod = new HashMap<String, Object>();
        mod.put("$mod", Arrays.<Object>asList(2, 1));
        query.put("age", mod);
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "age" : { "$mod" : [ 2, 1 ] } } ] }
        Map<String, Object> expected = new LinkedHashMap<String, Object>();
        Map<String, Object> exMod = new HashMap<String, Object>();
        exMod.put("$mod", Arrays.<Object>asList(2, 1));
        Map<String, Object> exAge = new HashMap<String, Object>();
        exAge.put("age", exMod);
        expected.put("$and", Collections.<Object>singletonList(exAge));
        assertThat(normalizedQuery, is(expected));
    }

    @Test
    public void normalizesQueryWithMODOperatorAndNonWholeNumberValues() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "age" : { "$mod" : [ 2.6, 1.7 ] } }
        Map<String, Object> mod = new HashMap<String, Object>();
        mod.put("$mod", Arrays.<Object>asList(2.6, 1.7));
        query.put("age", mod);
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "age" : { "$mod" : [ 2, 1 ] } } ] }
        Map<String, Object> expected = new LinkedHashMap<String, Object>();
        Map<String, Object> exMod = new HashMap<String, Object>();
        exMod.put("$mod", Arrays.<Object>asList(2, 1));
        Map<String, Object> exAge = new HashMap<String, Object>();
        exAge.put("age", exMod);
        expected.put("$and", Collections.<Object>singletonList(exAge));
        assertThat(normalizedQuery, is(expected));
    }

    @Test
    public void normalizesQueryWithMODOperatorAndNegativeValues() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "age" : { "$mod" : [ -2.6, -1.7 ] } }
        Map<String, Object> mod = new HashMap<String, Object>();
        mod.put("$mod", Arrays.<Object>asList(-2.6, -1.7));
        query.put("age", mod);
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "age" : { "$mod" : [ -2, -1 ] } } ] }
        Map<String, Object> expected = new LinkedHashMap<String, Object>();
        Map<String, Object> exMod = new HashMap<String, Object>();
        exMod.put("$mod", Arrays.<Object>asList(-2, -1));
        Map<String, Object> exAge = new HashMap<String, Object>();
        exAge.put("age", exMod);
        expected.put("$and", Collections.<Object>singletonList(exAge));
        assertThat(normalizedQuery, is(expected));
    }

    @Test(expected = QueryException.class)
    public void returnsNullWhenMODArgumentNotArray() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "age" : { "$mod" : "blah" } }
        Map<String, Object> mod = new HashMap<String, Object>();
        mod.put("$mod", "blah");
        query.put("age", mod);
        QueryValidator.normaliseAndValidateQuery(query);
    }

    @Test(expected = QueryException.class)
    public void returnsNullWhenTooManyMODArguments() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "age" : { "$mod" : [ 2, 1, 0 ] } }
        Map<String, Object> mod = new HashMap<String, Object>();
        mod.put("$mod", Arrays.<Object>asList(2, 1, 0));
        query.put("age", mod);
        QueryValidator.normaliseAndValidateQuery(query);
    }

    @Test(expected = QueryException.class)
    public void returnsNullWhenMODArgumentIsInvalid() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "age" : { "$mod" : [ 2, "blah" ] } }
        Map<String, Object> mod = new HashMap<String, Object>();
        mod.put("$mod", Arrays.<Object>asList(2, "blah"));
        query.put("age", mod);
        QueryValidator.normaliseAndValidateQuery(query);
    }

    @Test(expected = QueryException.class)
    public void returnsNullWhenMODDivisorArgumentIs0() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "age" : { "$mod" : [ 0, 1 ] } }
        Map<String, Object> mod = new HashMap<String, Object>();
        mod.put("$mod", Arrays.<Object>asList(0, 1));
        query.put("age", mod);
        QueryValidator.normaliseAndValidateQuery(query);
    }

    @Test(expected = QueryException.class)
    public void returnsNullWhenMODDivisorArgumentIs0point0() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "age" : { "$mod" : [ 0.0, 1 ] } }
        Map<String, Object> mod = new HashMap<String, Object>();
        mod.put("$mod", Arrays.<Object>asList(0.0, 1));
        query.put("age", mod);
        QueryValidator.normaliseAndValidateQuery(query);
    }

    @Test(expected = QueryException.class)
    public void returnsNullWhenMODDivisorArgumentIsBetween0And1() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "age" : { "$mod" : [ 0.2, 1 ] } }
        Map<String, Object> mod = new HashMap<String, Object>();
        mod.put("$mod", Arrays.<Object>asList(0.2, 1));
        query.put("age", mod);
        QueryValidator.normaliseAndValidateQuery(query);
    }

    @Test(expected = QueryException.class)
    public void returnsNullWhenMODDivisorArgumentIsBetween0AndNegative1() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "age" : { "$mod" : [ -0.2, 1 ] } }
        Map<String, Object> mod = new HashMap<String, Object>();
        mod.put("$mod", Arrays.<Object>asList(-0.2, 1));
        query.put("age", mod);
        QueryValidator.normaliseAndValidateQuery(query);
    }

    @Test
    public void normalizesQueryWithSIZEOperator() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "pet" : { "$size" : 2 } }
        Map<String, Object> size = new HashMap<String, Object>();
        size.put("$size", 2);
        query.put("pet", size);
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "pet" : { "$size" : 2 } } ] }
        Map<String, Object> expected = new LinkedHashMap<String, Object>();
        Map<String, Object> exSize = new HashMap<String, Object>();
        exSize.put("$size", 2);
        Map<String, Object> exPet = new HashMap<String, Object>();
        exPet.put("pet", exSize);
        expected.put("$and", Collections.<Object>singletonList(exPet));
        assertThat(normalizedQuery, is(expected));
    }

    @Test(expected = QueryException.class)
    public void returnsNullForQueryWhenSIZEArgumentInvalid() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();
        // query - { "pet" : { "$size" : [2] } }
        Map<String, Object> size = new HashMap<String, Object>();
        size.put("$size", Collections.singletonList(2));
        query.put("pet", size);
        QueryValidator.normaliseAndValidateQuery(query);
    }

    @Test
    public void normalizesQueryWithEQOperator() throws QueryException {
        Map<String, Object> query = new HashMap<String, Object>();

        // query - { "animal" : true }
        query.put("animal", Boolean.TRUE);
        Map<String, Object> normalizedQuery = QueryValidator.normaliseAndValidateQuery(query);

        // normalized query - { "$and" : [ { "animal" : { "$eq" : true } } ] }
        Map<String, Object> c1op = new HashMap<String, Object>();
        c1op.put("$eq", Boolean.TRUE);
        Map<String, Object> c1 = new HashMap<String, Object>();
        c1.put("animal", c1op);
        Map<String, Object> expected = new LinkedHashMap<String, Object>();
        expected.put("$and", Arrays.<Object>asList(c1));
        assertThat(normalizedQuery, is(expected));
    }

}
