//  Copyright (c) 2015 Cloudant. All rights reserved.
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

import static com.cloudant.sync.query.UnindexedMatcher.compareEq;
import static com.cloudant.sync.query.UnindexedMatcher.compareGT;
import static com.cloudant.sync.query.UnindexedMatcher.compareGTE;
import static com.cloudant.sync.query.UnindexedMatcher.compareLT;
import static com.cloudant.sync.query.UnindexedMatcher.compareLTE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.cloudant.sync.datastore.DocumentBody;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.datastore.DocumentRevisionBuilder;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class UnindexedMatcherTest {

    DocumentRevision rev;

    @Before
    public void setUp() {
        // body content: { "name" : "mike",
        //                 "age" : 31,
        //                 "pets" : [ "white_cat", "black_cat" ],
        //                 "address" : { "number" : "1", "road" : "infinite loop" }
        //               }
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 31);
        bodyMap.put("pets", Arrays.asList("white_cat", "black_cat"));
        Map<String, String> addressDetail = new HashMap<String, String>();
        addressDetail.put("number", "1");
        addressDetail.put("road", "infinite loop");
        bodyMap.put("address", addressDetail);
        DocumentBody body = DocumentBodyFactory.create(bodyMap);
        DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
        builder.setDocId("dsfsdfdfs");
        builder.setRevId("1-qweqeqwewqe");
        builder.setBody(body);
        rev = builder.build();
    }

    @Test
    @SuppressWarnings("UnnecessaryBoxing")
    public void compareNulls() {
        assertThat(compareEq(null, null), is(false));
        assertThat(compareEq(null, "mike"), is(false));
        assertThat(compareEq("mike", null), is(false));
        assertThat(compareEq(null, 1), is(false));
        assertThat(compareEq(1, null), is(false));

        assertThat(compareLT(null, null), is(false));
        assertThat(compareLT(null, "mike"), is(false));
        assertThat(compareLT("mike", null), is(false));
        assertThat(compareLT(null, 1), is(false));
        assertThat(compareLT(1, null), is(false));

        assertThat(compareLTE(null, null), is(false));
        assertThat(compareLTE(null, "mike"), is(false));
        assertThat(compareLTE("mike", null), is(false));
        assertThat(compareLTE(null, 1), is(false));
        assertThat(compareLTE(1, null), is(false));

        assertThat(compareGT(null, null), is(false));
        assertThat(compareGT(null, "mike"), is(false));
        assertThat(compareGT("mike", null), is(false));
        assertThat(compareGT(null, 1), is(false));
        assertThat(compareGT(1, null), is(false));

        assertThat(compareGTE(null, null), is(false));
        assertThat(compareGTE(null, "mike"), is(false));
        assertThat(compareGTE("mike", null), is(false));
        assertThat(compareGTE(null, 1), is(false));
        assertThat(compareGTE(1, null), is(false));
    }

    @Test
    public void compareStringToString() {
        assertThat(compareEq("mike", "mike"), is(true));
        assertThat(compareEq("mike", "Mike"), is(false));

        assertThat(compareLT("mike", "mike"), is(false));
        assertThat(compareLT("mike", "fred"), is(false));
        assertThat(compareLT("fred", "mike"), is(true));

        assertThat(compareLTE("mike", "mike"), is(true));
        assertThat(compareLTE("mike", "fred"), is(false));
        assertThat(compareLTE("fred", "mike"), is(true));

        assertThat(compareGT("mike", "mike"), is(false));
        assertThat(compareGT("mike", "fred"), is(true));
        assertThat(compareGT("fred", "mike"), is(false));

        assertThat(compareGTE("mike", "mike"), is(true));
        assertThat(compareGTE("mike", "fred"), is(true));
        assertThat(compareGTE("fred", "mike"), is(false));
    }

    @Test
    @SuppressWarnings("UnnecessaryBoxing")
    public void compareStringToNumber() {
        assertThat(compareEq("1", new Integer(1)), is(false));
        assertThat(compareEq(new Integer(1), "1"), is(false));

        assertThat(compareLT("1", new Integer(1)), is(false));
        assertThat(compareLT(new Integer(1), "1"), is(true));

        assertThat(compareLTE("1", new Integer(1)), is(false));
        assertThat(compareLTE(new Integer(1), "1"), is(true));

        assertThat(compareGT("1", new Integer(1)), is(true));
        assertThat(compareGT(new Integer(1), "1"), is(false));

        assertThat(compareGTE("1", new Integer(1)), is(true));
        assertThat(compareGTE(new Integer(1), "1"), is(false));
    }

    @Test
    @SuppressWarnings("UnnecessaryBoxing")
    public void compareDoubleToDouble() {
        assertThat(compareEq(new Double(1.0), new Double(1.0)), is(true));
        assertThat(compareEq(new Double(1.0), new Double(1.00)), is(true));
        assertThat(compareEq(new Double(1.0), new Double(1.1)), is(false));
        assertThat(compareEq(new Double(1.1), new Double(1.0)), is(false));

        assertThat(compareLT(new Double(1.0), new Double(1.0)), is(false));
        assertThat(compareLT(new Double(1.0), new Double(1.00)), is(false));
        assertThat(compareLT(new Double(1.0), new Double(1.1)), is(true));
        assertThat(compareLT(new Double(1.1), new Double(1.0)), is(false));

        assertThat(compareLTE(new Double(1.0), new Double(1.0)), is(true));
        assertThat(compareLTE(new Double(1.0), new Double(1.00)), is(true));
        assertThat(compareLTE(new Double(1.0), new Double(1.1)), is(true));
        assertThat(compareLTE(new Double(1.1), new Double(1.0)), is(false));

        assertThat(compareGT(new Double(1.0), new Double(1.0)), is(false));
        assertThat(compareGT(new Double(1.0), new Double(1.00)), is(false));
        assertThat(compareGT(new Double(1.0), new Double(1.1)), is(false));
        assertThat(compareGT(new Double(1.1), new Double(1.0)), is(true));

        assertThat(compareGTE(new Double(1.0), new Double(1.0)), is(true));
        assertThat(compareGTE(new Double(1.0), new Double(1.00)), is(true));
        assertThat(compareGTE(new Double(1.0), new Double(1.1)), is(false));
        assertThat(compareGTE(new Double(1.1), new Double(1.0)), is(true));
    }

    @Test
    @SuppressWarnings("UnnecessaryBoxing")
    public void compareEqDoubleToLong() {
        assertThat(compareEq(new Double(1.0), new Long(1l)), is(true));
        assertThat(compareEq(new Long(1l), new Double(1.0)), is(true));
        assertThat(compareEq(new Double(1.1), new Long(1l)), is(false));
        assertThat(compareEq(new Long(1l), new Double(1.1)), is(false));

        assertThat(compareLT(new Double(1.0), new Long(1l)), is(false));
        assertThat(compareLT(new Long(1l), new Double(1.0)), is(false));
        assertThat(compareLT(new Double(1.1), new Long(1l)), is(false));
        assertThat(compareLT(new Long(2l), new Double(1.1)), is(false));
        assertThat(compareLT(new Long(1l), new Double(1.1)), is(true));
        assertThat(compareLT(new Double(1.1), new Long(2l)), is(true));

        assertThat(compareLTE(new Double(1.0), new Long(1l)), is(true));
        assertThat(compareLTE(new Long(1l), new Double(1.0)), is(true));
        assertThat(compareLTE(new Double(1.1), new Long(1l)), is(false));
        assertThat(compareLTE(new Long(2l), new Double(1.1)), is(false));
        assertThat(compareLTE(new Long(1l), new Double(1.1)), is(true));
        assertThat(compareLTE(new Double(1.1), new Long(2l)), is(true));

        assertThat(compareGT(new Double(1.0), new Long(1l)), is(false));
        assertThat(compareGT(new Long(1l), new Double(1.0)), is(false));
        assertThat(compareGT(new Double(1.1), new Long(1l)), is(true));
        assertThat(compareGT(new Long(2l), new Double(1.1)), is(true));
        assertThat(compareGT(new Long(1l), new Double(1.1)), is(false));
        assertThat(compareGT(new Double(1.1), new Long(2l)), is(false));

        assertThat(compareGTE(new Double(1.0), new Long(1l)), is(true));
        assertThat(compareGTE(new Long(1l), new Double(1.0)), is(true));
        assertThat(compareGTE(new Double(1.1), new Long(1l)), is(true));
        assertThat(compareGTE(new Long(2l), new Double(1.1)), is(true));
        assertThat(compareGTE(new Long(1l), new Double(1.1)), is(false));
        assertThat(compareGTE(new Double(1.1), new Long(2l)), is(false));

    }

    @Test
    @SuppressWarnings("UnnecessaryBoxing")
    public void compareEqDoubleToInt() {
        assertThat(compareEq(new Double(1.0), new Integer(1)), is(true));
        assertThat(compareEq(new Integer(1), new Double(1.0)), is(true));
        assertThat(compareEq(new Double(1.1), new Integer(1)), is(false));
        assertThat(compareEq(new Integer(1), new Double(1.1)), is(false));

        assertThat(compareLT(new Double(1.0), new Integer(1)), is(false));
        assertThat(compareLT(new Integer(1), new Double(1.0)), is(false));
        assertThat(compareLT(new Double(1.1), new Integer(1)), is(false));
        assertThat(compareLT(new Integer(2), new Double(1.1)), is(false));
        assertThat(compareLT(new Integer(1), new Double(1.1)), is(true));
        assertThat(compareLT(new Double(1.1), new Integer(2)), is(true));

        assertThat(compareLTE(new Double(1.0), new Integer(1)), is(true));
        assertThat(compareLTE(new Integer(1), new Double(1.0)), is(true));
        assertThat(compareLTE(new Double(1.1), new Integer(1)), is(false));
        assertThat(compareLTE(new Integer(2), new Double(1.1)), is(false));
        assertThat(compareLTE(new Integer(1), new Double(1.1)), is(true));
        assertThat(compareLTE(new Double(1.1), new Integer(2)), is(true));

        assertThat(compareGT(new Double(1.0), new Integer(1)), is(false));
        assertThat(compareGT(new Integer(1), new Double(1.0)), is(false));
        assertThat(compareGT(new Double(1.1), new Integer(1)), is(true));
        assertThat(compareGT(new Integer(2), new Double(1.1)), is(true));
        assertThat(compareGT(new Integer(1), new Double(1.1)), is(false));
        assertThat(compareGT(new Double(1.1), new Integer(2)), is(false));

        assertThat(compareGTE(new Double(1.0), new Integer(1)), is(true));
        assertThat(compareGTE(new Integer(1), new Double(1.0)), is(true));
        assertThat(compareGTE(new Double(1.1), new Integer(1)), is(true));
        assertThat(compareGTE(new Integer(2), new Double(1.1)), is(true));
        assertThat(compareGTE(new Integer(1), new Double(1.1)), is(false));
        assertThat(compareGTE(new Double(1.1), new Integer(2)), is(false));
    }

    @Test
    @SuppressWarnings("UnnecessaryBoxing")
    public void compareEqLongToLong() {
        assertThat(compareEq(new Long(1l), new Long(1l)), is(true));
        assertThat(compareEq(new Long(1l), new Long(2l)), is(false));
        assertThat(compareEq(new Long(2l), new Long(1l)), is(false));

        assertThat(compareLT(new Long(1l), new Long(1l)), is(false));
        assertThat(compareLT(new Long(1l), new Long(2l)), is(true));
        assertThat(compareLT(new Long(2l), new Long(1l)), is(false));

        assertThat(compareLTE(new Long(1l), new Long(1l)), is(true));
        assertThat(compareLTE(new Long(1l), new Long(2l)), is(true));
        assertThat(compareLTE(new Long(2l), new Long(1l)), is(false));

        assertThat(compareGT(new Long(1l), new Long(1l)), is(false));
        assertThat(compareGT(new Long(1l), new Long(2l)), is(false));
        assertThat(compareGT(new Long(2l), new Long(1l)), is(true));

        assertThat(compareGTE(new Long(1l), new Long(1l)), is(true));
        assertThat(compareGTE(new Long(1l), new Long(2l)), is(false));
        assertThat(compareGTE(new Long(2l), new Long(1l)), is(true));
    }

    @Test
    @SuppressWarnings("UnnecessaryBoxing")
    public void compareEqLongToInt() {
        assertThat(compareEq(new Long(1l), new Integer(1)), is(true));
        assertThat(compareEq(new Integer(1), new Long(1l)), is(true));
        assertThat(compareEq(new Long(1l), new Integer(2)), is(false));
        assertThat(compareEq(new Integer(2), new Long(1l)), is(false));

        assertThat(compareLT(new Long(1l), new Integer(1)), is(false));
        assertThat(compareLT(new Integer(1), new Long(1l)), is(false));
        assertThat(compareLT(new Long(2l), new Integer(1)), is(false));
        assertThat(compareLT(new Integer(2), new Long(1l)), is(false));
        assertThat(compareLT(new Integer(1), new Long(2l)), is(true));
        assertThat(compareLT(new Long(1l), new Integer(2)), is(true));

        assertThat(compareLTE(new Long(1l), new Integer(1)), is(true));
        assertThat(compareLTE(new Integer(1), new Long(1l)), is(true));
        assertThat(compareLTE(new Long(2l), new Integer(1)), is(false));
        assertThat(compareLTE(new Integer(2), new Long(1l)), is(false));
        assertThat(compareLTE(new Integer(1), new Long(2l)), is(true));
        assertThat(compareLTE(new Long(1l), new Integer(2)), is(true));

        assertThat(compareGT(new Long(1l), new Integer(1)), is(false));
        assertThat(compareGT(new Integer(1), new Long(1l)), is(false));
        assertThat(compareGT(new Long(2l), new Integer(1)), is(true));
        assertThat(compareGT(new Integer(2), new Long(1l)), is(true));
        assertThat(compareGT(new Integer(1), new Long(2l)), is(false));
        assertThat(compareGT(new Long(1l), new Integer(2)), is(false));

        assertThat(compareGTE(new Long(1l), new Integer(1)), is(true));
        assertThat(compareGTE(new Integer(1), new Long(1l)), is(true));
        assertThat(compareGTE(new Long(2l), new Integer(1)), is(true));
        assertThat(compareGTE(new Integer(2), new Long(1l)), is(true));
        assertThat(compareGTE(new Integer(1), new Long(2l)), is(false));
        assertThat(compareGTE(new Long(1l), new Integer(2)), is(false));
    }

    @Test
    public void singleEqMatch() {
        // Selector - { "name" : { "$eq" : "mike" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> eq = new HashMap<String, Object>();
        eq.put("$eq", "mike");
        selector.put("name", eq);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleEqNoMatch() {
        // Selector - { "name" : { "$eq" : "fred" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> eq = new HashMap<String, Object>();
        eq.put("$eq", "fred");
        selector.put("name", eq);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleEqNoMatchBadField() {
        // Selector - { "species" : { "$eq" : "fred" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> eq = new HashMap<String, Object>();
        eq.put("$eq", "fred");
        selector.put("species", eq);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleImpliedEqMatch() {
        // Selector - { "name" : "mike" }
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("name", "mike");
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleImpliedEqNoMatch() {
        // Selector - { "name" : "fred" }
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("name", "fred");
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleImpliedEqNoMatchBadField() {
        // Selector - { "species" : "fred" }
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("species", "fred");
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleNeMatch() {
        // Selector - { "name" : { "$ne" : "fred" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> ne = new HashMap<String, Object>();
        ne.put("$ne", "fred");
        selector.put("name", ne);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleNeNoMatch() {
        // Selector - { "name" : { "$ne" : "mike" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> ne = new HashMap<String, Object>();
        ne.put("$ne", "mike");
        selector.put("name", ne);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleNeMatchesOnBadField() {
        // Selector - { "species" : { "$ne" : "fred" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> ne = new HashMap<String, Object>();
        ne.put("$ne", "fred");
        selector.put("species", ne);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleGtStringMatch() {
        // Selector - { "name" : { "$gt" : "andy" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gt", "andy");
        selector.put("name", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleGtIntMatch() {
        // Selector - { "age" : { "$gt" : 12 } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gt", 12);
        selector.put("age", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleGtStringNoMatch() {
        // Selector - { "name" : { "$gt" : "robert" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gt", "robert");
        selector.put("name", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleGtIntNoMatch() {
        // Selector - { "age" : { "$gt" : 45 } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gt", 45);
        selector.put("age", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleGtNoMatchBadField() {
        // Selector - { "species" : { "$gt" : "fred" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gt", "fred");
        selector.put("species", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleGteStringMatch() {
        // Selector - { "name" : { "$gte" : "andy" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gte", "andy");
        selector.put("name", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleGteStringMatchEq() {
        // Selector - { "name" : { "$gte" : "mike" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gte", "mike");
        selector.put("name", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleGteIntMatch() {
        // Selector - { "age" : { "$gte" : 12 } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gte", 12);
        selector.put("age", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleGteIntMatchEq() {
        // Selector - { "age" : { "$gte" : 31 } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gte", 31);
        selector.put("age", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleGteStringNoMatch() {
        // Selector - { "name" : { "$gte" : "robert" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gte", "robert");
        selector.put("name", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleGteIntNoMatch() {
        // Selector - { "age" : { "$gte" : 45 } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gte", 45);
        selector.put("age", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleGteNoMatchBadField() {
        // Selector - { "species" : { "$gte" : "fred" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$gte", "fred");
        selector.put("species", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleLtStringMatch() {
        // Selector - { "name" : { "$lt" : "robert" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lt", "robert");
        selector.put("name", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleLtIntMatch() {
        // Selector - { "age" : { "$lt" : 45 } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lt", 45);
        selector.put("age", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleLtStringNoMatch() {
        // Selector - { "name" : { "$lt" : "andy" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lt", "andy");
        selector.put("name", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleLtIntNoMatch() {
        // Selector - { "age" : { "$lt" : 12 } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lt", 12);
        selector.put("age", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleLtNoMatchBadField() {
        // Selector - { "species" : { "$lt" : "fred" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lt", "fred");
        selector.put("species", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleLteStringMatch() {
        // Selector - { "name" : { "$lte" : "robert" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lte", "robert");
        selector.put("name", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleLteStringMatchEq() {
        // Selector - { "name" : { "$lte" : "mike" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lte", "mike");
        selector.put("name", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleLteIntMatch() {
        // Selector - { "age" : { "$lte" : 45 } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lte", 45);
        selector.put("age", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleLteIntMatchEq() {
        // Selector - { "age" : { "$lte" : 31 } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lte", 31);
        selector.put("age", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleLteStringNoMatch() {
        // Selector - { "name" : { "$lte" : "andy" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lte", "andy");
        selector.put("name", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleLteIntNoMatch() {
        // Selector - { "age" : { "$lte" : 12 } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lte", 12);
        selector.put("age", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleLteNoMatchBadField() {
        // Selector - { "species" : { "$lte" : "fred" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$lte", "fred");
        selector.put("species", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleExistsMatch() {
        // Selector - { "name" : { "$exists" : true } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> exists = new HashMap<String, Object>();
        exists.put("$exists", true);
        selector.put("name", exists);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleExistsNoMatch() {
        // Selector - { "name" : { "$exists" : false } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> exists = new HashMap<String, Object>();
        exists.put("$exists", false);
        selector.put("name", exists);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleExistsMatchOnMissing() {
        // Selector - { "species" : { "$exists" : false } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> exists = new HashMap<String, Object>();
        exists.put("$exists", false);
        selector.put("species", exists);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleExistsNoMatchOnMissing() {
        // Selector - { "species" : { "$exists" : true } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> exists = new HashMap<String, Object>();
        exists.put("$exists", true);
        selector.put("species", exists);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void compoundAndMatchAll() {
        // Selector - { "$and" : [ { "name" : { "$eq" : "mike" } }, { "age" : { "$eq" : 31 } } ] }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> eqName = new HashMap<String, Object>();
        eqName.put("$eq", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", eqName);
        Map<String, Object> eqAge = new HashMap<String, Object>();
        eqAge.put("$eq", 31);
        Map<String, Object> age = new HashMap<String, Object>();
        age.put("age", eqAge);
        selector.put("$and", Arrays.<Object>asList(name, age));
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void compoundAndNoMatchSome() {
        // Selector - { "$and" : [ { "name" : { "$eq" : "mike" } }, { "age" : { "$eq" : 12 } } ] }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> eqName = new HashMap<String, Object>();
        eqName.put("$eq", "mike");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", eqName);
        Map<String, Object> eqAge = new HashMap<String, Object>();
        eqAge.put("$eq", 12);
        Map<String, Object> age = new HashMap<String, Object>();
        age.put("age", eqAge);
        selector.put("$and", Arrays.<Object>asList(name, age));
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void compoundAndNoMatchAny() {
        // Selector - { "$and" : [ { "name" : { "$eq" : "fred" } }, { "age" : { "$eq" : 12 } } ] }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> eqName = new HashMap<String, Object>();
        eqName.put("$eq", "fred");
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", eqName);
        Map<String, Object> eqAge = new HashMap<String, Object>();
        eqAge.put("$eq", 12);
        Map<String, Object> age = new HashMap<String, Object>();
        age.put("age", eqAge);
        selector.put("$and", Arrays.<Object>asList(name, age));
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void compoundImplicitAndMatch() {
        // Selector - { "name" : { "$eq" : "mike" }, "age" : { "$eq" : 31 } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> eqName = new HashMap<String, Object>();
        eqName.put("$eq", "mike");
        Map<String, Object> eqAge = new HashMap<String, Object>();
        eqAge.put("$eq", 31);
        selector.put("name", eqName);
        selector.put("age", eqAge);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void compoundImplicitAndNoMatch() {
        // Selector - { "name" : { "$eq" : "mike" }, "age" : { "$eq" : 12 } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> eqName = new HashMap<String, Object>();
        eqName.put("$eq", "mike");
        Map<String, Object> eqAge = new HashMap<String, Object>();
        eqAge.put("$eq", 12);
        selector.put("name", eqName);
        selector.put("age", eqAge);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void orMatchAllFields() {
        // Selector - { "$or" : [ { "name" : { "$eq" : "mike" } }, { "age" : { "$eq" : 31 } } ] }
        Map<String, Object> c1op = new HashMap<String, Object>();
        c1op.put("$eq", "mike");
        Map<String, Object> c1 = new HashMap<String, Object>();
        c1.put("name", c1op);
        Map<String, Object> c2op = new HashMap<String, Object>();
        c2op.put("$eq", 31);
        Map<String, Object> c2 = new HashMap<String, Object>();
        c2.put("age", c2op);
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("$or", Arrays.<Object>asList(c1, c2));
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void orMatchOnOneField() {
        // Selector - { "$or" : [ { "name" : { "$eq" : "mike" } }, { "age" : { "$eq" : 12 } } ] }
        Map<String, Object> c1op = new HashMap<String, Object>();
        c1op.put("$eq", "mike");
        Map<String, Object> c1 = new HashMap<String, Object>();
        c1.put("name", c1op);
        Map<String, Object> c2op = new HashMap<String, Object>();
        c2op.put("$eq", 12);
        Map<String, Object> c2 = new HashMap<String, Object>();
        c2.put("age", c2op);
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("$or", Arrays.<Object>asList(c1, c2));
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void orNoMatch() {
        // Selector - { "$or" : [ { "name" : { "$eq" : "fred" } }, { "age" : { "$eq" : 12 } } ] }
        Map<String, Object> c1op = new HashMap<String, Object>();
        c1op.put("$eq", "fred");
        Map<String, Object> c1 = new HashMap<String, Object>();
        c1.put("name", c1op);
        Map<String, Object> c2op = new HashMap<String, Object>();
        c2op.put("$eq", 12);
        Map<String, Object> c2 = new HashMap<String, Object>();
        c2.put("age", c2op);
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("$or", Arrays.<Object>asList(c1, c2));
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    // ($not) - We can be fairly simple here as we know that the internal is that $not just negates.
    // ($ne)  - $ne translates to $not..$eq

    @Test
     public void noMatchNotEq() {
        // Selector - { "name" : { "$not" : { "$eq" : "mike" } } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$eq", "mike");
        Map<String, Object> not = new HashMap<String, Object>();
        not.put("$not", op);
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("name", not);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void noMatchNe() {
        // Selector - { "name" : { "$ne" : "mike" } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$ne", "mike");
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("name", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void noMatchNotNe() {
        // Selector - { "name" : { "$not" : { "$ne" : "fred" } } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$ne", "fred");
        Map<String, Object> not = new HashMap<String, Object>();
        not.put("$not", op);
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("name", not);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void matchNotEq() {
        // Selector - { "name" : { "$not" : { "$eq" : "fred" } } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$eq", "fred");
        Map<String, Object> not = new HashMap<String, Object>();
        not.put("$not", op);
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("name", not);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void matchNe() {
        // Selector - { "name" : { "$ne" : "fred" } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$ne", "fred");
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("name", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void matchNotNe() {
        // Selector - { "name" : { "$not" : { "$ne" : "mike" } } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$ne", "mike");
        Map<String, Object> not = new HashMap<String, Object>();
        not.put("$not", op);
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("name", not);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void matchNotEqBadField() {
        // Selector - { "species" : { "$not" : { "$eq" : "fred" } } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$eq", "fred");
        Map<String, Object> not = new HashMap<String, Object>();
        not.put("$not", op);
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("species", not);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void matchNeBadField() {
        // Selector - { "species" : { "$ne" : "fred" } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$ne", "fred");
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("species", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void matchOnArrayFields() {
        // Selector - { "pets" : "white_cat" }
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("pets", "white_cat");
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void noMatchGoodItemWithNot() {
        // Selector - { "pets" : { "$not" : { "$eq" : "white_cat" } } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$eq", "white_cat");
        Map<String, Object> not = new HashMap<String, Object>();
        not.put("$not", op);
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("pets", not);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void noMatchGoodItemWithNe() {
        // Selector - { "pets" : { "$ne" : "white_cat" } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$ne", "white_cat");
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("pets", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void noMatchOnBadItem() {
        // Selector - { "pets" : "tabby_cat" }
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("pets", "tabby_cat");
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void matchBadItemWithNot() {
        // Selector - { "pets" : { "$not" : { "$eq" : "tabby_cat" } } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$eq", "tabby_cat");
        Map<String, Object> not = new HashMap<String, Object>();
        not.put("$not", op);
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("pets", not);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void matchBadItemWithNe() {
        // Selector - { "pets" : { "$ne" : "tabby_cat" } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$ne", "tabby_cat");
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("pets", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void matchOnArrayUsingIn() {
        // Selector - { "pets" : { "$in" : [ "white_cat", "tabby_cat" ] } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$in", Arrays.<Object>asList("white_cat", "tabby_cat"));
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("pets", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void noMatchOnArrayUsingIn() {
        // Selector - { "pets" : { "$in" : [ "grey_cat", "tabby_cat" ] } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$in", Arrays.<Object>asList("grey_cat", "tabby_cat"));
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("pets", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void matchOnNonArrayUsingIn() {
        // Selector - { "name" : { "$in" : [ "mike", "fred" ] } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$in", Arrays.<Object>asList("mike", "fred"));
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("name", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void noMatchOnNonArrayUsingIn() {
        // Selector - { "name" : { "$in" : [ "john", "fred" ] } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$in", Arrays.<Object>asList("john", "fred"));
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("name", op);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void matchOnArrayUsingNotIn() {
        // Selector - { "pets" : { "$not" : { "$in" : [ "grey_cat", "tabby_cat" ] } } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$in", Arrays.<Object>asList("grey_cat", "tabby_cat"));
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", op);
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("pets", notOp);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void noMatchOnArrayUsingNotIn() {
        // Selector - { "pets" : { "$not" : { "$in" : [ "white_cat", "tabby_cat" ] } } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$in", Arrays.<Object>asList("white_cat", "tabby_cat"));
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", op);
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("pets", notOp);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void matchOnNonArrayUsingNotIn() {
        // Selector - { "name" : { "$not" : { "$in" : [ "john", "fred" ] } } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$in", Arrays.<Object>asList("john", "fred"));
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", op);
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("name", notOp);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void noMatchOnNonArrayUsingNotIn() {
        // Selector - { "name" : { "$not" : { "$in" : [ "mike", "fred" ] } } }
        Map<String, Object> op = new HashMap<String, Object>();
        op.put("$in", Arrays.<Object>asList("mike", "fred"));
        Map<String, Object> notOp = new HashMap<String, Object>();
        notOp.put("$not", op);
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("name", notOp);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void matchOnDottedFields() {
        // Selector - { "address.number" : "1" }
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("address.number", "1");
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void noMatchOnDottedFields() {
        // Selector - { "address.number" : "2" }
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("address.number", "2");
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher.matches(rev), is(false));
    }

}
