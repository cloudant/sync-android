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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

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
    public void compareEqNulls() {
        assertThat(compareEq(null, null), is(false));
        assertThat(compareEq(null, "mike"), is(false));
        assertThat(compareEq("mike", null), is(false));
        assertThat(compareEq(null, new Double(1.1)), is(false));
        assertThat(compareEq(new Double(1.1), null), is(false));
        assertThat(compareEq(null, new Long(1l)), is(false));
        assertThat(compareEq(new Long(1l), null), is(false));
        assertThat(compareEq(null, 1), is(false));
        assertThat(compareEq(1, null), is(false));
    }

    // Floats are not allowed.  Result should always be false.
    @Test
    @SuppressWarnings("UnnecessaryBoxing")
    public void compareEqFloats() {
        assertThat(compareEq(new Float(1.0f), new Float(1.0f)), is(false));
        assertThat(compareEq(new Float(1.0f), new Double(1.0)), is(false));
        assertThat(compareEq(new Double(1.0), new Float(1.0f)), is(false));
        assertThat(compareEq(new Float(1.0f), new Long(1l)), is(false));
        assertThat(compareEq(new Long(1l), new Float(1.0f)), is(false));
        assertThat(compareEq(new Float(1.0f), new Integer(1)), is(false));
        assertThat(compareEq(new Integer(1), new Float(1.0f)), is(false));

        assertThat(compareEq(new Float(1.0f), new Float(1.1f)), is(false));
        assertThat(compareEq(new Float(1.1f), new Float(1.0f)), is(false));
        assertThat(compareEq(new Float(1.1f), new Double(1.0)), is(false));
        assertThat(compareEq(new Double(1.0), new Float(1.1f)), is(false));
        assertThat(compareEq(new Float(1.1f), new Long(1l)), is(false));
        assertThat(compareEq(new Long(1l), new Float(1.1f)), is(false));
        assertThat(compareEq(new Float(1.1f), new Integer(1)), is(false));
        assertThat(compareEq(new Integer(1), new Float(1.1f)), is(false));
    }

    @Test
    public void compareEqStringToString() {
        assertThat(compareEq("mike", "mike"), is(true));
        assertThat(compareEq("mike", "Mike"), is(false));
    }

    @Test
    @SuppressWarnings("UnnecessaryBoxing")
    public void compareEqStringToNumber() {
        assertThat(compareEq("1", new Integer(1)), is(false));
        assertThat(compareEq(new Integer(1), "1"), is(false));
    }

    @Test
    @SuppressWarnings("UnnecessaryBoxing")
    public void compareEqDoubleToDouble() {
        assertThat(compareEq(new Double(1.0), new Double(1.0)), is(true));
        assertThat(compareEq(new Double(1.0), new Double(1.00)), is(true));
        assertThat(compareEq(new Double(1.0), new Double(1.1)), is(false));
        assertThat(compareEq(new Double(1.1), new Double(1.0)), is(false));
    }

    @Test
    @SuppressWarnings("UnnecessaryBoxing")
    public void compareEqDoubleToLong() {
        assertThat(compareEq(new Double(1.0), new Long(1l)), is(true));
        assertThat(compareEq(new Long(1l), new Double(1.0)), is(true));
        assertThat(compareEq(new Double(1.1), new Long(1l)), is(false));
        assertThat(compareEq(new Long(1l), new Double(1.1)), is(false));
    }

    @Test
    @SuppressWarnings("UnnecessaryBoxing")
    public void compareEqDoubleToInt() {
        assertThat(compareEq(new Double(1.0), new Integer(1)), is(true));
        assertThat(compareEq(new Integer(1), new Double(1.0)), is(true));
        assertThat(compareEq(new Double(1.1), new Integer(1)), is(false));
        assertThat(compareEq(new Integer(1), new Double(1.1)), is(false));
    }

    @Test
    @SuppressWarnings("UnnecessaryBoxing")
    public void compareEqLongToLong() {
        assertThat(compareEq(new Long(1l), new Long(1l)), is(true));
        assertThat(compareEq(new Long(1l), new Long(2l)), is(false));
        assertThat(compareEq(new Long(2l), new Long(1l)), is(false));
    }

    @Test
    @SuppressWarnings("UnnecessaryBoxing")
    public void compareEqLongToInt() {
        assertThat(compareEq(new Long(1l), new Integer(1)), is(true));
        assertThat(compareEq(new Integer(1), new Long(1l)), is(true));
        assertThat(compareEq(new Long(1l), new Integer(2)), is(false));
        assertThat(compareEq(new Integer(2), new Long(1l)), is(false));
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
        assertThat(matcher, is(notNullValue()));
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
        assertThat(matcher, is(notNullValue()));
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
        assertThat(matcher, is(notNullValue()));
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleImpliedEqMatch() {
        // Selector - { "name" : "mike" }
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("name", "mike");
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher, is(notNullValue()));
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleImpliedEqNoMatch() {
        // Selector - { "name" : "fred" }
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("name", "fred");
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher, is(notNullValue()));
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleImpliedEqNoMatchBadField() {
        // Selector - { "species" : "fred" }
        Map<String, Object> selector = new HashMap<String, Object>();
        selector.put("species", "fred");
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher, is(notNullValue()));
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
        assertThat(matcher, is(notNullValue()));
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
        assertThat(matcher, is(notNullValue()));
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleMatchesOnBadField() {
        // Selector - { "species" : { "$ne" : "fred" } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> ne = new HashMap<String, Object>();
        ne.put("$ne", "fred");
        selector.put("species", ne);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher, is(notNullValue()));
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleExistingMatch() {
        // Selector - { "name" : { "$exists" : true } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> exists = new HashMap<String, Object>();
        exists.put("$exists", true);
        selector.put("name", exists);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher, is(notNullValue()));
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleExistingNoMatch() {
        // Selector - { "name" : { "$exists" : false } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> exists = new HashMap<String, Object>();
        exists.put("$exists", false);
        selector.put("name", exists);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher, is(notNullValue()));
        assertThat(matcher.matches(rev), is(false));
    }

    @Test
    public void singleExistingMatchOnMissing() {
        // Selector - { "species" : { "$exists" : false } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> exists = new HashMap<String, Object>();
        exists.put("$exists", false);
        selector.put("species", exists);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher, is(notNullValue()));
        assertThat(matcher.matches(rev), is(true));
    }

    @Test
    public void singleExistingNoMatchOnMissing() {
        // Selector - { "species" : { "$exists" : true } }
        Map<String, Object> selector = new HashMap<String, Object>();
        Map<String, Object> exists = new HashMap<String, Object>();
        exists.put("$exists", true);
        selector.put("species", exists);
        selector = QueryValidator.normaliseAndValidateQuery(selector);
        UnindexedMatcher matcher = UnindexedMatcher.matcherWithSelector(selector);
        assertThat(matcher, is(notNullValue()));
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
        assertThat(matcher, is(notNullValue()));
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
        assertThat(matcher, is(notNullValue()));
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
        assertThat(matcher, is(notNullValue()));
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
        assertThat(matcher, is(notNullValue()));
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
        assertThat(matcher, is(notNullValue()));
        assertThat(matcher.matches(rev), is(false));
    }

}