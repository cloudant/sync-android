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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexCreatorTest extends AbstractIndexTestBase {

    @Test
    public void emptyIndexList() {
        Map<String, Map<String, Object>> indexes = im.listIndexes();
        assertThat(indexes, is(notNullValue()));
        assertThat(indexes.isEmpty(), is(true));
    }

    @Test
    public void preconditionsToCreatingIndexes() throws QueryException {
        // doesn't create an index on null fields
        try {
            im.ensureIndexed(null, "basic");
            Assert.fail("Expected ensureIndexed to throw a QueryException");
        } catch (QueryException qe) {
            ;
        }

        List<FieldSort> fieldNames = null;
        // doesn't create an index on no fields
        try {
            fieldNames = new ArrayList<FieldSort>();
            im.ensureIndexed(fieldNames, "basic");
            Assert.fail("Expected ensureIndexed to throw a QueryException");
        } catch (QueryException qe) {
            ;
        }

        // doesn't create an index without a name
        try {
            im.ensureIndexed(fieldNames, "");
            Assert.fail("Expected ensureIndexed to throw a QueryException");
        } catch (QueryException qe) {
            ;
        }

        // doesn't create an index on null index type
        try {
            im.ensureIndexed(fieldNames, "basic", null);
            Assert.fail("Expected ensureIndexed to throw a QueryException");
        } catch (QueryException qe) {
            ;
        }

        // doesn't create an index if duplicate fields
        try {
            fieldNames = Arrays.<FieldSort>asList(new FieldSort("age"), new FieldSort("pet"), new
                    FieldSort("age"));
            im.ensureIndexed(fieldNames, "basic");
            Assert.fail("Expected ensureIndexed to throw a QueryException");
        } catch (QueryException qe) {
            ;
        }

    }

    @Test
    public void createIndexOverOneField() throws QueryException {
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name")), "basic");
        assertThat(indexName, is("basic"));

        Map<String, Map<String, Object>> indexes = im.listIndexes();
        assertThat(indexes, hasKey("basic"));

        @SuppressWarnings("unchecked")
        Map<String, Object> index = indexes.get("basic");
        @SuppressWarnings("unchecked")
        List<String> fields = (List<String>) index.get("fields");
        assertThat(fields, containsInAnyOrder("_id", "_rev", "name"));
    }

    @Test
    public void createIndexOverTwoFields() throws QueryException {
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "basic");
        assertThat(indexName, is("basic"));

        Map<String, Map<String, Object>> indexes = im.listIndexes();
        assertThat(indexes, hasKey("basic"));

        @SuppressWarnings("unchecked")
        Map<String, Object> index = indexes.get("basic");
        @SuppressWarnings("unchecked")
        List<String> fields = (List<String>) index.get("fields");
        assertThat(fields, containsInAnyOrder("_id", "_rev", "name", "age"));
    }

    @Test
    public void createIndexUsingDottedNotation() throws QueryException {
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name.first"), new FieldSort("age.years")),
                                            "basic");
        assertThat(indexName, is("basic"));

        Map<String, Map<String, Object>> indexes = im.listIndexes();
        assertThat(indexes, hasKey("basic"));

        @SuppressWarnings("unchecked")
        Map<String, Object> index = indexes.get("basic");
        @SuppressWarnings("unchecked")
        List<String> fields = (List<String>) index.get("fields");
        assertThat(fields, containsInAnyOrder("_id", "_rev", "name.first", "age.years"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void createMultipleIndexes() throws QueryException {
        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "basic");
        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "another");
        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("cat")), "petname");

        Map<String, Map<String, Object>> indexes = im.listIndexes();
        assertThat(indexes.keySet(), containsInAnyOrder("basic", "another", "petname"));

        Map<String, Object> index = indexes.get("basic");
        List<String> fields = (List<String>) index.get("fields");
        assertThat(fields, containsInAnyOrder("_id", "_rev", "name", "age"));

        index = (Map<String, Object>) indexes.get("another");
        fields = (List<String>) index.get("fields");
        assertThat(fields, containsInAnyOrder("_id", "_rev", "name", "age"));

        index = (Map<String, Object>) indexes.get("petname");
        fields = (List<String>) index.get("fields");
        assertThat(fields, containsInAnyOrder("_id", "_rev", "cat"));
    }

    @Test
    public void createIndexSpecifiedWithAscOrDesc() throws QueryException {
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(
                new FieldSort("name", FieldSort.Direction.ASCENDING),
                new FieldSort("age", FieldSort.Direction.DESCENDING)), "basic");
        assertThat(indexName, is("basic"));

        Map<String, Map<String, Object>> indexes = im.listIndexes();
        assertThat(indexes, hasKey("basic"));

        @SuppressWarnings("unchecked")
        Map<String, Object> index = indexes.get("basic");
        @SuppressWarnings("unchecked")
        List<String> fields = (List<String>) index.get("fields");
        assertThat(fields, containsInAnyOrder("_id", "_rev", "name", "age"));
    }

    @Test
    public void createIndexWhenIndexNameExistsIdxDefinitionSame() throws QueryException {
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(
                new FieldSort("name", FieldSort.Direction.ASCENDING),
                new FieldSort("age", FieldSort.Direction.DESCENDING)), "basic");
        assertThat(indexName, is("basic"));

        // succeeds when the index definition is the same
        indexName = im.ensureIndexed(Arrays.<FieldSort>asList(
                new FieldSort("name", FieldSort.Direction.ASCENDING),
                new FieldSort("age", FieldSort.Direction.DESCENDING)), "basic");
        assertThat(indexName, is("basic"));
    }

    @Test
    public void createIndexWhenIndexNameExistsIdxDefinitionDifferent() throws QueryException {
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(
                new FieldSort("name", FieldSort.Direction.ASCENDING),
                new FieldSort("age", FieldSort.Direction.DESCENDING)), "basic");
        assertThat(indexName, is("basic"));

        // fails when the index definition is different
        try {
            indexName = im.ensureIndexed(Arrays.<FieldSort>asList(
                    new FieldSort("name", FieldSort.Direction.ASCENDING),
                    new FieldSort("pet", FieldSort.Direction.DESCENDING)), "basic");
            Assert.fail("ensureIndexed should throw QueryException");
        } catch (QueryException qe) {
            ;
        }
    }

    @Test
    public void createIndexWithJsonType() throws QueryException {
        // supports using the json type
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(
                new FieldSort("name", FieldSort.Direction.ASCENDING),
                new FieldSort("age", FieldSort.Direction.DESCENDING)),
                                            "basic",
                                            IndexType.JSON);
        assertThat(indexName, is("basic"));
        Map<String, Map<String, Object>> indexes = im.listIndexes();
        assertThat(indexes.size(), is(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> index = indexes.get("basic");
        assertThat((IndexType) index.get("type"), is(IndexType.JSON));
        assertThat(index.get("settings"), is(nullValue()));
    }

    @Test
    public void createIndexWithTextType() throws QueryException {
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(
                new FieldSort("name", FieldSort.Direction.ASCENDING),
                new FieldSort("age", FieldSort.Direction.DESCENDING)),
                                            "basic",
                                            IndexType.TEXT);
        assertThat(indexName, is("basic"));
        Map<String, Map<String, Object>> indexes = im.listIndexes();
        assertThat(indexes.size(), is(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> index = indexes.get("basic");
        assertThat((IndexType) index.get("type"), is(IndexType.TEXT));
        assertThat((String) index.get("settings"), is("{\"tokenize\":\"simple\"}"));
    }

    @Test
    public void createIndexWithTextTypeAndTokenizeSetting() throws QueryException {
        Map<String, String> settings = new HashMap<String, String>();
        settings.put("tokenize", "porter");
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")),
                "basic",
                IndexType.TEXT,
                settings);
        assertThat(indexName, is("basic"));
        Map<String, Map<String, Object>> indexes = im.listIndexes();
        assertThat(indexes.size(), is(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> index = indexes.get("basic");
        assertThat((IndexType) index.get("type"), is(IndexType.TEXT));
        assertThat((String) index.get("settings"), is("{\"tokenize\":\"porter\"}"));
    }

    @Test
    public void indexAndTextIndexCanCoexist() throws QueryException {
        Map<String, String> settings = new HashMap<String, String>();
        settings.put("tokenize", "porter");
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")),
                                            "textIndex",
                                            IndexType.TEXT,
                                            settings);
        assertThat(indexName, is("textIndex"));
        indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "jsonIndex");
        assertThat(indexName, is("jsonIndex"));
        Map<String, Map<String, Object>> indexes = im.listIndexes();
        assertThat(indexes.keySet(), containsInAnyOrder("textIndex", "jsonIndex"));
    }

    @Test
    public void correctlyLimitsTextIndexesToOne() throws QueryException {
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "basic", IndexType.TEXT);
        assertThat(indexName, is("basic"));
        indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "anotherIndex", IndexType.TEXT);
        assertThat(indexName, is(nullValue()));
    }

    @Test
    public void createIndexUsingNonAsciiText() throws QueryException {
        // can create indexes successfully
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("اسم"), new FieldSort("datatype"), new FieldSort("ages")),
                                            "basic");
        assertThat(indexName, is("basic"));
    }

    @Test
    public void normalizeIndexFields() {
        HashMap<String, String> nameField = new HashMap<String, String>();
        nameField.put("name", "asc");
        HashMap<String, String> petField = new HashMap<String, String>();
        petField.put("pet", "desc");

        // removes directions from the field specifiers
        List<String> fields;
        fields = IndexCreator.removeDirectionsFromFields(Arrays.<FieldSort>asList(new FieldSort("name", FieldSort.Direction.ASCENDING),
                                                                               new FieldSort("pet", FieldSort.Direction.DESCENDING),
                                                                               new FieldSort("age")));
        assertThat(fields, containsInAnyOrder("name", "pet", "age"));
    }

    @Test
    public void createIndexWhereFieldNameContainsDollarSign() throws QueryException {

        // rejects indexes with $ at start
        try {
            im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("$name"), new FieldSort("datatype")), "basic");
            Assert.fail("Expected ensureIndexed to throw a QueryException");
        } catch (QueryException qe) {
            ;
        }

        // creates indexes with $ not at start
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("na$me"), new FieldSort("datatype$")), "basic");
        assertThat(indexName, is("basic"));
    }

    @Test
    public void validateFieldNames() {
        // allows single fields
        assertThat(IndexCreator.validFieldName("name"), is(true));

        // allows dotted notation fields
        assertThat(IndexCreator.validFieldName("name.first"), is(true));
        assertThat(IndexCreator.validFieldName("name.first.prefix"), is(true));

        // allows dollars in positions other than first letter of a part
        assertThat(IndexCreator.validFieldName("na$me"), is(true));
        assertThat(IndexCreator.validFieldName("name.fir$t"), is(true));
        assertThat(IndexCreator.validFieldName("name.fir$t.pref$x"), is(true));

        // rejects dollars in first letter of a part
        assertThat(IndexCreator.validFieldName("$name"), is(false));
        assertThat(IndexCreator.validFieldName("name.$first"), is(false));
        assertThat(IndexCreator.validFieldName("name.$first.$prefix"), is(false));
        assertThat(IndexCreator.validFieldName("name.first.$prefix"), is(false));
        assertThat(IndexCreator.validFieldName("name.first.$pr$efix"), is(false));
        assertThat(IndexCreator.validFieldName("name.$$$$.prefix"), is(false));
    }

}
