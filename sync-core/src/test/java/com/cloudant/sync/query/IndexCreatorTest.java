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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexCreatorTest extends AbstractIndexTestBase {

    @Test
    public void emptyIndexList() {
        Map<String, Object> indexes = im.listIndexes();
        assertThat(indexes, is(notNullValue()));
        assertThat(indexes.isEmpty(), is(true));
    }

    @Test
    public void preconditionsToCreatingIndexes() {
        // doesn't create an index on null fields
        String name = im.ensureIndexed(null, "basic");
        assertThat(name, is(nullValue()));

        // doesn't create an index on no fields
        List<Object> fieldNames = new ArrayList<Object>();
        name = im.ensureIndexed(fieldNames, "basic");
        assertThat(name, is(nullValue()));

        // doesn't create an index on null name
        fieldNames = Arrays.<Object>asList("name");
        name = im.ensureIndexed(fieldNames, null);
        assertThat(name, is(nullValue()));

        // doesn't create an index without a name
        name = im.ensureIndexed(fieldNames, "");
        assertThat(name, is(nullValue()));

        // doesn't create an index on null index type
        name = im.ensureIndexed(fieldNames, "basic", null);
        assertThat(name, is(nullValue()));

        // doesn't create an index if duplicate fields
        fieldNames = Arrays.<Object>asList("age", "pet", "age");
        name = im.ensureIndexed(fieldNames, "basic");
        assertThat(name, is(nullValue()));
    }

    @Test
    public void createIndexOverOneField() {
        String indexName = im.ensureIndexed(Arrays.<Object>asList("name"), "basic");
        assertThat(indexName, is("basic"));

        Map<String, Object> indexes = im.listIndexes();
        assertThat(indexes, hasKey("basic"));

        @SuppressWarnings("unchecked")
        Map<String, Object> index = (Map<String, Object>) indexes.get("basic");
        @SuppressWarnings("unchecked")
        List<String> fields = (List<String>) index.get("fields");
        assertThat(fields, containsInAnyOrder("_id", "_rev", "name"));
    }

    @Test
    public void createIndexOverTwoFields() {
        String indexName = im.ensureIndexed(Arrays.<Object>asList("name", "age"), "basic");
        assertThat(indexName, is("basic"));

        Map<String, Object> indexes = im.listIndexes();
        assertThat(indexes, hasKey("basic"));

        @SuppressWarnings("unchecked")
        Map<String, Object> index = (Map<String, Object>) indexes.get("basic");
        @SuppressWarnings("unchecked")
        List<String> fields = (List<String>) index.get("fields");
        assertThat(fields, containsInAnyOrder("_id", "_rev", "name", "age"));
    }

    @Test
    public void createIndexUsingDottedNotation() {
        String indexName = im.ensureIndexed(Arrays.<Object>asList("name.first", "age.years"),
                                            "basic");
        assertThat(indexName, is("basic"));

        Map<String, Object> indexes = im.listIndexes();
        assertThat(indexes, hasKey("basic"));

        @SuppressWarnings("unchecked")
        Map<String, Object> index = (Map<String, Object>) indexes.get("basic");
        @SuppressWarnings("unchecked")
        List<String> fields = (List<String>) index.get("fields");
        assertThat(fields, containsInAnyOrder("_id", "_rev", "name.first", "age.years"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void createMultipleIndexes() {
        im.ensureIndexed(Arrays.<Object>asList("name", "age"), "basic");
        im.ensureIndexed(Arrays.<Object>asList("name", "age"), "another");
        im.ensureIndexed(Arrays.<Object>asList("cat"), "petname");

        Map<String, Object> indexes = im.listIndexes();
        assertThat(indexes.keySet(), containsInAnyOrder("basic", "another", "petname"));

        Map<String, Object> index = (Map<String, Object>) indexes.get("basic");
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
    public void createIndexSpecifiedWithAscOrDesc() {
        HashMap<String, String> nameField = new HashMap<String, String>();
        nameField.put("name", "asc");
        HashMap<String, String> ageField = new HashMap<String, String>();
        ageField.put("age", "desc");
        String indexName = im.ensureIndexed(Arrays.<Object>asList(nameField, ageField), "basic");
        assertThat(indexName, is("basic"));

        Map<String, Object> indexes = im.listIndexes();
        assertThat(indexes, hasKey("basic"));

        @SuppressWarnings("unchecked")
        Map<String, Object> index = (Map<String, Object>) indexes.get("basic");
        @SuppressWarnings("unchecked")
        List<String> fields = (List<String>) index.get("fields");
        assertThat(fields, containsInAnyOrder("_id", "_rev", "name", "age"));
    }

    @Test
    public void createIndexWhenIndexNameExistsIdxDefinitionSame() {
        HashMap<String, String> nameField = new HashMap<String, String>();
        nameField.put("name", "asc");
        HashMap<String, String> ageField = new HashMap<String, String>();
        ageField.put("age", "desc");
        String indexName = im.ensureIndexed(Arrays.<Object>asList(nameField, ageField), "basic");
        assertThat(indexName, is("basic"));

        // succeeds when the index definition is the same
        indexName = im.ensureIndexed(Arrays.<Object>asList(nameField, ageField), "basic");
        assertThat(indexName, is("basic"));
    }

    @Test
    public void createIndexWhenIndexNameExistsIdxDefinitionDifferent() {
        HashMap<String, String> nameField = new HashMap<String, String>();
        nameField.put("name", "asc");
        HashMap<String, String> ageField = new HashMap<String, String>();
        ageField.put("age", "desc");
        String indexName = im.ensureIndexed(Arrays.<Object>asList(nameField, ageField), "basic");
        assertThat(indexName, is("basic"));

        // fails when the index definition is different
        HashMap<String, String> petField = new HashMap<String, String>();
        petField.put("pet", "desc");
        indexName = im.ensureIndexed(Arrays.<Object>asList(nameField, petField), "basic");
        assertThat(indexName, is(nullValue()));
    }

    @Test
    public void createIndexWithJsonType() {
        HashMap<String, String> nameField = new HashMap<String, String>();
        nameField.put("name", "asc");
        HashMap<String, String> ageField = new HashMap<String, String>();
        ageField.put("age", "desc");

        // supports using the json type
        String indexName = im.ensureIndexed(Arrays.<Object>asList(nameField, ageField),
                                            "basic",
                                            "json");
        assertThat(indexName, is("basic"));
    }

    @Test
    public void createIndexWithTextType() {
        HashMap<String, String> nameField = new HashMap<String, String>();
        nameField.put("name", "asc");
        HashMap<String, String> ageField = new HashMap<String, String>();
        ageField.put("age", "desc");

        // doesn't support using the text type
        String indexName = im.ensureIndexed(Arrays.<Object>asList(nameField, ageField),
                                            "basic",
                                            "text");
        assertThat(indexName, is(nullValue()));
    }

    @Test
    public void createIndexWithGeoType() {
        HashMap<String, String> nameField = new HashMap<String, String>();
        nameField.put("name", "asc");
        HashMap<String, String> ageField = new HashMap<String, String>();
        ageField.put("age", "desc");

        // doesn't support using the geo type
        String indexName = im.ensureIndexed(Arrays.<Object>asList(nameField, ageField),
                                            "basic",
                                            "geo");
        assertThat(indexName, is(nullValue()));
    }

    @Test
    public void createIndexWithUnplannedType() {
        HashMap<String, String> nameField = new HashMap<String, String>();
        nameField.put("name", "asc");
        HashMap<String, String> ageField = new HashMap<String, String>();
        ageField.put("age", "desc");

        // doesn't support using the unplanned type
        String indexName = im.ensureIndexed(Arrays.<Object>asList(nameField, ageField),
                                            "basic",
                                            "frog");
        assertThat(indexName, is(nullValue()));
    }

    @Test
    public void createIndexUsingNonAsciiText() {
        // can create indexes successfully
        String indexName = im.ensureIndexed(Arrays.<Object>asList("اسم", "datatype", "ages"),
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
        fields = IndexCreator.removeDirectionsFromFields(Arrays.<Object>asList(nameField,
                                                                               petField,
                                                                               "age"));
        assertThat(fields, containsInAnyOrder("name", "pet", "age"));
    }

    @Test
    public void createIndexWhereFieldNameContainsDollarSign() {
        // rejects indexes with $ at start
        String indexName = im.ensureIndexed(Arrays.<Object>asList("$name", "datatype"), "basic");
        assertThat(indexName, is(nullValue()));

        // creates indexes with $ not at start
        indexName = im.ensureIndexed(Arrays.<Object>asList("na$me", "datatype$"), "basic");
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