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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexTest {

    private List<FieldSort> fieldNames;
    private String indexName;

    @Before
    public void setUp() {
        fieldNames = Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age"));
        indexName = "basic";

    }

    @Test
    public void constructsIndexWithDefaultType() {
        Index index = Index.getInstance(fieldNames, indexName);
        assertThat(index.indexName, is("basic"));
        assertThat(index.fieldNames, is(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age"))));
        assertThat(index.indexType, is(IndexType.JSON));
        assertThat(index.indexSettings, is(nullValue()));
    }

    @Test
    public void constructsIndexWithTextTypeDefaultSettings() {
        Index index = Index.getInstance(fieldNames, indexName, IndexType.TEXT);
        assertThat(index.indexName, is("basic"));
        assertThat(index.fieldNames, is(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age"))));
        assertThat(index.indexType, is(IndexType.TEXT));
        assertThat(index.indexSettings.size(), is(1));
        assertThat(index.indexSettings.get("tokenize"), is("simple"));
    }

    @Test
    public void returnsNullWhenNoFields() {
        Index index = Index.getInstance(null, indexName);
        assertThat(index, is(nullValue()));

        index = Index.getInstance(new ArrayList<FieldSort>(), indexName);
        assertThat(index, is(nullValue()));
    }

    @Test
    public void returnsValueWhenIndexNameIsNull(){
        Index index = Index.getInstance(fieldNames, null);
        assertThat(index, is(notNullValue()));
    }

    @Test
    public void returnsNullWhenNoIndexName() {
        Index index = Index.getInstance(fieldNames, "");
        assertThat(index, is(nullValue()));
    }

    @Test
    public void returnsNullWhenInvalidIndexSettings() {
        Map<String, String> indexSettings = new HashMap<String, String>();
        indexSettings.put("foo", "bar");
        Index index = Index.getInstance(fieldNames, indexName, IndexType.TEXT, indexSettings);
        assertThat(index, is(nullValue()));
    }

    @Test
    public void correctlyIgnoresIndexSettings() {
        Map<String, String> indexSettings = new HashMap<String, String>();
        indexSettings.put("tokenize", "porter");
        // json indexes do not support index settings.  Index settings will be ignored.
        Index index = Index.getInstance(fieldNames, indexName, IndexType.JSON, indexSettings);
        assertThat(index.indexSettings, is(nullValue()));
    }

    @Test
    public void correctlySetsIndexSettings() {
        Map<String, String> indexSettings = new HashMap<String, String>();
        indexSettings.put("tokenize", "porter");
        // text indexes support the tokenize setting.
        Index index = Index.getInstance(fieldNames, indexName, IndexType.TEXT, indexSettings);
        assertThat(index.indexSettings.size(), is(1));
        assertThat(index.indexSettings.get("tokenize"), is("porter"));
    }

    @Test
    public void comparesIndexTypeAndReturnsInEquality() {
        Index index = Index.getInstance(fieldNames, indexName);
        assertThat(index.compareIndexTypeTo(IndexType.TEXT, null), is(false));
    }

    @Test
    public void comparesIndexTypeAndReturnsEquality() {
        Index index = Index.getInstance(fieldNames, indexName);
        assertThat(index.compareIndexTypeTo(IndexType.JSON, null), is(true));
    }

    @Test
    public void comparesIndexSettingsAndReturnsInEquality() {
        Index index = Index.getInstance(fieldNames, indexName, IndexType.TEXT);
        assertThat(index.compareIndexTypeTo(IndexType.TEXT, "{\"tokenize\":\"porter\"}"), is(false));
    }

    @Test
    public void comparesIndexSettingsAndReturnsEquality() {
        Index index = Index.getInstance(fieldNames, indexName, IndexType.TEXT);
        assertThat(index.compareIndexTypeTo(IndexType.TEXT, "{\"tokenize\":\"simple\"}"), is(true));
    }

    @Test
    public void returnsIndexSettingsAsAString() {
        Index index = Index.getInstance(fieldNames, indexName, IndexType.TEXT);
        assertThat(index.settingsAsJSON(), is("{\"tokenize\":\"simple\"}"));
    }

    @Test
    public void returnsNullIndexSettingsAsAString() {
        Index index = Index.getInstance(fieldNames, indexName);
        assertThat(index.settingsAsJSON(), is(nullValue()));
    }

}
