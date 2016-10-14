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
        Index index = new Index(fieldNames, indexName);
        assertThat(index.indexName, is("basic"));
        assertThat(index.fieldNames, is(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age"))));
        assertThat(index.indexType, is(IndexType.JSON));
        assertThat(index.tokenize, is(nullValue()));
    }

    @Test
    public void constructsIndexWithTextTypeDefaultSettings() {
        Index index = new Index(fieldNames, indexName, IndexType.TEXT);
        assertThat(index.indexName, is("basic"));
        assertThat(index.fieldNames, is(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age"))));
        assertThat(index.indexType, is(IndexType.TEXT));
        assertThat(index.tokenize, is("simple"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void returnsNullWhenNoFields() {
        Index index = new Index(null, indexName);
        assertThat(index, is(nullValue()));

        index = new Index(new ArrayList<FieldSort>(), indexName);
        assertThat(index, is(nullValue()));
    }

    @Test
    public void returnsValueWhenIndexNameIsNull(){
        Index index = new Index(fieldNames, null);
        assertThat(index, is(notNullValue()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void returnsNullWhenNoIndexName() {
        Index index = new Index(fieldNames, "");
        assertThat(index, is(nullValue()));
    }

    @Test
    public void correctlyIgnoresIndexSettings() {
        // json indexes do not support index settings.  Index settings will be ignored.
        Index index = new Index(fieldNames, indexName, IndexType.JSON, "porter");
        assertThat(index.tokenize, is(nullValue()));
    }

    @Test
    public void correctlySetsIndexSettings() {
        // text indexes support the tokenize setting.
        Index index = new Index(fieldNames, indexName, IndexType.TEXT, "porter");
        assertThat(index.tokenize, is("porter"));
    }
    // TODO these should test that .equals() does the right thing
/*
    @Test
    public void comparesIndexTypeAndReturnsInEquality() {
        Index index = new Index(fieldNames, indexName);
        assertThat(index.compareIndexTypeTo(IndexType.TEXT, null), is(false));
    }

    @Test
    public void comparesIndexTypeAndReturnsEquality() {
        Index index = new Index(fieldNames, indexName);
        assertThat(index.compareIndexTypeTo(IndexType.JSON, null), is(true));
    }

    @Test
    public void comparesIndexSettingsAndReturnsInEquality() {
        Index index = new Index(fieldNames, indexName, IndexType.TEXT);
        assertThat(index.compareIndexTypeTo(IndexType.TEXT, "{\"tokenize\":\"porter\"}"), is(false));
    }

    @Test
    public void comparesIndexSettingsAndReturnsEquality() {
        Index index = new Index(fieldNames, indexName, IndexType.TEXT);
        assertThat(index.compareIndexTypeTo(IndexType.TEXT, "{\"tokenize\":\"simple\"}"), is(true));
    }*/

    @Test
    public void returnsIndexSettingsAsAString() {
        Index index = new Index(fieldNames, indexName, IndexType.TEXT);
        assertThat(index.settingsAsJSON(), is("{\"tokenize\":\"simple\"}"));
    }

    @Test
    public void returnsNullIndexSettingsAsAString() {
        Index index = new Index(fieldNames, indexName);
        assertThat(index.settingsAsJSON(), is("{}"));
    }

}
