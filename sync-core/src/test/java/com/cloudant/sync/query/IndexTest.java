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
import static org.hamcrest.Matchers.nullValue;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexTest {

    private List<Object> fieldNames;
    private String indexName;

    @Before
    public void setUp() {
        fieldNames = Arrays.<Object>asList("name", "age");
        indexName = "basic";

    }

    @Test
    public void constructsIndexWithDefaultType() {
        Index index = Index.setUp(fieldNames, indexName);
        assertThat(index.indexName, is("basic"));
        assertThat(index.fieldNames, is(Arrays.<Object>asList("name", "age")));
        assertThat(index.indexType, is("json"));
        assertThat(index.indexSettings, is(nullValue()));
    }

    @Test
    public void constructsIndexWithTextTypeDefaultSettings() {
        Index index = Index.setUp(fieldNames, indexName, "text");
        assertThat(index.indexName, is("basic"));
        assertThat(index.fieldNames, is(Arrays.<Object>asList("name", "age")));
        assertThat(index.indexType, is("text"));
        assertThat(index.indexSettings.size(), is(1));
        assertThat(index.indexSettings.get("tokenize"), is("simple"));
    }

    @Test
    public void returnsNullWhenNoFields() {
        Index index = Index.setUp(null, indexName);
        assertThat(index, is(nullValue()));

        index = Index.setUp(new ArrayList<Object>(), indexName);
        assertThat(index, is(nullValue()));
    }

    @Test
    public void returnsNullWhenNoIndexName() {
        Index index = Index.setUp(fieldNames, null);
        assertThat(index, is(nullValue()));

        index = Index.setUp(fieldNames, "");
        assertThat(index, is(nullValue()));
    }

    @Test
    public void returnsNullWhenNoIndexType() {
        Index index = Index.setUp(fieldNames, indexName, null);
        assertThat(index, is(nullValue()));

        index = Index.setUp(fieldNames, indexName, "");
        assertThat(index, is(nullValue()));
    }

    @Test
    public void returnsNullWhenInvalidIndexType() {
        Index index = Index.setUp(fieldNames, indexName, "blah");
        assertThat(index, is(nullValue()));
    }

    @Test
    public void returnsNullWhenInvalidIndexSettings() {
        Map<String, String> indexSettings = new HashMap<String, String>();
        indexSettings.put("foo", "bar");
        Index index = Index.setUp(fieldNames, indexName, "text", indexSettings);
        assertThat(index, is(nullValue()));
    }

    @Test
    public void correctlyIgnoresIndexSettings() {
        Map<String, String> indexSettings = new HashMap<String, String>();
        indexSettings.put("tokenize", "porter");
        // json indexes do not support index settings.  Index settings will be ignored.
        Index index = Index.setUp(fieldNames, indexName, "json", indexSettings);
        assertThat(index.indexSettings, is(nullValue()));
    }

    @Test
    public void correctlySetsIndexSettings() {
        Map<String, String> indexSettings = new HashMap<String, String>();
        indexSettings.put("tokenize", "porter");
        // text indexes support the tokenize setting.
        Index index = Index.setUp(fieldNames, indexName, "text", indexSettings);
        assertThat(index.indexSettings.size(), is(1));
        assertThat(index.indexSettings.get("tokenize"), is("porter"));
    }

    @Test
    public void comparesIndexTypeAndReturnsInEquality() {
        Index index = Index.setUp(fieldNames, indexName);
        assertThat(index.compareIndexTypeTo("text", null), is(false));
    }

    @Test
    public void comparesIndexTypeAndReturnsEquality() {
        Index index = Index.setUp(fieldNames, indexName);
        assertThat(index.compareIndexTypeTo("json", null), is(true));
    }

    @Test
    public void comparesIndexSettingsAndReturnsInEquality() {
        Index index = Index.setUp(fieldNames, indexName, "text");
        assertThat(index.compareIndexTypeTo("text", "{\"tokenize\":\"porter\"}"), is(false));
    }

    @Test
    public void comparesIndexSettingsAndReturnsEquality() {
        Index index = Index.setUp(fieldNames, indexName, "text");
        assertThat(index.compareIndexTypeTo("text", "{\"tokenize\":\"simple\"}"), is(true));
    }

}
