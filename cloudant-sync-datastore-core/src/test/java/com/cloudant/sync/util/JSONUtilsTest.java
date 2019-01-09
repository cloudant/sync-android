/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.util;

import com.cloudant.sync.internal.mazha.CouchDbInfo;
import com.cloudant.sync.internal.replication.Foo;
import com.cloudant.sync.internal.util.JSONUtils;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.io.FileUtils;
import static org.hamcrest.CoreMatchers.is;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

public class JSONUtilsTest {

    @Test
    public void isValidJSON_validMap() {
        Map obj = new HashMap<String, String>();
        obj.put("name", "the great wall");
        Assert.assertTrue(JSONUtils.isValidJSON(obj));
    }

    @Test
    public void isValidJSON_invalidMap() {
        Map obj = new HashMap<String, String>();
        obj.put(null, "123");
        Assert.assertFalse(JSONUtils.isValidJSON(obj));
    }

    @Test
    public void isValidJSON_invalidJSONString() {
        Assert.assertFalse(JSONUtils.isValidJSON("haha"));
    }

    @Test
    public void isValidJSON_integerString() {
        Assert.assertFalse(JSONUtils.isValidJSON("101"));
    }

    @Test
    public void serializeAsBytes() {
        Map obj = new HashMap<String, String>();
        obj.put("name", "the great wall");
        String json = "{\"name\":\"the great wall\"}";
        Arrays.equals(json.getBytes(), JSONUtils.serializeAsBytes(obj));
    }

    @Test(expected = IllegalStateException.class)
    public void serializeAsBytes_invalidMap_exception() {
        Map obj = new HashMap<String, String>();
        obj.put(null, "the great wall");
        JSONUtils.serializeAsBytes(obj);
    }

    @Test(expected = IllegalStateException.class)
    public void serializeAsString_invalidMap_exception() {
        Map obj = new HashMap<String, String>();
        obj.put(null, "the great wall");
        JSONUtils.serializeAsString(obj);
    }

    @Test
    public void serializeAsString() {
        Map obj = new HashMap<String, String>();
        obj.put("name", "the great wall");
        String json = "{\"name\":\"the great wall\"}";
        Assert.assertEquals(json, JSONUtils.serializeAsString(obj));
    }

    @Test
    public void serializeAsString_document_idAndRevShouldBeFilteredOut() {
        // it is important the couch reserved key words are filtered out
        String expectedJson = "{\"foo\":\"haha\"}";
        Foo foo = new Foo();
        foo.setId("someId");
        foo.setRevision("1-somerevid");
        foo.setFoo("haha");
        String json = JSONUtils.serializeAsString(foo);
        Assert.assertEquals(expectedJson, json);
    }

    @Test
    public void deserialize_documentWithId_idAndRevShouldBeDeserialized() {
        String expectedJson = "{\"foo\":\"haha\", \"_id\":\"someId\"}";
        Foo foo = JSONUtils.deserialize(expectedJson.getBytes(), Foo.class);
        Assert.assertEquals("haha", foo.getFoo());
        Assert.assertEquals("someId", foo.getId());
        Assert.assertNull(foo.getRevision());
    }

    @Test
    public void deserialize_numberInDocument_correctlySerializedAsProperNumber() throws Exception {
        byte[] data = readJsonDataFromFile("fixture/json_utils_test_number.json");
        Map<String, Object> m = JSONUtils.deserialize(data, Map.class);
        Assert.assertEquals(Integer.valueOf(1), m.get("number"));
        Assert.assertEquals(Long.valueOf(30000000000000000L), m.get("number1"));
        Assert.assertEquals(Double.valueOf(12.0909F), (Double)m.get("number2"), 0.00001);
    }

    @Test
    public void deserialize_booleanInDocument_correctlySerializedAsBoolean() throws Exception {
        byte[] data = readJsonDataFromFile("fixture/json_utils_test_boolean.json");
        Map<String, Object> m = JSONUtils.deserialize(data, Map.class);
        Assert.assertEquals(Boolean.TRUE, m.get("boolean1"));
        Assert.assertEquals(Boolean.FALSE, m.get("boolean2"));
    }

    @Test
    public void deserialize_complexDocumentWith_correctlySerializedToMap() throws Exception {
        byte[] jsonData = readJsonDataFromFile("fixture/json_utils_test.json");
        Map<String, Object> jsonDocumentMap = JSONUtils.deserialize(jsonData, Map.class);
        Assert.assertEquals(6, jsonDocumentMap.size());

        Map<String, Object> organizer = (Map)jsonDocumentMap.get("Organizer");
        Assert.assertEquals(2, organizer.size());
        Assert.assertEquals("Tom", organizer.get("Name"));
        Assert.assertEquals("Male", organizer.get("Sex"));

        List<String> fullHours = (List)jsonDocumentMap.get("FullHours");
        Assert.assertEquals(10, fullHours.size());
        Assert.assertEquals(10, fullHours.get(9));

        List activities = (List)jsonDocumentMap.get("Activities");
        Assert.assertEquals(2, activities.size());

        Map<String, Object> football = (Map)activities.get(0);
        Assert.assertEquals(3, football.size());
        Assert.assertEquals(Integer.valueOf(2), football.get("Duration"));

        Map<String, Object> breakfast = (Map)activities.get(1);
        Assert.assertEquals(4, breakfast.size());
        Assert.assertEquals(Integer.valueOf(40), breakfast.get("Duration"));
    }

    @Test
    public void deserialize_CouchDBInfo_purgeSeq_from_long() throws Exception {
        File dbinfoData = TestUtils.loadFixture("fixture/dbinfo-pre23.json");
        CouchDbInfo deserialized = JSONUtils.fromJson(new FileReader(dbinfoData), new
                TypeReference<CouchDbInfo>() {
        });
        Long purgeSeq = Long.valueOf(deserialized.getPurgeSeq());
        Assert.assertThat(purgeSeq, is(0l));
    }

    private byte[] readJsonDataFromFile(String filename) throws IOException {
        byte[] data = FileUtils.readFileToByteArray(TestUtils.loadFixture(filename));
        return data;
    }
}

