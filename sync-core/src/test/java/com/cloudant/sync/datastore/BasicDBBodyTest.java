/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.datastore;

import com.cloudant.sync.util.JSONUtils;
import com.cloudant.sync.util.TestUtils;

import org.junit.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BasicDBBodyTest {

    String documentOneFile = "fixture/document_1.json";
    byte[] jsonData;

    @Before
    public void setUp() throws Exception {
        jsonData = FileUtils.readFileToByteArray(TestUtils.loadFixture(documentOneFile));
    }

    // No null input should be used
    @Test(expected = IllegalArgumentException.class)
    public void constructor_nullInput_exception() {
        Map m = null;
        new BasicDocumentBody(m);
    }

    // Invalid input should result in am object with empty JSON body
    @Test(expected = IllegalArgumentException.class)
    public void constructor_invalidInput_objectWithEmptyJsonShouldBeCreated() {
        DocumentBody body = new BasicDocumentBody("[]".getBytes());
        Assert.assertTrue(Arrays.equals("{}".getBytes(), body.asBytes()));
        Assert.assertNotNull(body.asMap());
        Assert.assertTrue(body.asMap().size() == 0);
    }

    @Test
    public void constructor_byteArray_correctObjectShouldBeCreated() throws Exception {
        DocumentBody body = new BasicDocumentBody(jsonData);
        Assert.assertTrue(Arrays.equals(jsonData, body.asBytes()));
        Assert.assertNotNull(body.asMap());

        Map<String, Object> actualMap = body.asMap();
        assertMapIsCorrect(actualMap);
    }

    @Test
    public void constructor_map_correctObjectShouldBeCreated() {
        DocumentBody body = new BasicDocumentBody(JSONUtils.deserialize(jsonData));
        Map<String, Object> map = JSONUtils.deserialize(body.asBytes());
        assertMapIsCorrect(map);
    }

    @Test
    public void constructor_emptyMap_objectWithEmptyJsonShouldBeCreated() {
        DocumentBody body = new BasicDocumentBody(new HashMap());
        Assert.assertTrue(Arrays.equals("{}".getBytes(), body.asBytes()));
        Assert.assertNotNull(body.asMap());
        Assert.assertTrue(body.asMap().size() == 0);
    }

    @Test
    public void asMap_differentNumberTypes_jacksonPicksNaturalMapping() throws IOException {
        byte[] d = FileUtils.readFileToByteArray(TestUtils.loadFixture("fixture/basic_bdbody_test_as_map.json"));
        DocumentBody body = new BasicDocumentBody(d);
        Assert.assertEquals("-101", body.asMap().get("StringValue"));

        Map<String, Object> m = body.asMap();

        Assert.assertTrue(m.get("LongValue") instanceof Long);
        Assert.assertTrue(m.get("LongValue").equals(2147483648l)); // Integer.MAX_VALUE + 1

        Assert.assertTrue(m.get("IntegerValue") instanceof Integer);
        Assert.assertTrue(m.get("IntegerValue").equals(2147483647)); // Integer.MAX_VALUE
    }

    private void assertMapIsCorrect(Map<String, Object> actualMap) {
        Assert.assertEquals(5, actualMap.size());
        Assert.assertTrue((Boolean) actualMap.get("Sunrise"));
        Assert.assertEquals("A run to the head of the blood", (String)actualMap.get("Data"));
        Assert.assertEquals(2, ((List) actualMap.get("Activities")).size());
    }
}
