//  Copyright (c) 2014 Cloudant. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.cloudant.sync.query;

import com.cloudant.sync.datastore.DocumentBody;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.datastore.DocumentRevisionBuilder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ValueExtractorTest {

    DocumentRevision revision;
    DocumentBody body;

    @Before
    public void setUp() {
        // body content: { "name" : "mike" }
        Map<String, String> bodyMap = new HashMap<String, String>();
        bodyMap.put("name", "mike");
        body = DocumentBodyFactory.create(bodyMap);
        DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
        builder.setDocId("dsfsdfdfs");
        builder.setRevId("1-qweqeqwewqe");
        builder.setBody(body);
        revision = builder.build();
    }

    @Test
    public void getDocIdFromRevision() {
        String value = (String) ValueExtractor.extractValueForFieldName("_id", revision);
        Assert.assertTrue(value.equals("dsfsdfdfs"));
    }

    @Test
    public void getRevIdFromRevision() {
        String value = (String) ValueExtractor.extractValueForFieldName("_rev", revision);
        Assert.assertTrue(value.equals("1-qweqeqwewqe"));
    }

    @Test
    public void getNameFromRevision() {
        String value = (String) ValueExtractor.extractValueForFieldName("name", revision);
        Assert.assertTrue(value.equals("mike"));
    }

    @Test
    public void extractSingleFieldWhenEmptyFieldName() {
        // returns null for empty field name
        Object v = ValueExtractor.extractValueForFieldName("", body);
        Assert.assertNull(v);
    }

    @Test
    public void extractSingleFieldForSingleDepth() {
        // returns value for single field depth
        String v = (String) ValueExtractor.extractValueForFieldName("name", body);
        Assert.assertTrue(v.equals("mike"));
    }

    @Test
    public void extractSingleFieldForDoubleDepth() {
        // returns value for two field depth
        String v = (String) ValueExtractor.extractValueForFieldName("name.first",
                                                                    getTwoLevelBody());
        Assert.assertTrue(v.equals("mike"));
    }

    @Test
    public void extractSingleFieldForTripleDepth() {

        // returns value for three field depth
        String v = (String) ValueExtractor.extractValueForFieldName("aaa.bbb.ccc",
                                                                    getThreeLevelBody());
        Assert.assertTrue(v.equals("mike"));
    }

    @Test
    public void extractSingleFieldWhenFieldNamePrefix() {
        // copes when a prefix of the field name exists
        Object v = (String) ValueExtractor.extractValueForFieldName("name.first", body);
        Assert.assertNull(v);

        v = (String) ValueExtractor.extractValueForFieldName("name.first.mike",
                                                             getThreeLevelBody());
        Assert.assertNull(v);
    }

    @Test
    public void extractSubDocument() {
        // returns the sub-document if the path doesn't terminate with a value
        Map<String, String> v = (Map) ValueExtractor.extractValueForFieldName("aaa.bbb",
                                                                              getThreeLevelBody());
        Assert.assertNotNull(v);
        Assert.assertTrue(v instanceof Map);
        Assert.assertEquals(v.size(), 1);
        Assert.assertTrue(v.containsKey("ccc"));
        Assert.assertTrue(v.get("ccc").equals("mike"));
    }

    private DocumentBody getTwoLevelBody() {
        // body content: { "name" : { "first" : "mike" } }
        Map<String, String> levelTwo = new HashMap<String, String>();
        levelTwo.put("first", "mike");
        Map<String, Object> levelOne = new HashMap<String, Object>();
        levelOne.put("name", levelTwo);
        return DocumentBodyFactory.create(levelOne);
    }

    private DocumentBody getThreeLevelBody() {
        // body content: { "aaa" : { "bbb" : { "ccc" : "mike" } } }
        Map<String, String> levelThree = new HashMap<String, String>();
        levelThree.put("ccc", "mike");
        Map<String, Object> levelTwo = new HashMap<String, Object>();
        levelTwo.put("bbb", levelThree);
        Map<String, Object> levelOne = new HashMap<String, Object>();
        levelOne.put("aaa", levelTwo);
        return DocumentBodyFactory.create(levelOne);
    }
}
