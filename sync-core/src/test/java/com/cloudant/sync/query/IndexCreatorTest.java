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
        Map<String, Object> indexes = im.listIndexes();
        Assert.assertNotNull(indexes);
        Assert.assertTrue(indexes.isEmpty());
    }

    @Test
    public void preconditionsToCreatingIndexes() {
        // doesn't create an index on null fields
        String name = im.ensureIndexed(null, "basic");
        Assert.assertNull(name);

        // doesn't create an index on no fields
        List<Object> fieldNames = new ArrayList<Object>();
        name = im.ensureIndexed(fieldNames, "basic");
        Assert.assertNull(name);

        // doesn't create an index on null name
        fieldNames = Arrays.<Object>asList("name");
        name = im.ensureIndexed(fieldNames, null);
        Assert.assertNull(name);

        // doesn't create an index without a name
        name = im.ensureIndexed(fieldNames, "");
        Assert.assertNull(name);

        // doesn't create an index on null index type
        name = im.ensureIndexed(fieldNames, "basic", null);
        Assert.assertNull(name);

        // doesn't create an index if duplicate fields
        fieldNames = Arrays.<Object>asList("age", "pet", "age");
        name = im.ensureIndexed(fieldNames, "basic");
        Assert.assertNull(name);
    }

    @Test
    public void createIndexOverOneField() {
        List<Object> fieldNames = Arrays.<Object>asList("name");
        String name = im.ensureIndexed(fieldNames, "basic");
        Assert.assertTrue(name.equals("basic"));

        Map<String, Object> indexes = im.listIndexes();
        Assert.assertEquals(1, indexes.size());
        Assert.assertTrue(indexes.containsKey("basic"));

        @SuppressWarnings("unchecked")
        Map<String, Object> index = (Map<String, Object>) indexes.get("basic");
        @SuppressWarnings("unchecked")
        List<String> fields = (List<String>) index.get("fields");
        Assert.assertEquals(3, fields.size());
        Assert.assertTrue(fields.contains("_id"));
        Assert.assertTrue(fields.contains("_rev"));
        Assert.assertTrue(fields.contains("name"));
    }

    @Test
    public void createIndexOverTwoFields() {
        List<Object> fieldNames = Arrays.<Object>asList("name", "age");
        String name = im.ensureIndexed(fieldNames, "basic");
        Assert.assertTrue(name.equals("basic"));

        Map<String, Object> indexes = im.listIndexes();
        Assert.assertEquals(1, indexes.size());
        Assert.assertTrue(indexes.containsKey("basic"));

        @SuppressWarnings("unchecked")
        Map<String, Object> index = (Map<String, Object>) indexes.get("basic");
        @SuppressWarnings("unchecked")
        List<String> fields = (List<String>) index.get("fields");
        Assert.assertEquals(4, fields.size());
        Assert.assertTrue(fields.contains("_id"));
        Assert.assertTrue(fields.contains("_rev"));
        Assert.assertTrue(fields.contains("name"));
        Assert.assertTrue(fields.contains("age"));
    }

    @Test
    public void createIndexUsingDottedNotation() {
        List<Object> fieldNames = Arrays.<Object>asList("name.first", "age.years");
        String name = im.ensureIndexed(fieldNames, "basic");
        Assert.assertTrue(name.equals("basic"));

        Map<String, Object> indexes = im.listIndexes();
        Assert.assertEquals(1, indexes.size());
        Assert.assertTrue(indexes.containsKey("basic"));

        @SuppressWarnings("unchecked")
        Map<String, Object> index = (Map<String, Object>) indexes.get("basic");
        @SuppressWarnings("unchecked")
        List<String> fields = (List<String>) index.get("fields");
        Assert.assertEquals(4, fields.size());
        Assert.assertTrue(fields.contains("_id"));
        Assert.assertTrue(fields.contains("_rev"));
        Assert.assertTrue(fields.contains("name.first"));
        Assert.assertTrue(fields.contains("age.years"));
    }

    @Test
    public void createMultipleIndexes() {
        List<Object> fieldNames = Arrays.<Object>asList("name", "age");
        im.ensureIndexed(fieldNames, "basic");
        im.ensureIndexed(fieldNames, "another");
        fieldNames = Arrays.<Object>asList("cat");
        im.ensureIndexed(fieldNames, "petname");

        Map<String, Object> indexes = im.listIndexes();
        Assert.assertEquals(3, indexes.size());
        Assert.assertTrue(indexes.containsKey("basic"));
        Assert.assertTrue(indexes.containsKey("another"));
        Assert.assertTrue(indexes.containsKey("petname"));

        @SuppressWarnings("unchecked")
        Map<String, Object> basicIndex = (Map<String, Object>) indexes.get("basic");
        @SuppressWarnings("unchecked")
        List<String> basicIndexFields = (List<String>) basicIndex.get("fields");
        Assert.assertEquals(4, basicIndexFields.size());
        Assert.assertTrue(basicIndexFields.contains("_id"));
        Assert.assertTrue(basicIndexFields.contains("_rev"));
        Assert.assertTrue(basicIndexFields.contains("name"));
        Assert.assertTrue(basicIndexFields.contains("age"));

        @SuppressWarnings("unchecked")
        Map<String, Object> anotherIndex = (Map<String, Object>) indexes.get("another");
        @SuppressWarnings("unchecked")
        List<String> anotherIndexFields = (List<String>) anotherIndex.get("fields");
        Assert.assertEquals(4, anotherIndexFields.size());
        Assert.assertTrue(anotherIndexFields.contains("_id"));
        Assert.assertTrue(anotherIndexFields.contains("_rev"));
        Assert.assertTrue(anotherIndexFields.contains("name"));
        Assert.assertTrue(anotherIndexFields.contains("age"));

        @SuppressWarnings("unchecked")
        Map<String, Object> petnameIndex = (Map<String, Object>) indexes.get("petname");
        @SuppressWarnings("unchecked")
        List<String> petnameIndexFields = (List<String>) petnameIndex.get("fields");
        Assert.assertEquals(3, petnameIndexFields.size());
        Assert.assertTrue(petnameIndexFields.contains("_id"));
        Assert.assertTrue(petnameIndexFields.contains("_rev"));
        Assert.assertTrue(petnameIndexFields.contains("cat"));
    }

    @Test
    public void createIndexSpecifiedWithAscOrDesc() {
        HashMap<String, String> nameField = new HashMap<String, String>();
        nameField.put("name", "asc");
        HashMap<String, String> ageField = new HashMap<String, String>();
        ageField.put("age", "desc");
        List<Object> fieldNames = Arrays.<Object>asList(nameField, ageField);
        String name = im.ensureIndexed(fieldNames, "basic");
        Assert.assertTrue(name.equals("basic"));

        Map<String, Object> indexes = im.listIndexes();
        Assert.assertEquals(1, indexes.size());
        Assert.assertTrue(indexes.containsKey("basic"));

        @SuppressWarnings("unchecked")
        Map<String, Object> index = (Map<String, Object>) indexes.get("basic");
        @SuppressWarnings("unchecked")
        List<String> fields = (List<String>) index.get("fields");
        Assert.assertEquals(4, fields.size());
        Assert.assertTrue(fields.contains("_id"));
        Assert.assertTrue(fields.contains("_rev"));
        Assert.assertTrue(fields.contains("name"));
        Assert.assertTrue(fields.contains("age"));
    }

    @Test
    public void createIndexWhenIndexNameExistsIdxDefinitionSame() {
        HashMap<String, String> nameField = new HashMap<String, String>();
        nameField.put("name", "asc");
        HashMap<String, String> ageField = new HashMap<String, String>();
        ageField.put("age", "desc");
        List<Object> fieldNames = Arrays.<Object>asList(nameField, ageField);
        String name = im.ensureIndexed(fieldNames, "basic");
        Assert.assertTrue(name.equals("basic"));

        // succeeds when the index definition is the same
        name = im.ensureIndexed(fieldNames, "basic");
        Assert.assertTrue(name.equals("basic"));
    }

    @Test
    public void createIndexWhenIndexNameExistsIdxDefinitionDifferent() {
        HashMap<String, String> nameField = new HashMap<String, String>();
        nameField.put("name", "asc");
        HashMap<String, String> ageField = new HashMap<String, String>();
        ageField.put("age", "desc");
        List<Object> fieldNames = Arrays.<Object>asList(nameField, ageField);
        String name = im.ensureIndexed(fieldNames, "basic");
        Assert.assertTrue(name.equals("basic"));

        // fails when the index definition is different
        fieldNames = Arrays.<Object>asList(nameField);
        HashMap<String, String> petField = new HashMap<String, String>();
        petField.put("pet", "desc");
        Arrays.<Object>asList(nameField, petField);
        name = im.ensureIndexed(fieldNames, "basic");
        Assert.assertNull(name);
    }

    @Test
    public void createIndexWithJsonType() {
        HashMap<String, String> nameField = new HashMap<String, String>();
        nameField.put("name", "asc");
        HashMap<String, String> ageField = new HashMap<String, String>();
        ageField.put("age", "desc");
        List<Object> fieldNames = Arrays.<Object>asList(nameField, ageField);

        // supports using the json type
        String name = im.ensureIndexed(fieldNames, "basic", "json");
        Assert.assertTrue(name.equals("basic"));
    }

    @Test
    public void createIndexWithTextType() {
        HashMap<String, String> nameField = new HashMap<String, String>();
        nameField.put("name", "asc");
        HashMap<String, String> ageField = new HashMap<String, String>();
        ageField.put("age", "desc");
        List<Object> fieldNames = Arrays.<Object>asList(nameField, ageField);

        // doesn't support using the text type
        String name = im.ensureIndexed(fieldNames, "basic", "text");
        Assert.assertNull(name);
    }

    @Test
    public void createIndexWithGeoType() {
        HashMap<String, String> nameField = new HashMap<String, String>();
        nameField.put("name", "asc");
        HashMap<String, String> ageField = new HashMap<String, String>();
        ageField.put("age", "desc");
        List<Object> fieldNames = Arrays.<Object>asList(nameField, ageField);

        // doesn't support using the geo type
        String name = im.ensureIndexed(fieldNames, "basic", "geo");
        Assert.assertNull(name);
    }

    @Test
    public void createIndexWithUnplannedType() {
        HashMap<String, String> nameField = new HashMap<String, String>();
        nameField.put("name", "asc");
        HashMap<String, String> ageField = new HashMap<String, String>();
        ageField.put("age", "desc");
        List<Object> fieldNames = Arrays.<Object>asList(nameField, ageField);

        // doesn't support using the unplanned type
        String name = im.ensureIndexed(fieldNames, "basic", "frog");
        Assert.assertNull(name);
    }

    @Test
    public void createIndexUsingNonAsciiText() {
        List<Object> fieldNames = Arrays.<Object>asList("اسم", "datatype", "ages");

        // can create indexes successfully
        String name = im.ensureIndexed(fieldNames, "nonascii");
        Assert.assertTrue(name.equals("nonascii"));
    }

    @Test
    public void normalizeIndexFields() {
        HashMap<String, String> nameField = new HashMap<String, String>();
        nameField.put("name", "asc");
        HashMap<String, String> petField = new HashMap<String, String>();
        petField.put("pet", "desc");
        List<Object> fieldNames = Arrays.<Object>asList(nameField, petField, "age");

        // removes directions from the field specifiers
        List<String> fields = IndexCreator.removeDirectionsFromFields(fieldNames);
        Assert.assertEquals(3, fields.size());
        Assert.assertTrue(fields.contains("name"));
        Assert.assertTrue(fields.contains("pet"));
        Assert.assertTrue(fields.contains("age"));
    }

    @Test
    public void createIndexWhereFieldNameContainsDollarSign() {
        List<Object> fieldNames = Arrays.<Object>asList("$name", "datatype");

        // rejects indexes with $ at start
        String name = im.ensureIndexed(fieldNames, "basic");
        Assert.assertNull(name);

        fieldNames = Arrays.<Object>asList("na$me", "datatype$");

        // creates indexes with $ not at start
        name = im.ensureIndexed(fieldNames, "basic");
        Assert.assertTrue(name.equals("basic"));
    }

    @Test
    public void validateFieldNames() {
        // allows single fields
        Assert.assertTrue(IndexCreator.validFieldName("name"));

        // allows dotted notation fields
        Assert.assertTrue(IndexCreator.validFieldName("name.first"));
        Assert.assertTrue(IndexCreator.validFieldName("name.first.prefix"));

        // allows dollars in positions other than first letter of a part
        Assert.assertTrue(IndexCreator.validFieldName("na$me"));
        Assert.assertTrue(IndexCreator.validFieldName("name.fir$t"));
        Assert.assertTrue(IndexCreator.validFieldName("name.fir$t.pref$x"));

        // rejects dollars in first letter of a part
        Assert.assertFalse(IndexCreator.validFieldName("$name"));
        Assert.assertFalse(IndexCreator.validFieldName("name.$first"));
        Assert.assertFalse(IndexCreator.validFieldName("name.$first.$prefix"));
        Assert.assertFalse(IndexCreator.validFieldName("name.first.$prefix"));
        Assert.assertFalse(IndexCreator.validFieldName("name.first.$pr$efix"));
        Assert.assertFalse(IndexCreator.validFieldName("name.$$$$.prefix"));
    }

}