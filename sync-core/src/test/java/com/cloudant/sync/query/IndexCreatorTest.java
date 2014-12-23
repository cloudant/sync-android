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

package com.cloudant.sync.query;

import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.SQLDatabaseTestUtils;
import com.cloudant.sync.util.TestUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexCreatorTest {

    String factoryPath = null;
    DatastoreManager factory = null;
    DatastoreExtended ds = null;
    IndexManager im = null;
    SQLDatabase db = null;

    @Before
    public void setUp() throws SQLException {
        factoryPath = TestUtils.createTempTestingDir(IndexCreatorTest.class.getName());
        Assert.assertNotNull(factoryPath);
        factory = new DatastoreManager(factoryPath);
        Assert.assertNotNull(factory);
        ds = (DatastoreExtended) factory.openDatastore(IndexCreatorTest.class.getSimpleName());
        Assert.assertNotNull(ds);
        im = new IndexManager(ds);
        Assert.assertNotNull(im);
        db = im.getDatabase();
        Assert.assertNotNull(db);
        Assert.assertNotNull(im.getQueue());
        String[] metadataTableList = new String[] { IndexManager.INDEX_METADATA_TABLE_NAME };
        SQLDatabaseTestUtils.assertTablesExist(db, metadataTableList);
    }

    @After
    public void tearDown() {
        im.close();
        Assert.assertTrue(im.getQueue().isShutdown());
        ds.close();
        TestUtils.deleteDatabaseQuietly(db);
        TestUtils.deleteTempTestingDir(factoryPath);

        im = null;
        ds = null;
        factory = null;
        factoryPath = null;
    }

    @Test
    public void emptyIndexList() {
        Map<String, Object> indexes = im.listIndexes();
        Assert.assertNotNull(indexes);
        Assert.assertTrue(indexes.isEmpty());
    }

    @Test
    public void failuresCreatingIndexes() {
        // doesn't create an index on null fields
        ArrayList<Object> fieldNames = null;
        String name = im.ensureIndexed(fieldNames, "basic");
        Assert.assertNull(name);

        // doesn't create an index on no fields
        fieldNames = new ArrayList<Object>();
        name = im.ensureIndexed(fieldNames, "basic");
        Assert.assertNull(name);

        // doesn't create an index on null name
        fieldNames.add("name");
        name = im.ensureIndexed(fieldNames, null);
        Assert.assertNull(name);

        // doesn't create an index without a name
        name = im.ensureIndexed(fieldNames, "");
        Assert.assertNull(name);

        // doesn't create an index on null index type
        name = im.ensureIndexed(fieldNames, "basic", null);
        Assert.assertNull(name);

        // doesn't create an index on index type != json
        name = im.ensureIndexed(fieldNames, "basic", "text");
        Assert.assertNull(name);

        // doesn't create an index if duplicate fields
        fieldNames.clear();
        fieldNames.add("age");
        fieldNames.add("pet");
        fieldNames.add("age");
        name = im.ensureIndexed(fieldNames, "basic");
        Assert.assertNull(name);
    }

    @Test
    public void createIndexOverOneField() {
        ArrayList<Object> fieldNames = new ArrayList<Object>();
        fieldNames.add("name");
        String name = im.ensureIndexed(fieldNames, "basic");
        Assert.assertTrue(name.equals("basic"));

        Map<String, Object> indexes = im.listIndexes();
        Assert.assertEquals(1, indexes.size());
        Assert.assertTrue(indexes.containsKey("basic"));

        Map<String, Object> index = (Map) indexes.get("basic");
        List<String> fields = (List) index.get("fields");
        Assert.assertEquals(3, fields.size());
        Assert.assertTrue(fields.contains("_id"));
        Assert.assertTrue(fields.contains("_rev"));
        Assert.assertTrue(fields.contains("name"));
    }

    @Test
    public void createIndexOverTwoFields() {
        ArrayList<Object> fieldNames = new ArrayList<Object>();
        fieldNames.add("name");
        fieldNames.add("age");
        String name = im.ensureIndexed(fieldNames, "basic");
        Assert.assertTrue(name.equals("basic"));

        Map<String, Object> indexes = im.listIndexes();
        Assert.assertEquals(1, indexes.size());
        Assert.assertTrue(indexes.containsKey("basic"));

        Map<String, Object> index = (Map) indexes.get("basic");
        List<String> fields = (List) index.get("fields");
        Assert.assertEquals(4, fields.size());
        Assert.assertTrue(fields.contains("_id"));
        Assert.assertTrue(fields.contains("_rev"));
        Assert.assertTrue(fields.contains("name"));
        Assert.assertTrue(fields.contains("age"));
    }

    @Test
    public void createIndexUsingDottedNotation() {
        ArrayList<Object> fieldNames = new ArrayList<Object>();
        fieldNames.add("name.first");
        fieldNames.add("age.years");
        String name = im.ensureIndexed(fieldNames, "basic");
        Assert.assertTrue(name.equals("basic"));

        Map<String, Object> indexes = im.listIndexes();
        Assert.assertEquals(1, indexes.size());
        Assert.assertTrue(indexes.containsKey("basic"));

        Map<String, Object> index = (Map) indexes.get("basic");
        List<String> fields = (List) index.get("fields");
        Assert.assertEquals(4, fields.size());
        Assert.assertTrue(fields.contains("_id"));
        Assert.assertTrue(fields.contains("_rev"));
        Assert.assertTrue(fields.contains("name.first"));
        Assert.assertTrue(fields.contains("age.years"));
    }

    @Test
    public void createMultipleIndexes() {
        ArrayList<Object> fieldNames = new ArrayList<Object>();
        fieldNames.add("name");
        fieldNames.add("age");
        im.ensureIndexed(fieldNames, "basic");
        im.ensureIndexed(fieldNames, "another");
        fieldNames.clear();
        fieldNames.add("cat");
        im.ensureIndexed(fieldNames, "petname");

        Map<String, Object> indexes = im.listIndexes();
        Assert.assertEquals(3, indexes.size());
        Assert.assertTrue(indexes.containsKey("basic"));
        Assert.assertTrue(indexes.containsKey("another"));
        Assert.assertTrue(indexes.containsKey("petname"));

        Map<String, Object> basicIndex = (Map) indexes.get("basic");
        List<String> basicIndexFields = (List) basicIndex.get("fields");
        Assert.assertEquals(4, basicIndexFields.size());
        Assert.assertTrue(basicIndexFields.contains("_id"));
        Assert.assertTrue(basicIndexFields.contains("_rev"));
        Assert.assertTrue(basicIndexFields.contains("name"));
        Assert.assertTrue(basicIndexFields.contains("age"));

        Map<String, Object> anotherIndex = (Map) indexes.get("another");
        List<String> anotherIndexFields = (List) anotherIndex.get("fields");
        Assert.assertEquals(4, anotherIndexFields.size());
        Assert.assertTrue(anotherIndexFields.contains("_id"));
        Assert.assertTrue(anotherIndexFields.contains("_rev"));
        Assert.assertTrue(anotherIndexFields.contains("name"));
        Assert.assertTrue(anotherIndexFields.contains("age"));

        Map<String, Object> petnameIndex = (Map) indexes.get("petname");
        List<String> petnameIndexFields = (List) petnameIndex.get("fields");
        Assert.assertEquals(3, petnameIndexFields.size());
        Assert.assertTrue(petnameIndexFields.contains("_id"));
        Assert.assertTrue(petnameIndexFields.contains("_rev"));
        Assert.assertTrue(petnameIndexFields.contains("cat"));
    }

    @Test
    public void createIndexSpecifiedWithAscOrDesc() {
        ArrayList<Object> fieldNames = new ArrayList<Object>();
        HashMap<String, String> nameField = new HashMap<String, String>();
        nameField.put("name", "asc");
        HashMap<String, String> ageField = new HashMap<String, String>();
        ageField.put("age", "desc");
        fieldNames.add(nameField);
        fieldNames.add(ageField);
        String name = im.ensureIndexed(fieldNames, "basic");
        Assert.assertTrue(name.equals("basic"));

        Map<String, Object> indexes = im.listIndexes();
        Assert.assertEquals(1, indexes.size());
        Assert.assertTrue(indexes.containsKey("basic"));

        Map<String, Object> index = (Map) indexes.get("basic");
        List<String> fields = (List) index.get("fields");
        Assert.assertEquals(4, fields.size());
        Assert.assertTrue(fields.contains("_id"));
        Assert.assertTrue(fields.contains("_rev"));
        Assert.assertTrue(fields.contains("name"));
        Assert.assertTrue(fields.contains("age"));

    }

}
