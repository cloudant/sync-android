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
    public void failuresWhenCreatingIndexes() {
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

}
