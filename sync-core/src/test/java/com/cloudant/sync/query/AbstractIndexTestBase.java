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

import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.SQLDatabaseTestUtils;
import com.cloudant.sync.util.TestUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.sql.SQLException;

public class AbstractIndexTestBase {

    String factoryPath = null;
    DatastoreManager factory = null;
    DatastoreExtended ds = null;
    IndexManager im = null;
    SQLDatabase db = null;

    @Before
    public void setUp() throws SQLException {
        factoryPath = TestUtils.createTempTestingDir(AbstractIndexTestBase.class.getName());
        Assert.assertNotNull(factoryPath);
        factory = new DatastoreManager(factoryPath);
        Assert.assertNotNull(factory);
        ds = (DatastoreExtended) factory.openDatastore(AbstractIndexTestBase.class.getSimpleName());
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

}
