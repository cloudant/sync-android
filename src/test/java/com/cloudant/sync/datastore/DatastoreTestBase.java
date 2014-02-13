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

import com.cloudant.sync.sqlite.sqlite4java.SQLiteWrapper;
import com.cloudant.sync.util.TestUtils;
import org.junit.After;
import org.junit.Before;

/**
 * Test base for any test suite need a <code>DatastoreManager</code> and <code>Datastore</code> instance. It
 * automatically set up and clean up the temp file directly for you.
 */
public abstract class DatastoreTestBase {

    String datastore_manager_dir;
    DatastoreManager datastoreManager;
    SQLiteWrapper database = null;
    BasicDatastore datastore = null;

    @Before
    public void setUp() throws Exception {
        datastore_manager_dir = TestUtils.createTempTestingDir(this.getClass().getName());
        datastoreManager = new DatastoreManager(this.datastore_manager_dir);
        datastore = (BasicDatastore)(this.datastoreManager.openDatastore(getClass().getSimpleName()));
        database = (SQLiteWrapper)(this.datastore.getSQLDatabase());
    }

    @After
    public void testDown() {
        TestUtils.deleteDatabaseQuietly(database);
        TestUtils.deleteTempTestingDir(datastore_manager_dir);
    }
}
