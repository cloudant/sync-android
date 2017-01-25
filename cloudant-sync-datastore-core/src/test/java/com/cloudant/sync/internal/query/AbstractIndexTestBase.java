/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2014 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.internal.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.documentstore.DocumentStore;
import com.cloudant.sync.internal.sqlite.SQLDatabaseQueue;
import com.cloudant.sync.util.SQLDatabaseTestUtils;
import com.cloudant.sync.util.TestUtils;

import org.junit.After;
import org.junit.Before;

import java.io.File;

public abstract class AbstractIndexTestBase {

    String factoryPath = null;
    DatabaseImpl ds = null;
    QueryImpl im = null;
    SQLDatabaseQueue indexManagerDatabaseQueue;

    @Before
    public void setUp() throws Exception {
        factoryPath = TestUtils.createTempTestingDir(AbstractIndexTestBase.class.getName());
        assertThat(factoryPath, is(notNullValue()));
        DocumentStore documentStore = DocumentStore.getInstance(new File(factoryPath));
        ds = (DatabaseImpl) documentStore.database();
        assertThat(ds, is(notNullValue()));
        im = (QueryImpl) documentStore.query();
        assertThat(im, is(notNullValue()));
        indexManagerDatabaseQueue = TestUtils.getDBQueue(im);
        assertThat(indexManagerDatabaseQueue, is(notNullValue()));
        String[] metadataTableList = new String[] { QueryImpl.INDEX_METADATA_TABLE_NAME };
        SQLDatabaseTestUtils.assertTablesExist(indexManagerDatabaseQueue,
                                               metadataTableList);
    }

    @After
    public void tearDown() throws Exception {
        im.close();
        assertThat(indexManagerDatabaseQueue.isShutdown(), is(true));
        ds.close();
        TestUtils.deleteTempTestingDir(factoryPath);

        im = null;
        ds = null;
        factoryPath = null;
    }

}
