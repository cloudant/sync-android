/**
 * Copyright (C) 2013, 2016 IBM Corp. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.datastore;

import com.cloudant.mazha.Document;
import com.cloudant.sync.util.MultiThreadedTestHelper;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

public class DatabaseManagerTest {

    public static File TEST_PATH = null;

    @Before
    public void setUp() {
        TEST_PATH = new File(FileUtils.getTempDirectory().getAbsolutePath(),
                "DatastoreManagerTest" + UUID.randomUUID());
    }

    @After
    public void tearDown() {
        FileUtils.deleteQuietly(TEST_PATH);
    }

    @Test(expected = IllegalArgumentException.class)
    public void nonWritableDatastoreManagerPathThrows() throws DatastoreNotCreatedException {
        File f = new File(TEST_PATH, "c_root_test");
        try {
            f.mkdir();
            f.setReadOnly();
            DocumentStore ds = DocumentStore.getInstance(f);
            ds.close();
        } finally {
            f.setWritable(true);
            f.delete();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void createFailsIfMissingIntermediates() throws DatastoreNotCreatedException {
        File f = new File(TEST_PATH, "missing_test");
        try {
            DocumentStore ds = DocumentStore.getInstance(f);
            ds.close();
        } finally {
            f.delete();
        }
    }

    @Test
    public void createDirectoryIfMissing() throws DatastoreNotCreatedException {
        File f = TEST_PATH;
        try {
            DocumentStore ds = DocumentStore.getInstance(f);
            ds.close();
        } finally {
            f.delete();
        }
    }

    @Test
    public void openDatastore_name_dbShouldBeCreated() throws Exception {
        DocumentStore ds = createAndAssertDatastore();
        ds.close();
        // TODO assert?
    }

    private DocumentStore createAndAssertDatastore() throws Exception {
        DocumentStore d = DocumentStore.getInstance(TEST_PATH);
        Database ds = d.database;
        Assert.assertNotNull(ds);
        boolean assertsFailed = true;
        try {
            String dbDir = TEST_PATH.getAbsolutePath();
            Assert.assertTrue(new File(dbDir).exists());
            Assert.assertTrue(new File(dbDir).isDirectory());

            String dbFile = dbDir + "/db.sync";
            Assert.assertTrue(new File(dbFile).exists());
            Assert.assertTrue(new File(dbFile).isFile());
            assertsFailed = false;
        } finally {
            if (assertsFailed) {
                d.close();
            }
        }
        return d;
    }

    @Test
    public void deleteDatastore_dbExist_dbShouldBeDeleted() throws Exception {
        DocumentStore ds = createAndAssertDatastore();
        ds.delete();
        String dbDir = TEST_PATH.getAbsolutePath();
        Assert.assertFalse(new File(dbDir).exists());
    }

    @Test(expected = IOException.class)
    public void deleteDatastore_dbNotExist_nothing() throws Exception {
        DocumentStore ds = createAndAssertDatastore();
        ds.delete();
        // should be an error to try and delete twice
        ds.delete();
    }

    @Test(expected = IllegalStateException.class)
    public void deleteDatastore_createDocumentUsingDeletedDatastore_exception() throws Exception {
        DocumentStore ds = createAndAssertDatastore();
        DocumentRevision object = ds.database.createDocumentFromRevision(createDBBody("Tom"));
        Assert.assertNotNull(object);

        ds.delete();
        String dbDir = TEST_PATH.getAbsolutePath();
        Assert.assertFalse(new File(dbDir).exists());

        ds.database.createDocumentFromRevision(createDBBody("Jerry"));
    }

    public DocumentRevision createDBBody(String name) throws IOException {
        Map m = new HashMap<String, Object>();
        m.put("name", name);
        DocumentRevision rev = new DocumentRevision();
        rev.setBody(new DocumentBodyImpl(m));
        return rev;
    }

    @Test
    public void multithreadedDatastoreCreation() throws Exception {
        new MultiThreadedTestHelper<DocumentStore>(25) {

            @Override
            protected void doAssertions() throws Exception {
                DocumentStore ds0 = results.get(0);
                try {
                    for (DocumentStore ds : results) {
                        Assert.assertSame("The datastore instances should all be the same", ds0, ds);

                    }
                } finally {
                    // only call close() on the first one - subsequent calls to close() would
                    // result in an IllegalStateException
                    ds0.close();
                }
            }

            @Override
            protected Callable<DocumentStore> getCallable() {
                return new
                        Callable<DocumentStore>() {
                            @Override
                            public DocumentStore call() throws Exception {
                                return DocumentStore.getInstance(TEST_PATH);
                            }
                        };
            }
        }.run();
    }

    @Test
    public void datastoreInstanceNotReusedAfterClose() throws Exception {
        DocumentStore ds1 = null, ds2 = null;
        try {
            ds1 = DocumentStore.getInstance(TEST_PATH);
        } finally {
            if (ds1 != null) ds1.close();
        }
        try {
            ds2 = DocumentStore.getInstance(TEST_PATH);
            Assert.assertNotSame("The documentstore instances should not be the same.", ds1, ds2);
            Assert.assertNotSame("The database instances should not be the same.", ds1.database, ds2.database);
        } finally {
            if (ds2 != null) ds2.close();
        }
    }

    @Test
    public void assertFactoryReturnsSameInstanceString() throws Exception {
        DocumentStore manager = DocumentStore.getInstance(TEST_PATH);
        DocumentStore manager2 = DocumentStore.getInstance(TEST_PATH);
        Assert.assertSame("The DatastoreManager instances should be the same.", manager, manager2);
        manager.close();
    }

    @Test
    public void multithreadedDatastoreManagerAccess() throws Exception {
        final DocumentStore manager = DocumentStore.getInstance(TEST_PATH);

        new MultiThreadedTestHelper<DocumentStore>(25) {

            @Override
            protected void doAssertions() throws Exception {
                for (DocumentStore gotManager : results) {
                    Assert.assertSame("The datastore manager instances should all be the same",
                            manager, gotManager);
                }
            }

            @Override
            protected Callable<DocumentStore> getCallable() {
                return new
                        Callable<DocumentStore>() {
                            @Override
                            public DocumentStore call() throws Exception {
                                return DocumentStore.getInstance(TEST_PATH);
                            }
                        };
            }
        }.run();
    }

    // closing an already closed datastore should raise an IllegalStateException
    @Test(expected = IllegalStateException.class)
    public void closeTwice() throws DatastoreNotCreatedException {
        DocumentStore ds = DocumentStore.getInstance(TEST_PATH);
        ds.close();
        ds.close();
    }

}
