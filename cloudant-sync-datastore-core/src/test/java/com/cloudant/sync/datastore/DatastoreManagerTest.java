/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
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

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DatastoreManagerTest {

    public static String TEST_PATH = null;
    public DatastoreManager manager = null;

    @Before
    public void setUp() {
        TEST_PATH = FileUtils.getTempDirectory().getAbsolutePath() + File.separator +
                "DatastoreManagerTest";
        new File(TEST_PATH).mkdirs();
        manager = new DatastoreManager(TEST_PATH);
    }

    @After
    public void tearDown() {
        FileUtils.deleteQuietly(new File(TEST_PATH));
    }

    @Test(expected = IllegalArgumentException.class)
    public void nonWritableDatastoreManagerPathThrows() {
        File f = new File(TEST_PATH, "c_root_test");
        try {
            f.mkdir();
            f.setReadOnly();
            manager = new DatastoreManager(f.getAbsolutePath());
        } finally {
            f.setWritable(true);
            f.delete();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void createFailsIfMissingIntermediates(){
        File f = new File(TEST_PATH, "missing_inter/missing_test");
        try {
            manager = new DatastoreManager(f.getAbsolutePath());
        } finally {
            f.delete();
        }
    }

    @Test
    public void createDirectoryIfMissing(){
        File f = new File(TEST_PATH, "missing_test");
        try {
            manager = new DatastoreManager(f.getAbsolutePath());
        } finally {
            f.delete();
        }
    }

    @Test
    public void getDatastorePath() {
        Assert.assertEquals(TEST_PATH, manager.getPath());
    }

    @Test
    public void test_possible_filenames() {
        Assert.assertTrue("Aalfd_sn9".matches(DatastoreManager.LEGAL_CHARACTERS));
        Assert.assertTrue("a_alf_dsn9".matches(DatastoreManager.LEGAL_CHARACTERS));
        Assert.assertFalse("0Aalfdsn9".matches(DatastoreManager.LEGAL_CHARACTERS));
        Assert.assertTrue("Aa-lfdsn9".matches(DatastoreManager.LEGAL_CHARACTERS));
        Assert.assertTrue("Aa/lfdsn9".matches(DatastoreManager.LEGAL_CHARACTERS));
        Assert.assertFalse("Aa lfdsn9".matches(DatastoreManager.LEGAL_CHARACTERS));
        Assert.assertFalse("".matches(DatastoreManager.LEGAL_CHARACTERS));
    }

    @Test
    public void openDatastore_name_dbShouldBeCreated() throws Exception {
        Datastore ds = createAndAssertDatastore();
        ds.close();
    }

    private Datastore createAndAssertDatastore() throws Exception {
        Datastore ds = manager.openDatastore("mydatastore");
        Assert.assertNotNull(ds);
        boolean assertsFailed = true;
        try {
            String dbDir = TEST_PATH + "/mydatastore";
            Assert.assertTrue(new File(dbDir).exists());
            Assert.assertTrue(new File(dbDir).isDirectory());

            String dbFile = dbDir + "/db.sync";
            Assert.assertTrue(new File(dbFile).exists());
            Assert.assertTrue(new File(dbFile).isFile());
            assertsFailed = false;
        } finally {
            if (assertsFailed) {
                ds.close();
            }
        }
        return ds;
    }

    @Test
    public void deleteDatastore_dbExist_dbShouldBeDeleted() throws Exception {
        Datastore ds = createAndAssertDatastore();

        manager.deleteDatastore("mydatastore");
        String dbDir = TEST_PATH + "/mydatastore";
        Assert.assertFalse(new File(dbDir).exists());
    }

    @Test(expected = IOException.class)
    public void deleteDatastore_dbNotExist_nothing() throws Exception {
        manager.deleteDatastore("db_not_exist");
    }

    @Test(expected = IllegalStateException.class)
    public void deleteDatastore_createDocumentUsingDeletedDatastore_exception() throws Exception {
        Datastore ds = createAndAssertDatastore();
        DocumentRevision object = ds.createDocumentFromRevision(createDBBody("Tom"));
        Assert.assertNotNull(object);

        manager.deleteDatastore("mydatastore");
        String dbDir = TEST_PATH + "/mydatastore";
        Assert.assertFalse(new File(dbDir).exists());

        ds.createDocumentFromRevision(createDBBody("Jerry"));
    }

    public DocumentRevision createDBBody(String name) throws IOException {
        Map m = new HashMap<String, Object>();
        m.put("name", name);
        DocumentRevision rev = new DocumentRevision();
        rev.setBody(new DocumentBodyImpl(m));
        return rev;
    }

    @Test
    public void createDatastoreWithForwardSlashChar() throws Exception {
        Datastore ds = manager.openDatastore("datastore/mynewdatastore");
        try {
            String dbDir = TEST_PATH + "/datastore.mynewdatastore";
            Assert.assertTrue(new File(dbDir).exists());
            Assert.assertTrue(new File(dbDir).isDirectory());
            String dbFile = dbDir + "/db.sync";
            Assert.assertTrue(new File(dbFile).exists());
            Assert.assertTrue(new File(dbFile).isFile());
        } finally {
            ds.close();
        }


    }

    @Test
    public void list5Datastores() throws Exception {
        List<Datastore> opened = new ArrayList<Datastore>();

        try {
            for (int i = 0; i < 5; i++) {
                opened.add(manager.openDatastore("datastore" + i));
            }

            List<String> datastores = manager.listAllDatastores();

            Assert.assertEquals(5, datastores.size());
            for (int i = 0; i < 5; i++) {
                Assert.assertTrue(datastores.contains("datastore" + i));
            }
        } finally {
            for (Datastore ds : opened) {
                ds.close();
            }
        }

    }

    @Test
    public void listDatastoresWithSlashes() throws Exception {
        Datastore ds = manager.openDatastore("datastore/mynewdatastore");
        try {
            List<String> datastores = manager.listAllDatastores();

            Assert.assertEquals(1, datastores.size());
            Assert.assertEquals("datastore/mynewdatastore", datastores.get(0));
        } finally {
            ds.close();
        }

    }

    @Test
    public void listEmptyDatastore() throws Exception {
        List<String> datastores = manager.listAllDatastores();
        Assert.assertEquals(0, datastores.size());
    }

    @Test
    public void multithreadedDatastoreCreation() throws Exception {
        int numberOfThreads = 25;
        final List<Datastore> datastores = Collections.synchronizedList(new ArrayList<Datastore>());
        final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());
        final CountDownLatch latch = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<Thread>(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            threads.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        latch.await(20, TimeUnit.SECONDS);
                        Datastore ds = manager.openDatastore("sameDatastoreName");
                        datastores.add(ds);
                    } catch(Throwable e) {
                        exceptions.add(e);
                    }
                }
            }));
        }
        for (Thread thread : threads) {
            thread.start();
        }
        // Release many threads simultaneously
        latch.countDown();
        // Wait for all threads to finish
        for (Thread thread : threads) {
            thread.join();
        }
        Assert.assertTrue("There should be no exceptions.", exceptions.isEmpty());
        Assert.assertEquals("There should be the correct number of datastores", numberOfThreads, datastores.size());
        Datastore ds0 = datastores.get(0);
        for (Datastore ds : datastores) {
            Assert.assertSame("The datastore instances should all be the same", ds0, ds);
        }
    }

    @Test
    public void datastoreInstanceNotReusedAfterClose() throws Exception {
        Datastore ds1 = manager.openDatastore("ds1");
        ds1.close();
        Datastore ds2 = manager.openDatastore("ds1");
        Assert.assertNotSame("The Datastore instances should not be the same.", ds1, ds2);
    }
}
