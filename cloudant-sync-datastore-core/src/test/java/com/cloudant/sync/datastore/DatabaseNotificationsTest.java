/**
 * Copyright (c) 2015 Cloudant, Inc. All rights reserved.
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

import com.cloudant.sync.event.Subscribe;
import com.cloudant.sync.notifications.DatabaseClosed;
import com.cloudant.sync.notifications.DatabaseCreated;
import com.cloudant.sync.notifications.DatabaseDeleted;
import com.cloudant.sync.notifications.DatabaseOpened;
import com.cloudant.sync.util.TestUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class DatabaseNotificationsTest {

    CountDownLatch databaseCreated, databaseOpened, databaseDeleted, databaseClosed;
    String datastoreManagerDir;

    @Before
    public void setUpClass() throws Exception {
        datastoreManagerDir = TestUtils
                .createTempTestingDir(DatabaseNotificationsTest.class.getName());
    }

    @After
    public void setDownClass() {
        TestUtils.deleteTempTestingDir(datastoreManagerDir);
    }

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void notification_database_opened() throws Exception{
        databaseOpened = new CountDownLatch(1);
        DocumentStore.getEventBus().register(this);
        DocumentStore documentStore = DocumentStore.getInstance(new File(datastoreManagerDir, "test123"));
        try {
            boolean ok = NotificationTestUtils.waitForSignal(databaseOpened);
            Assert.assertTrue("Didn't receive database opened event", ok);
        } finally {
            documentStore.close();
        }
    }

    @Test
    public void notification_database_created() throws Exception {
        databaseCreated = new CountDownLatch(1);
        DocumentStore.getEventBus().register(this);
        DocumentStore documentStore = DocumentStore.getInstance(new File(datastoreManagerDir, "test123"));
        try {
            boolean ok = NotificationTestUtils.waitForSignal(databaseCreated);
            Assert.assertTrue("Didn't receive database created event", ok);
        } finally {
            documentStore.close();
        }
    }

    @Test
    public void notification_database_deleted() throws Exception {
        databaseDeleted = new CountDownLatch(1);
        DocumentStore ds = DocumentStore.getInstance(new File(datastoreManagerDir, "test1234"));
        DocumentStore.getEventBus().register(this);
        try {
            ds.delete();
        } catch (IOException e) {
            Assert.fail("Got IOException when deleting " + e);
        }
        boolean ok = NotificationTestUtils.waitForSignal(databaseDeleted);
        Assert.assertTrue("Didn't receive database deleted event", ok);
    }

    @Test
    public void notification_database_closed() throws Exception{
        databaseClosed = new CountDownLatch((1));
        DocumentStore documentStore = DocumentStore.getInstance(new File(datastoreManagerDir, "testDatabaseClosed"));
        DocumentStore.getEventBus().register(this);
        documentStore.close();
        boolean ok = NotificationTestUtils.waitForSignal(databaseClosed);
        Assert.assertTrue("Did not received database closed event", ok);
    }

    @Subscribe
    public void onDatabaseCreated(DatabaseCreated dc) {
        if(databaseCreated != null) {
            databaseCreated.countDown();
        }
    }

    @Subscribe
    public void onDatabaseOpened(DatabaseOpened dd) {
        if(databaseOpened != null) {
            databaseOpened.countDown();
        }
    }

    @Subscribe
    public void onDatabaseDeleted(DatabaseDeleted dd) {
        if(databaseDeleted != null) {
            databaseDeleted.countDown();
        }
    }

    @Subscribe
    public void onDatabaseClosed(DatabaseClosed dc) {
        if(databaseClosed != null) {
            databaseClosed.countDown();
        }
    }
}
