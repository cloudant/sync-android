package com.cloudant.sync.datastore;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.cloudant.sync.notifications.DatabaseClosed;
import com.cloudant.sync.notifications.DatabaseCreated;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudant.sync.notifications.DatabaseOpened;
import com.cloudant.sync.notifications.DatabaseDeleted;
import com.cloudant.sync.util.TestUtils;
import com.google.common.eventbus.Subscribe;

public class DatabaseNotificationsTest {

    CountDownLatch databaseCreated, databaseOpened, databaseDeleted, databaseClosed;
    DatastoreManager datastoreManager;
    String datastoreManagerDir;

    @Before
    public void setUpClass() throws Exception {
        datastoreManagerDir = TestUtils
                .createTempTestingDir(DatabaseNotificationsTest.class.getName());
        datastoreManager = new DatastoreManager(datastoreManagerDir);
        datastoreManager.getEventBus().register(this);
    }

    @After
    public void setDownClass() {
        TestUtils.deleteTempTestingDir(datastoreManagerDir);
    }

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void notification_database_opened() {
        databaseOpened = new CountDownLatch(1);
        Datastore ds = datastoreManager.openDatastore("test123");
        try {
            boolean ok = NotificationTestUtils.waitForSignal(databaseOpened);
            Assert.assertTrue("Didn't receive database opened event", ok);
        } finally {
            ds.close();
        }
    }

    @Test
    public void notification_database_created() {
        databaseCreated = new CountDownLatch(1);
        Datastore ds = datastoreManager.openDatastore("test123");
        try {
            boolean ok = NotificationTestUtils.waitForSignal(databaseCreated);
            Assert.assertTrue("Didn't receive database created event", ok);
        } finally {
            ds.close();
        }
    }

    @Test
    public void notification_database_deleted() {
        databaseDeleted = new CountDownLatch(1);
        datastoreManager.openDatastore("test1234");
        try {
            datastoreManager.deleteDatastore("test1234");
        } catch (IOException e) {
            Assert.fail("Got IOException when deleting " + e);
        }
        boolean ok = NotificationTestUtils.waitForSignal(databaseDeleted);
        Assert.assertTrue("Didn't receive database deleted event", ok);
    }

    @Test
    public void notification_database_closed() {
        databaseClosed = new CountDownLatch((1));
        Datastore ds = datastoreManager.openDatastore("testDatabaseClosed");
        ds.getEventBus().register(this);
        ds.close();
        boolean ok = NotificationTestUtils.waitForSignal(databaseClosed);
        Assert.assertTrue("Did not received database closed event", ok);
    }

    @Test
    public void notification_databaseClosed_databaseManagerShouldPostDatabaseClosedEvent() {
        databaseClosed = new CountDownLatch((1));
        Datastore ds = datastoreManager.openDatastore("testDatabaseClosed");
        datastoreManager.getEventBus().register(this);
        ds.close();
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
