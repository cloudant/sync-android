package com.cloudant.sync.datastore;

import com.cloudant.sync.notifications.DatabaseClosed;
import com.cloudant.sync.notifications.DatabaseCreated;
import com.cloudant.sync.notifications.DatabaseOpened;
import com.cloudant.sync.util.TestUtils;
import com.google.common.eventbus.Subscribe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;

/**
 * Uses pattern called EventRecorder to assert certain and only certain
 * events are fired.
 *
 * {@code DatabaseNotificationsTest} uses {@code CountDownLatch} to assert
 * certain events are fired, but it is hard to assert certain events are not
 * fired. That is the reason this test suit is created.
 *
 * We should agree on one approach and merge these two tests suites.
 */
public class DatabaseNotificationsMoreTest {

    List<DatabaseCreated> databaseCreated = new ArrayList<DatabaseCreated>();
    List<DatabaseOpened> databaseOpened = new ArrayList<DatabaseOpened>();
    List<DatabaseClosed> databaseClosed = new ArrayList<DatabaseClosed>();
    DatastoreManager datastoreManager;
    String datastoreManagerDir;

    @Before
    public void setUp() throws Exception {
        datastoreManagerDir = TestUtils
                .createTempTestingDir(DatabaseNotificationsMoreTest.class.getName());
        datastoreManager = new DatastoreManager(datastoreManagerDir);
        datastoreManager.getEventBus().register(this);
        this.clearAllEventList();
    }

    @After
    public void setDown() {
        TestUtils.deleteTempTestingDir(datastoreManagerDir);
    }

    @Test
    public void notification_database_opened() {
        Datastore ds = datastoreManager.openDatastore("test123");
        try {
            Assert.assertThat(databaseCreated, hasSize(1));
            Assert.assertThat(databaseOpened, hasSize(1));
            Assert.assertEquals("test123", databaseCreated.get(0).dbName);
            Assert.assertEquals("test123", databaseOpened.get(0).dbName);
        } finally {
            ds.close();
        }
    }

    @Test
    public void notification_database_openedTwice() {
        Datastore ds = datastoreManager.openDatastore("test123");
        Datastore ds1 = null ;
        try {
            Assert.assertNotNull(ds);
            Assert.assertThat(databaseCreated, hasSize(1));
            Assert.assertThat(databaseOpened, hasSize(1));
            Assert.assertEquals("test123", databaseCreated.get(0).dbName);
            Assert.assertEquals("test123", databaseOpened.get(0).dbName);

            ds1 = datastoreManager.openDatastore("test123");
            Assert.assertThat(databaseCreated, hasSize(1));
            Assert.assertThat(databaseOpened, hasSize(1));
            Assert.assertNotNull(ds1);
        } finally {
            ds.close(); //only need to close ds since underlying object for ds and ds1 is the same
        }
    }

    @Test
    public void notification_databaseOpenCloseAndThenOpenedAgain_databaseCreatedEventShouldBeOnlyFireOnce() {
        Datastore ds = datastoreManager.openDatastore("test123");
        Assert.assertThat(databaseCreated, hasSize(1));
        Assert.assertThat(databaseOpened, hasSize(1));
        Assert.assertThat(databaseClosed, hasSize(0));
        Assert.assertEquals("test123", databaseCreated.get(0).dbName);
        Assert.assertEquals("test123", databaseOpened.get(0).dbName);

        this.clearAllEventList();
        ds.close();
        Assert.assertThat(databaseCreated, hasSize(0));
        Assert.assertThat(databaseOpened, hasSize(0));
        Assert.assertThat(databaseClosed, hasSize(1));
        Assert.assertEquals("test123", databaseClosed.get(0).dbName);

        // After database is closed, when it is opened, the
        // DatabaseOpened event should be fired, but the
        // DatabaseCreated event should NOT be fired.
        this.clearAllEventList();
        Datastore ds1 = datastoreManager.openDatastore("test123");
        try {
            Assert.assertNotNull(ds1);
            Assert.assertThat(databaseCreated, hasSize(0));
            Assert.assertThat(databaseOpened, hasSize(1));
            Assert.assertThat(databaseClosed, hasSize(0));
            Assert.assertEquals("test123", databaseOpened.get(0).dbName);
        } finally {
            ds1.close();
        }
    }

    private void clearAllEventList() {
        databaseCreated.clear();
        databaseClosed.clear();
        databaseOpened.clear();
    }

    @Subscribe
    public void onDatabaseOpened(DatabaseCreated dc) {
        this.databaseCreated.add(dc);
    }

    @Subscribe
    public void onDatabaseOpened(DatabaseOpened dd) {
        this.databaseOpened.add(dd);
    }

    @Subscribe
    public void onDatabaseClosed(DatabaseClosed dc) {
        this.databaseClosed.add(dc);
    }

}
