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

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasSize;

import com.cloudant.sync.event.Subscribe;
import com.cloudant.sync.notifications.DocumentStoreClosed;
import com.cloudant.sync.notifications.DocumentStoreCreated;
import com.cloudant.sync.notifications.DocumentStoreOpened;
import com.cloudant.sync.util.TestUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

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

    List<DocumentStoreCreated> documentStoreCreated = new ArrayList<DocumentStoreCreated>();
    List<DocumentStoreOpened> databaseOpened = new ArrayList<DocumentStoreOpened>();
    List<DocumentStoreClosed> documentStoreClosed = new ArrayList<DocumentStoreClosed>();
    String datastoreManagerDir;

    @Before
    public void setUp() throws Exception {
        DocumentStore.getEventBus().register(this);
        datastoreManagerDir = TestUtils
                .createTempTestingDir(DatabaseNotificationsMoreTest.class.getName());
        this.clearAllEventList();
    }

    @After
    public void setDown() {
        TestUtils.deleteTempTestingDir(datastoreManagerDir);
        DocumentStore.getEventBus().unregister(this);
    }

    @Test
    public void notification_database_opened() throws Exception{
        DocumentStore ds = DocumentStore.getInstance(new File(datastoreManagerDir, "test123"));
        try {
            Assert.assertThat(documentStoreCreated, hasSize(1));
            Assert.assertThat(databaseOpened, hasSize(1));
            Assert.assertThat(documentStoreCreated.get(0).dbName, endsWith("test123"));
            Assert.assertThat(databaseOpened.get(0).dbName, endsWith("test123"));
        } finally {
            ds.close();
        }
    }

    @Test
    public void notification_database_openedTwice() throws Exception {
        DocumentStore ds = DocumentStore.getInstance(new File(datastoreManagerDir, "test123"));
        Database ds1 = null ;
        try {
            Assert.assertNotNull(ds);
            Assert.assertThat(documentStoreCreated, hasSize(1));
            Assert.assertThat(databaseOpened, hasSize(1));
            Assert.assertThat(documentStoreCreated.get(0).dbName, endsWith("test123"));
            Assert.assertThat(databaseOpened.get(0).dbName, endsWith("test123"));

            ds1 = DocumentStore.getInstance(new File(datastoreManagerDir, "test123")).database;
            Assert.assertThat(documentStoreCreated, hasSize(1));
            Assert.assertThat(databaseOpened, hasSize(1));
            Assert.assertNotNull(ds1);
        } finally {
            ds.close(); //only need to close ds since underlying object for ds and ds1 is the same
        }
    }

    @Test
    public void notification_databaseOpenCloseAndThenOpenedAgain_databaseCreatedEventShouldBeOnlyFireOnce() throws Exception {
        DocumentStore ds = DocumentStore.getInstance(new File(datastoreManagerDir, "test123"));
        Assert.assertThat(documentStoreCreated, hasSize(1));
        Assert.assertThat(databaseOpened, hasSize(1));
        Assert.assertThat(documentStoreClosed, hasSize(0));
        Assert.assertThat(documentStoreCreated.get(0).dbName, endsWith("test123"));
        Assert.assertThat(databaseOpened.get(0).dbName, endsWith("test123"));

        this.clearAllEventList();
        ds.close();
        Assert.assertThat(documentStoreCreated, hasSize(0));
        Assert.assertThat(databaseOpened, hasSize(0));
        Assert.assertThat(documentStoreClosed, hasSize(1));
        Assert.assertThat(documentStoreClosed.get(0).dbName, endsWith("test123"));

        // After database is closed, when it is opened, the
        // DatabaseOpened event should be fired, but the
        // DatabaseCreated event should NOT be fired.
        this.clearAllEventList();
        DocumentStore ds1 = DocumentStore.getInstance(new File(datastoreManagerDir, "test123"));
        try {
            Assert.assertNotNull(ds1);
            Assert.assertThat(documentStoreCreated, hasSize(0));
            Assert.assertThat(databaseOpened, hasSize(1));
            Assert.assertThat(documentStoreClosed, hasSize(0));
            Assert.assertThat(databaseOpened.get(0).dbName, endsWith("test123"));
        } finally {
            ds1.close();
        }
    }

    private void clearAllEventList() {
        documentStoreCreated.clear();
        documentStoreClosed.clear();
        databaseOpened.clear();
    }

    @Subscribe
    public void onDatabaseOpened(DocumentStoreCreated dc) {
        this.documentStoreCreated.add(dc);
    }

    @Subscribe
    public void onDatabaseOpened(DocumentStoreOpened dd) {
        this.databaseOpened.add(dd);
    }

    @Subscribe
    public void onDatabaseClosed(DocumentStoreClosed dc) {
        this.documentStoreClosed.add(dc);
    }

}
