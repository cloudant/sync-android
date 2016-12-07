/*
 * Copyright © 2015 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.internal.documentstore;

import com.cloudant.sync.documentstore.ConflictException;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.event.Subscribe;
import com.cloudant.sync.event.notifications.DocumentCreated;
import com.cloudant.sync.event.notifications.DocumentDeleted;
import com.cloudant.sync.event.notifications.DocumentUpdated;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public class DocumentNotificationsTest extends BasicDatastoreTestBase {

    CountDownLatch documentCreated, documentUpdated, documentDeleted;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        datastore.getEventBus().register(this);
        documentCreated = new CountDownLatch(1);
        documentUpdated = new CountDownLatch(1);
        documentDeleted = new CountDownLatch(1);
    }

    @Test
    public void notification_document_created() throws Exception {
        DocumentRevision rev = new DocumentRevision();
        rev.setBody(bodyOne);
        datastore.create(rev);
        boolean ok = NotificationTestUtils.waitForSignal(documentCreated);
        Assert.assertTrue("Didn't receive document created event", ok);
    }

    @Test
    public void notification_document_updated() throws Exception {
        DocumentRevision rev_1Mut = new DocumentRevision();
        rev_1Mut.setBody(bodyOne);
        DocumentRevision rev_1 = datastore.create(rev_1Mut);
        validateNewlyCreatedDocument(rev_1);
        try {
            DocumentRevision rev_2Mut = rev_1;
            rev_2Mut.setBody(bodyTwo);
            datastore.update(rev_2Mut);
        } catch (ConflictException ce) {
            Assert.fail("Got ConflictException when updating");
        }
        boolean ok = NotificationTestUtils.waitForSignal(documentUpdated);
        Assert.assertTrue("Didn't receive document updated event", ok);
    }

    @Test
    public void notification_document_deleted() throws Exception {
        DocumentRevision rev_1Mut = new DocumentRevision();
        rev_1Mut.setBody(bodyOne);
        DocumentRevision rev_1 = datastore.create(rev_1Mut);
        try {
            datastore.delete(rev_1);
        } catch (ConflictException ce) {
            Assert.fail("Got ConflictException when deleting");
        }
        boolean ok = NotificationTestUtils.waitForSignal(documentDeleted);
        Assert.assertTrue("Didn't receive document deleted event", ok);
    }

    @Subscribe
    public void onDocumentCreated(DocumentCreated dc) throws Exception {
        documentCreated.countDown();
    }

    @Subscribe
    public void onDocumentUpdated(DocumentUpdated du) {
        documentUpdated.countDown();
    }

    @Subscribe
    public void onDocumentDeleted(DocumentDeleted dd) {
        documentDeleted.countDown();
    }

}
