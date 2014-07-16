package com.cloudant.sync.datastore;

import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudant.sync.notifications.DocumentCreated;
import com.cloudant.sync.notifications.DocumentDeleted;
import com.cloudant.sync.notifications.DocumentUpdated;
import com.google.common.eventbus.Subscribe;

public class DocumentNotificationsTest extends BasicDatastoreTestBase {

    static CountDownLatch documentCreated, documentUpdated, documentDeleted;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        datastore.getEventBus().register(this);
    }

    @Test
    public void notification_document_created() {
        documentCreated = new CountDownLatch(1);
        datastore.createDocument(bodyOne);
        boolean ok = NotificationTestUtils.waitForSignal(documentCreated);
        Assert.assertTrue("Didn't receive document created event", ok);
    }

    @Test
    public void notification_document_updated() {
        documentUpdated = new CountDownLatch(1);
        BasicDocumentRevision rev_1 = datastore.createDocument(bodyOne);
        validateNewlyCreatedDocument(rev_1);
        try {
            datastore.updateDocument(rev_1.getId(), rev_1.getRevision(),
                    bodyTwo);
        } catch (ConflictException ce) {
            Assert.fail("Got ConflictException when updating");
        }
        boolean ok = NotificationTestUtils.waitForSignal(documentUpdated);
        Assert.assertTrue("Didn't receive document updated event", ok);
    }

    @Test
    public void notification_document_deleted() {
        documentDeleted = new CountDownLatch(1);
        BasicDocumentRevision rev = datastore.createDocument(bodyOne);
        try {
            datastore.deleteDocument(rev.getId(), rev.getRevision());
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
