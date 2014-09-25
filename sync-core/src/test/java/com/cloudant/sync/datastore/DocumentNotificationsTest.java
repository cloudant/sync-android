package com.cloudant.sync.datastore;

import java.io.IOException;
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
    public void notification_document_created() throws IOException {
        documentCreated = new CountDownLatch(1);
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.body = bodyOne;
        datastore.createDocumentFromRevision(rev);
        boolean ok = NotificationTestUtils.waitForSignal(documentCreated);
        Assert.assertTrue("Didn't receive document created event", ok);
    }

    @Test
    public void notification_document_updated() throws IOException {
        documentUpdated = new CountDownLatch(1);
        MutableDocumentRevision rev_1Mut = new MutableDocumentRevision();
        rev_1Mut.body = bodyOne;
        BasicDocumentRevision rev_1 = datastore.createDocumentFromRevision(rev_1Mut);
        validateNewlyCreatedDocument(rev_1);
        try {
            MutableDocumentRevision rev_2Mut = rev_1.mutableCopy();
            rev_2Mut.body = bodyTwo;
            datastore.updateDocumentFromRevision(rev_2Mut);
        } catch (ConflictException ce) {
            Assert.fail("Got ConflictException when updating");
        }
        boolean ok = NotificationTestUtils.waitForSignal(documentUpdated);
        Assert.assertTrue("Didn't receive document updated event", ok);
    }

    @Test
    public void notification_document_deleted() throws IOException {
        documentDeleted = new CountDownLatch(1);
        MutableDocumentRevision rev_1Mut = new MutableDocumentRevision();
        rev_1Mut.body = bodyOne;
        BasicDocumentRevision rev_1 = datastore.createDocumentFromRevision(rev_1Mut);
        try {
            datastore.deleteDocumentFromRevision(rev_1);
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
