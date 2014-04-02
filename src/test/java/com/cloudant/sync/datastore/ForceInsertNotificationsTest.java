package com.cloudant.sync.datastore;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudant.sync.notifications.DocumentCreated;
import com.cloudant.sync.notifications.DocumentUpdated;
import com.google.common.eventbus.Subscribe;

public class ForceInsertNotificationsTest extends BasicDatastoreTestBase {

    static CountDownLatch documentCreated, documentUpdated;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        datastore.getEventBus().register(this);
    }
    
    @Test
    public void notification_forceinsert() {
        documentUpdated = new CountDownLatch(1);
        documentCreated = new CountDownLatch(2); // 2 because the call to createDocument will also fire
        // create a document and insert the first revision
        BasicDocumentRevision doc1_rev1 = datastore.createDocument(bodyOne);   

        ArrayList<String> revisionHistory = new ArrayList<String>();
        revisionHistory.add(doc1_rev1.getRevision());
        
        // now do a force insert - we should get an updated event as it's already there
        datastore.forceInsert(doc1_rev1, revisionHistory, null);
        boolean ok1 = NotificationTestUtils.waitForSignal(documentUpdated);
        Assert.assertTrue("Didn't receive document updated event", ok1);

        // now do a force insert but with a different id - we should get a (2nd) created event
        doc1_rev1.setId("new-id-12345");
        datastore.forceInsert(doc1_rev1, revisionHistory, null);
        boolean ok2 = NotificationTestUtils.waitForSignal(documentCreated);
        Assert.assertTrue("Didn't receive document created event", ok2);
    }

    @Subscribe
    public void onDocumentCreated(DocumentCreated dc) {
        documentCreated.countDown();
    }

    @Subscribe
    public void onDocumentUpdated(DocumentUpdated du) {
        documentUpdated.countDown();
    }
    
}
