/**
 * Copyright (c) 2014 Cloudant, Inc. All rights reserved.
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudant.sync.notifications.DocumentCreated;
import com.cloudant.sync.notifications.DocumentUpdated;
import com.google.common.eventbus.Subscribe;

public class ForceInsertTest extends BasicDatastoreTestBase {

    static CountDownLatch documentCreated, documentUpdated;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        datastore.getEventBus().register(this);
    }

    @Test
    public void notification_forceinsert() throws Exception {
        documentUpdated = new CountDownLatch(1);
        documentCreated = new CountDownLatch(2); // 2 because the call to createDocument will also fire
        // create a document and insert the first revision
        MutableDocumentRevision doc1_rev1Mut = new MutableDocumentRevision();
        doc1_rev1Mut.body = bodyOne;
        BasicDocumentRevision doc1_rev1 = datastore.createDocumentFromRevision(doc1_rev1Mut);

        ArrayList<String> revisionHistory = new ArrayList<String>();
        revisionHistory.add(doc1_rev1.getRevision());
        
        // now do a force insert - we should get an updated event as it's already there
        datastore.forceInsert(doc1_rev1, revisionHistory,null, null, false);
        boolean ok1 = NotificationTestUtils.waitForSignal(documentUpdated);
        Assert.assertTrue("Didn't receive document updated event", ok1);

        // now do a force insert but with a different id - we should get a (2nd) created event
        doc1_rev1.setId("new-id-12345");
        datastore.forceInsert(doc1_rev1, revisionHistory,null, null, false);
        boolean ok2 = NotificationTestUtils.waitForSignal(documentCreated);
        Assert.assertTrue("Didn't receive document created event", ok2);
    }

    @Test
    public void notification_forceinsertWithAttachments() throws Exception {

        // this test only makes sense if the data is inline base64 (there's no remote server to pull the attachment from)
        boolean pullAttachmentsInline = true;

        // create a document and insert the first revision
        MutableDocumentRevision doc1_rev1Mut = new MutableDocumentRevision();
        doc1_rev1Mut.body = bodyOne;
        BasicDocumentRevision doc1_rev1 = datastore.createDocumentFromRevision(doc1_rev1Mut);
        Map<String, Object> atts = new HashMap<String, Object>();
        Map<String, Object> att1 = new HashMap<String, Object>();

        atts.put("att1", att1);
        att1.put("data", new String(new Base64().encode("this is some data".getBytes())));
        att1.put("content_type", "text/plain");

        ArrayList<String> revisionHistory = new ArrayList<String>();
        revisionHistory.add(doc1_rev1.getRevision());

        // now do a force insert and then see if we get the attachment back
        datastore.forceInsert(doc1_rev1, revisionHistory, atts,null, pullAttachmentsInline);

        Attachment storedAtt = datastore.getAttachment(doc1_rev1, "att1");
        Assert.assertNotNull(storedAtt);

        Attachment noSuchAtt = datastore.getAttachment(doc1_rev1, "att2");
        Assert.assertNull(noSuchAtt);
    }

    @Test
    public void notification_forceinsertWithAttachmentsError() throws Exception{

        // this test only makes sense if the data is inline base64 (there's no remote server to pull the attachment from)
        boolean pullAttachmentsInline = true;

        // try and force an IOException when setting the attachment, and check everything is OK:

        // create a read only zero-length file where the extensions dir would go, to cause an IO exception
        File extensions = new File(datastore.datastoreDir + "/extensions");
        extensions.createNewFile();
        extensions.setWritable(false);

        MutableDocumentRevision doc1_rev1Mut = new MutableDocumentRevision();
        doc1_rev1Mut.body = bodyOne;
        BasicDocumentRevision doc1_rev1 = datastore.createDocumentFromRevision(doc1_rev1Mut);
        Map<String, Object> atts = new HashMap<String, Object>();
        Map<String, Object> att1 = new HashMap<String, Object>();

        atts.put("att1", att1);
        att1.put("data", new String(new Base64().encode("this is some data".getBytes())));
        att1.put("content_type", "text/plain");

        ArrayList<String> revisionHistory = new ArrayList<String>();
        revisionHistory.add(doc1_rev1.getRevision());
        doc1_rev1.setRevision("2-blah");
        revisionHistory.add(doc1_rev1.getRevision());
        // now do a force insert
        //catch the exception thrown se we can look into the database
        try {
            datastore.forceInsert(doc1_rev1, revisionHistory, atts, null, pullAttachmentsInline);
        } catch (DocumentException e){
            //do nothing.
        }

        // adding the attachment should have failed transactionally, so the rev should not exist as well
        Assert.assertFalse(datastore.containsDocument(doc1_rev1.getId(), doc1_rev1.getRevision()));

        Attachment storedAtt = datastore.getAttachment(doc1_rev1, "att1");
        Assert.assertNull(storedAtt);
    }

    // some tests don't care about these events so we need to check for null
    @Subscribe
    public void onDocumentCreated(DocumentCreated dc) {
        if (documentCreated != null)
            documentCreated.countDown();
    }

    @Subscribe
    public void onDocumentUpdated(DocumentUpdated du) {
        if (documentUpdated != null)
            documentUpdated.countDown();
    }
    
}
