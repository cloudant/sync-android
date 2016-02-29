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

import com.cloudant.sync.util.TestUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;



/**
 * Created by tomblench on 08/08/2014.
 */



/*
public class MutableDocumentCopyTest extends BasicDatastoreTestBase{

    BasicDocumentRevision saved;
    String initialAtt1 = "attachment_1.txt";
    String initialAtt2 = "attachment_2.txt";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "doc1";
        rev.body  = bodyOne;
        rev.attachments.put(initialAtt1, new UnsavedFileAttachment(TestUtils.loadFixture("fixture/"+ initialAtt1), "text/plain"));
        rev.attachments.put(initialAtt2, new UnsavedFileAttachment(TestUtils.loadFixture("fixture/"+ initialAtt2), "text/plain"));
        saved = datastore.createDocumentFromRevision(rev);
        Assert.assertNotNull("Saved DocumentRevision is null", saved);
        Attachment retrievedAtt1 = datastore.getAttachment(saved, initialAtt1);
        Assert.assertNotNull("Retrieved attachment is null", retrievedAtt1);
        Attachment retrievedAtt2 = datastore.getAttachment(saved, initialAtt2);
        Assert.assertNotNull("Retrieved attachment is null", retrievedAtt2);
        // also get the attachments through the documentrev
        Assert.assertEquals("Revision should have 2 attachments", 2, saved.getAttachments().size());
        Assert.assertNotNull("Document attachment 1 is null", saved.getAttachments().get(initialAtt1));
        Assert.assertNotNull("Document attachment 2 is null", saved.getAttachments().get(initialAtt2));
    }

    // Add a document from a mutableCopy, with attachments
    @Test
    public void createFromCopy() throws Exception {
        MutableDocumentRevision update = saved.mutableCopy();
        update.body = bodyTwo;
        update.docId = "copy";
        String attachmentName = "bonsai-boston.jpg";
        File f = TestUtils.loadFixture("fixture/"+ attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "image/jpeg");
        update.attachments.put(attachmentName, att);
        BasicDocumentRevision updated = datastore.createDocumentFromRevision(update);
        Assert.assertNotNull("Updated document is null", updated);
        BasicDocumentRevision retrieved = datastore.getDocument(update.docId);
        Assert.assertNotNull("Retrieved document is null", retrieved);
        // check the new attachment is present
        Attachment retrievedAtt = datastore.getAttachment(updated, attachmentName);
        Assert.assertNotNull("Retrieved attachment 1 is null", retrievedAtt);
        // check the initial attachments are still there
        Attachment retrievedAtt2 = datastore.getAttachment(updated, initialAtt1);
        Assert.assertNotNull("Retrieved attachment 2 is null", retrievedAtt2);
        Attachment retrievedAtt3 = datastore.getAttachment(updated, initialAtt2);
        Assert.assertNotNull("Retrieved attachment 3 is null", retrievedAtt3);
        // also get the attachments through the documentrev
        Assert.assertEquals("Revision should have 2 attachments", 3, saved.getAttachments().size());
        Assert.assertNotNull("Document attachment 1 is null", saved.getAttachments().get(initialAtt1));
        Assert.assertNotNull("Document attachment 2 is null", saved.getAttachments().get(initialAtt2));
        Assert.assertNotNull("Document attachment 2 is null", saved.getAttachments().get(attachmentName));
    }

    // Add a document from a mutableCopy, remove the attachments first
    @Test
    public void createFromCopyRemoveAttachments() throws Exception {
        MutableDocumentRevision update = saved.mutableCopy();
        update.body = bodyTwo;
        update.docId = "copy";
        update.attachments = null;
        BasicDocumentRevision updated = datastore.createDocumentFromRevision(update);
        Assert.assertNotNull("Updated document is null", updated);
        BasicDocumentRevision retrieved = datastore.getDocument(update.docId);
        Assert.assertNotNull("Retrieved document is null", retrieved);
        // check the new attachment is present
        Assert.assertEquals("Attachment count not 0", 0, datastore.attachmentsForRevision(updated).size());
        // also get the attachments through the documentrev
        Assert.assertEquals("Revision should have 0 attachments", 0, updated.getAttachments().size());
    }

    // Add a document from a mutableCopy, check that re-using ID fails
    @Test(expected = DocumentException.class)
    public void createFromCopyFailDuplicateId() throws Exception{
        MutableDocumentRevision update = saved.mutableCopy();
        update.body = bodyTwo;
        datastore.createDocumentFromRevision(update);
    }

}*/
