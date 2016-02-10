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
import java.util.HashMap;

/**
 * Created by tomblench on 06/08/2014.
 */
/*
public class MutableDocumentInitialAttachmentsTest extends BasicDatastoreTestBase{

    BasicDocumentRevision saved;
    String initialAtt1 = "attachment_1.txt";
    String initialAtt2 = "attachment_2.txt";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "doc1";
        rev.body  = bodyOne;
        rev.attachments.put(initialAtt1, new UnsavedFileAttachment(TestUtils.loadFixture("fixture/" + initialAtt1),
                "text/plain"));
        rev.attachments.put(initialAtt2, new UnsavedFileAttachment(TestUtils.loadFixture("fixture/"+ initialAtt2),
                "text/plain"));
        saved = datastore.createDocumentFromRevision(rev);
        Assert.assertNotNull("Saved DocumentRevision is null", saved);
        Attachment retrievedAtt1 = datastore.getAttachment(saved, initialAtt1);
        Assert.assertNotNull("Retrieved attachment is null", retrievedAtt1);
        Attachment retrievedAtt2 = datastore.getAttachment(saved, initialAtt2);
        Assert.assertNotNull("Retrieved attachment is null", retrievedAtt2);
        // also get the attachments through the documentrev
        Assert.assertEquals("Revision should have 2 attachments", 2,
                saved.getAttachments().size());
        Assert.assertNotNull("Document attachment 1 is null",
                saved.getAttachments().get(initialAtt1));
        Assert.assertNotNull("Document attachment 2 is null",
                saved.getAttachments().get(initialAtt2));
    }

    // Update body without changing attachments
    @Test
    public void updateBody() throws Exception {
        MutableDocumentRevision update = saved.mutableCopy();
        update.body = bodyTwo;
        BasicDocumentRevision updated = datastore.updateDocumentFromRevision(update);
        Assert.assertNotNull("Updated DocumentRevision is null", updated);
        Attachment retrievedAtt1 = datastore.getAttachment(updated, initialAtt1);
        Assert.assertNotNull("Retrieved attachment is null", retrievedAtt1);
        Attachment retrievedAtt2 = datastore.getAttachment(updated, initialAtt2);
        Assert.assertNotNull("Retrieved attachment is null", retrievedAtt2);
        // also get the attachments through the documentrev
        Assert.assertEquals("Revision should have 2 attachments", 2,
                updated.getAttachments().size());
        Assert.assertNotNull("Document attachment 1 is null",
                updated.getAttachments().get(initialAtt1));
        Assert.assertNotNull("Document attachment 2 is null",
                updated.getAttachments().get(initialAtt2));
    }

    // Update the attachments without changing the body, remove attachments
    @Test
    public void removeAttachment() throws Exception {
        MutableDocumentRevision update = saved.mutableCopy();
        update.attachments.remove(initialAtt1);
        BasicDocumentRevision updated = datastore.updateDocumentFromRevision(update);
        Assert.assertNotNull("Updated DocumentRevision is null", updated);
        Attachment retrievedAtt1 = datastore.getAttachment(updated, initialAtt1);
        Assert.assertNull("Retrieved attachment is not null", retrievedAtt1);
        Attachment retrievedAtt2 = datastore.getAttachment(updated, initialAtt2);
        Assert.assertNotNull("Retrieved attachment is null", retrievedAtt2);
        // also get the attachments through the documentrev
        Assert.assertEquals("Revision should have 1 attachments", 1,
                updated.getAttachments().size());
        Assert.assertNotNull("Document attachment 2 is null",
                updated.getAttachments().get(initialAtt2));
    }

    // Update the attachments without changing the body, add attachments
    @Test
    public void addAttachment() throws Exception {
        MutableDocumentRevision update = saved.mutableCopy();
        String attachmentName = "bonsai-boston.jpg";
        File f = TestUtils.loadFixture("fixture/"+ attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "image/jpeg");
        update.attachments.put(attachmentName, att);
        BasicDocumentRevision updated = datastore.updateDocumentFromRevision(update);
        Assert.assertNotNull("Updated DocumentRevision is null", updated);
        // check the new attachment is present
        Attachment retrievedAtt = datastore.getAttachment(updated, attachmentName);
        Assert.assertNotNull("Retrieved attachment 1 is null", retrievedAtt);
        // check the initial attachments are still there
        Attachment retrievedAtt2 = datastore.getAttachment(updated, initialAtt1);
        Assert.assertNotNull("Retrieved attachment 2 is null", retrievedAtt2);
        Attachment retrievedAtt3 = datastore.getAttachment(updated, initialAtt2);
        Assert.assertNotNull("Retrieved attachment 3 is null", retrievedAtt3);
        // also get the attachments through the documentrev
        Assert.assertEquals("Revision should have 3 attachments", 3,
                updated.getAttachments().size());
        Assert.assertNotNull("Document attachment 1 is null",
                updated.getAttachments().get(attachmentName));
        Assert.assertNotNull("Document attachment 2 is null",
                updated.getAttachments().get(initialAtt1));
        Assert.assertNotNull("Document attachment 3 is null",
                updated.getAttachments().get(initialAtt1));
    }

    // Update the attachments without changing the body, remove all attachments by setting null
    // for attachments hash
    @Test
    public void removeAllAttachmentsNull() throws Exception {
        MutableDocumentRevision update = saved.mutableCopy();
        update.attachments = null;
        BasicDocumentRevision updated = datastore.updateDocumentFromRevision(update);
        Assert.assertNotNull("Updated DocumentRevision is null", updated);
        // check no attachments present
        Assert.assertEquals("Attachment count not 0", 0,
                datastore.attachmentsForRevision(updated).size());
        // also get the attachments through the documentrev
        Assert.assertEquals("Revision should have 0 attachments", 0,
                updated.getAttachments().size());
    }

    // Update the attachments without changing the body, remove all attachments by setting an
    // empty hash
    @Test
    public void removeAllAttachmentsEmpty() throws Exception {
        MutableDocumentRevision update = saved.mutableCopy();
        update.attachments = new HashMap<String, Attachment>();
        BasicDocumentRevision updated = datastore.updateDocumentFromRevision(update);
        Assert.assertNotNull("Updated DocumentRevision is null", updated);
        // check no attachments present
        Assert.assertEquals("Attachment count not 0", 0,
                datastore.attachmentsForRevision(updated).size());
        // also get the attachments through the documentrev
        Assert.assertEquals("Revision should have 0 attachments", 0,
                updated.getAttachments().size());
    }

    // Copy an attachment from one document to another
    @Test
    public void copyAttachment() throws Exception {
        MutableDocumentRevision update = saved.mutableCopy();
        MutableDocumentRevision copy = new MutableDocumentRevision();
        copy.body = DocumentBodyFactory.create(
                "{\"description\": \"this is a document with copied attachments\"}".getBytes());
        copy.attachments = update.attachments;
        BasicDocumentRevision copied = datastore.createDocumentFromRevision(copy);
        // check the initial attachments are now associated with the copy
        Attachment retrievedAtt1 = datastore.getAttachment(copied, initialAtt1);
        Assert.assertNotNull("Retrieved attachment 1 is null", retrievedAtt1);
        Attachment retrievedAtt2 = datastore.getAttachment(copied, initialAtt2);
        Assert.assertNotNull("Retrieved attachment 2 is null", retrievedAtt2);
        // also get the attachments through the documentrev
        Assert.assertEquals("Revision should have 2 attachments", 2,
                copied.getAttachments().size());
        Assert.assertNotNull("Document attachment 1 is null",
                copied.getAttachments().get(initialAtt1));
        Assert.assertNotNull("Document attachment 2 is null",
                copied.getAttachments().get(initialAtt2));
    }

    // Try updating a 'stale' mutable copy and check that it fails and rolls back correctly
    @Test
    public void removeAllAttachmentsNullConflictTest() throws Exception {
        MutableDocumentRevision updateStale = saved.mutableCopy();
        MutableDocumentRevision update = saved.mutableCopy();
        update.body = bodyTwo;
        datastore.updateDocumentFromRevision(update);
        updateStale.attachments = null;
        updateStale.body = DocumentBodyFactory.create(
                "{\"description\": \"this update should not get committed\"}".getBytes());
        try {
            datastore.updateDocumentFromRevision(updateStale);
            Assert.fail("Expected ConflictException; not thrown");
        } catch (DocumentException ce) {
            ;
        }
        // check the original was intact
        BasicDocumentRevision retrieved = datastore.getDocument(saved.getId());
        Assert.assertEquals("Body has been updated", update.body.toString(),
                retrieved.getBody().toString());
        Assert.assertEquals("Attachments have been updated", 2,
                retrieved.getAttachments().size());
        Assert.assertNotNull("Document attachment 1 is null",
                retrieved.getAttachments().get(initialAtt1));
        Assert.assertNotNull("Document attachment 2 is null",
                retrieved.getAttachments().get(initialAtt2));
    }

    // Check that attachments on a retrieved DocumentRevision are immutable
    @Test
    public void documentAttachmentsAreImmutableTest() throws ConflictException, IOException {
        try {
            saved.getAttachments().put("blah", null);
            Assert.fail("UnsupportedOperationException not thrown");
        } catch (UnsupportedOperationException uoe) {
            ;
        }
        try {
            saved.getAttachments().remove(initialAtt1);
            Assert.fail("UnsupportedOperationException not thrown");
        } catch (UnsupportedOperationException uoe) {
            ;
        }
    }

    // check that we can create documents without setting the ID
    @Test
    public void createDocumentsWithAutoGeneratedId() throws Exception {
        MutableDocumentRevision rev1 = new MutableDocumentRevision();
        rev1.body = bodyOne;
        BasicDocumentRevision doc1 = datastore.createDocumentFromRevision(rev1);
        Assert.assertNotNull("Document is null", doc1);

        MutableDocumentRevision rev2 = new MutableDocumentRevision();
        rev2.body = bodyOne;
        BasicDocumentRevision doc2 = datastore.createDocumentFromRevision(rev2);
        Assert.assertNotNull("Document is null", doc2);
    }

}
*/