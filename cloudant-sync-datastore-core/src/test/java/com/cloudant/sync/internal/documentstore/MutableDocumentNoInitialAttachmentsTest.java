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

import com.cloudant.sync.documentstore.Attachment;
import com.cloudant.sync.documentstore.AttachmentException;
import com.cloudant.sync.documentstore.DocumentException;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.documentstore.UnsavedFileAttachment;
import com.cloudant.sync.util.TestUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

/**
 * Created by tomblench on 06/08/2014.
 */
public class MutableDocumentNoInitialAttachmentsTest extends BasicDatastoreTestBase{


    DocumentRevision saved;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        DocumentRevision rev = new DocumentRevision("doc1");
        rev.setBody(bodyOne);
        saved = datastore.createDocumentFromRevision(rev);
    }

    // Update revision with updated body
    @Test
    public void updateBody() throws Exception {
        DocumentRevision update = saved;
        update.setBody(bodyTwo);
        DocumentRevision updated = datastore.updateDocumentFromRevision(update);
        Assert.assertNotNull("Updated DocumentRevision is null", updated);
    }

    // Update revision with updated body set to null
    // TODO this probably should become become InvalidDocumentException
    @Test(expected = DocumentStoreException.class)
    public void updateBodyNull() throws Exception {
        DocumentRevision update = saved;
        update.setBody(null);
        DocumentRevision updated = datastore.updateDocumentFromRevision(update);
    }

    // Update revision with updated body and new attachment
    @Test
    public void updateBodyAndAttachments() throws Exception {
        DocumentRevision update = saved;
        update.setBody(bodyTwo);
        String attachmentName = "attachment_1.txt";
        File f = TestUtils.loadFixture("fixture/" + attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "text/plain");
        update.getAttachments().put(attachmentName, att);
        DocumentRevision updated = datastore.updateDocumentFromRevision(update);
        Assert.assertNotNull("Updated DocumentRevision is null", updated);
        Attachment retrievedAtt = datastore.getAttachment(updated.getId(), updated.getRevision(), attachmentName);
        Assert.assertNotNull("Retrieved attachment is null", retrievedAtt);
        // also get the attachments through the documentrev
        Assert.assertEquals("Revision should have 1 attachments", 1, updated.getAttachments().size());
        Assert.assertNotNull("Document attachment 1 is null", updated.getAttachments().get(attachmentName));
    }

    // Update revision with updated body, explicitly set attachments to null
    @Test
    public void updateBodySetNullAttachments() throws Exception {
        DocumentRevision update = saved;
        update.setBody(bodyTwo);
        update.setAttachments(null);
        DocumentRevision updated = datastore.updateDocumentFromRevision(update);
        Assert.assertNotNull("Updated DocumentRevision is null", updated);
        // also get the attachments through the documentrev
        Assert.assertEquals("Revision should have 0 attachments", 0, updated.getAttachments().size());
    }

    // Update revision, don't change body, add new attachment
    @Test
    public void updateAttachments() throws Exception {
        DocumentRevision update = saved;
        String attachmentName = "attachment_1.txt";
        File f = TestUtils.loadFixture("fixture/"+attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "text/plain");
        update.getAttachments().put(attachmentName, att);
        DocumentRevision updated = datastore.updateDocumentFromRevision(update);
        Assert.assertNotNull("Updated DocumentRevision is null", updated);
        Attachment retrievedAtt = datastore.getAttachment(updated.getId(), updated.getRevision(), attachmentName);
        Assert.assertNotNull("Retrieved attachment is null", retrievedAtt);
        // also get the attachments through the documentrev
        Assert.assertEquals("Revision should have 1 attachments", 1, updated.getAttachments().size());
        Assert.assertNotNull("Document attachment 1 is null", updated.getAttachments().get(attachmentName));
    }

    // Test transactionality - update body and try to set non-existent attachment
    // check that this fails correctly and that no new revision is created
    @Test
    public void updateBodySetInvalidAttachments() throws Exception {
        DocumentRevision update = saved;
        update.setBody(bodyTwo);
        String attachmentName = "doesnt_exist_attachment";
        File f = TestUtils.loadFixture("fixture/"+ attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "text/plain");
        update.getAttachments().put(attachmentName, att);
        DocumentRevision updated = null;
        try {
            updated = datastore.updateDocumentFromRevision(update);
            Assert.fail("Expected AttachmentException; not thrown");
        } catch (AttachmentException e) {
            ;
        }
        // document should not be updated
        Assert.assertNull("Updated DocumentRevision is not null", updated);
        DocumentRevision retrieved = this.datastore.getDocument(saved.getId());
        Assert.assertEquals("Document should not be updated", retrieved.getRevision(), saved.getRevision());
        // also get the attachments through the documentrev
        Assert.assertEquals("Revision should have 0 attachments", 0, retrieved.getAttachments().size());
    }

}
