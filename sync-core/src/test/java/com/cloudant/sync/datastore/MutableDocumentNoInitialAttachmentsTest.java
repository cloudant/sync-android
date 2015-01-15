package com.cloudant.sync.datastore;

import com.cloudant.sync.util.TestUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Created by tomblench on 06/08/2014.
 */
public class MutableDocumentNoInitialAttachmentsTest extends BasicDatastoreTestBase{


    BasicDocumentRevision saved;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "doc1";
        rev.body  = bodyOne;
        saved = datastore.createDocumentFromRevision(rev);
    }

    // Update revision with updated body
    @Test
    public void updateBody() throws ConflictException, IOException {
        MutableDocumentRevision update = saved.mutableCopy();
        update.body = bodyTwo;
        BasicDocumentRevision updated = datastore.updateDocumentFromRevision(update);
        Assert.assertNotNull("Updated DocumentRevision is null", updated);
    }

    // Update revision with updated body set to null
    @Test(expected = NullPointerException.class)
    public void updateBodyNull() throws ConflictException, IOException {
        MutableDocumentRevision update = saved.mutableCopy();
        update.body = null;
        BasicDocumentRevision updated = datastore.updateDocumentFromRevision(update);
        Assert.fail("Expected NullPointerException");
    }

    // Update revision with updated body and new attachment
    @Test
    public void updateBodyAndAttachments() throws ConflictException, IOException {
        MutableDocumentRevision update = saved.mutableCopy();
        update.body = bodyTwo;
        String attachmentName = "attachment_1.txt";
        File f = TestUtils.loadFixture("fixture/" + attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "text/plain");
        update.attachments.put(attachmentName, att);
        BasicDocumentRevision updated = datastore.updateDocumentFromRevision(update);
        Assert.assertNotNull("Updated DocumentRevision is null", updated);
        Attachment retrievedAtt = datastore.getAttachment(updated, attachmentName);
        Assert.assertNotNull("Retrieved attachment is null", retrievedAtt);
        // also get the attachments through the documentrev
        Assert.assertEquals("Revision should have 1 attachments", 1, updated.getAttachments().size());
        Assert.assertNotNull("Document attachment 1 is null", updated.getAttachments().get(attachmentName));
    }

    // Update revision with updated body, explicitly set attachments to null
    @Test
    public void updateBodySetNullAttachments() throws ConflictException, IOException {
        MutableDocumentRevision update = saved.mutableCopy();
        update.body = bodyTwo;
        update.attachments = null;
        BasicDocumentRevision updated = datastore.updateDocumentFromRevision(update);
        Assert.assertNotNull("Updated DocumentRevision is null", updated);
        // also get the attachments through the documentrev
        Assert.assertEquals("Revision should have 0 attachments", 0, updated.getAttachments().size());
    }

    // Update revision, don't change body, add new attachment
    @Test
    public void updateAttachments() throws ConflictException, IOException {
        MutableDocumentRevision update = saved.mutableCopy();
        String attachmentName = "attachment_1.txt";
        File f = TestUtils.loadFixture("fixture/"+attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "text/plain");
        update.attachments.put(attachmentName, att);
        BasicDocumentRevision updated = datastore.updateDocumentFromRevision(update);
        Assert.assertNotNull("Updated DocumentRevision is null", updated);
        Attachment retrievedAtt = datastore.getAttachment(updated, attachmentName);
        Assert.assertNotNull("Retrieved attachment is null", retrievedAtt);
        // also get the attachments through the documentrev
        Assert.assertEquals("Revision should have 1 attachments", 1, updated.getAttachments().size());
        Assert.assertNotNull("Document attachment 1 is null", updated.getAttachments().get(attachmentName));
    }

    // Test transactionality - update body and try to set non-existent attachment
    // check that this fails correctly and that no new revision is created
    @Test
    public void updateBodySetInvalidAttachments() throws ConflictException {
        MutableDocumentRevision update = saved.mutableCopy();
        update.body = bodyTwo;
        String attachmentName = "doesnt_exist_attachment";
        File f = TestUtils.loadFixture("fixture/"+ attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "text/plain");
        update.attachments.put(attachmentName, att);
        BasicDocumentRevision updated = null;
        try {
            updated = datastore.updateDocumentFromRevision(update);
            Assert.fail("Expected IOException; not thrown");
        } catch (IOException ioe) {
            ;
        }
        // document should not be updated
        Assert.assertNull("Updated DocumentRevision is not null", updated);
        BasicDocumentRevision retrieved = this.datastore.getDocument(saved.getId());
        Assert.assertEquals("Document should not be updated", retrieved.getRevision(), saved.getRevision());
        // also get the attachments through the documentrev
        Assert.assertEquals("Revision should have 0 attachments", 0, retrieved.getAttachments().size());
    }

}
