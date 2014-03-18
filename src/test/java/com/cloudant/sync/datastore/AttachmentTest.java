package com.cloudant.sync.datastore;

import com.cloudant.sync.util.Misc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tomblench on 12/03/2014.
 */
public class AttachmentTest extends BasicDatastoreTestBase {

    @Test
    public void setAndGetAttachmentsTest() {
        String attachmentName = "attachment_1.txt";
        BasicDocumentRevision rev_1 = datastore.createDocument(bodyOne);
        File f = new File("fixture", attachmentName);
        UnsavedFileAttachment att = new UnsavedFileAttachment(f, "text/plain");
        List<UnsavedFileAttachment> atts = new ArrayList<UnsavedFileAttachment>();
        atts.add(att);
        DocumentRevision newRevision = null;
        try {
            newRevision = datastore.updateAttachments(rev_1, atts);
        } catch (ConflictException ce){
            Assert.fail("ConflictException thrown: "+ce);
        } catch (IOException ioe) {
            Assert.fail("IOException thrown: "+ioe);
        }
        // get attachment...
        try {
            byte[] expectedSha1 = Misc.getSha1(new FileInputStream(f));

            SavedAttachment savedAtt = (SavedAttachment) datastore.getAttachment(newRevision, attachmentName);
            Assert.assertArrayEquals(expectedSha1, savedAtt.key);

            SavedAttachment savedAtt2 = (SavedAttachment) datastore.attachmentsForRevision(newRevision).get(0);
            Assert.assertArrayEquals(expectedSha1, savedAtt2.key);
        } catch (FileNotFoundException fnfe) {
            Assert.fail("FileNotFoundException thrown "+fnfe);
        }
    }

    // this test should throw a conflictexception when we try to add attachments to an old revision
    @Test(expected = ConflictException.class)
    public void setAttachmentsConflictTest() throws ConflictException {
        String attachmentName = "attachment_1.txt";
        BasicDocumentRevision rev_1 = datastore.createDocument(bodyOne);

        BasicDocumentRevision rev_2;
        try {
            datastore.updateDocument(rev_1.getId(), rev_1.getRevision(), bodyTwo);
        } catch (ConflictException ce) {
            Assert.fail("ConflictException thrown: "+ce);
        }
        File f = new File("fixture", attachmentName);
        UnsavedFileAttachment att = new UnsavedFileAttachment(f, "text/plain");
        List<UnsavedFileAttachment> atts = new ArrayList<UnsavedFileAttachment>();
        atts.add(att);
        DocumentRevision newRevision = null;
        try {
            newRevision = datastore.updateAttachments(rev_1, atts);
        } catch (IOException ioe) {
            Assert.fail("IOException thrown: "+ioe);
        }
    }

    @Test
    public void createDeleteAttachmentsTest() {

        String attachmentName = "attachment_1.txt";
        BasicDocumentRevision rev_1 = datastore.createDocument(bodyOne);
        File f = new File("fixture", attachmentName);
        UnsavedFileAttachment att = new UnsavedFileAttachment(f, "text/plain");
        List<UnsavedFileAttachment> atts = new ArrayList<UnsavedFileAttachment>();
        atts.add(att);
        DocumentRevision rev2 = null;
        try {
            rev2 = datastore.updateAttachments(rev_1, atts);
            Assert.assertNotNull("Revision null", rev2);
        } catch (ConflictException ce){
            Assert.fail("ConflictException thrown: "+ce);
        } catch (IOException ioe) {
            Assert.fail("IOException thrown: "+ioe);
        }

        DocumentRevision rev3 = null;
        try {
            rev3 = datastore.removeAttachments(rev2, new String[]{attachmentName});
            Assert.assertNotNull("Revision null", rev3);
        } catch (Exception e) {
            Assert.fail("Exception thrown: "+e);
        }

    }

    @Test
    public void deleteNonexistentAttachmentsTest() {

        String attachmentName = "attachment_1.txt";
        BasicDocumentRevision rev1 = datastore.createDocument(bodyOne);

        DocumentRevision rev2 = null;
        try {
            rev2 = datastore.removeAttachments(rev1, new String[]{attachmentName});
            Assert.assertEquals("Revisions not the same", rev2, rev1);
        } catch (Exception e) {
            Assert.fail("Exception thrown: "+e);
        }

    }

    @Test
    public void attachmentsForRevisionTest() {
        BasicDocumentRevision rev_1 = datastore.createDocument(bodyOne);
        UnsavedFileAttachment att1 = new UnsavedFileAttachment(new File("fixture", "attachment_1.txt"), "text/plain");
        UnsavedFileAttachment att2 = new UnsavedFileAttachment(new File("fixture", "attachment_2.txt"), "text/plain");
        List<UnsavedFileAttachment> atts = new ArrayList<UnsavedFileAttachment>();
        atts.add(att1);
        atts.add(att2);
        DocumentRevision newRevision = null;
        try {
            newRevision = datastore.updateAttachments(rev_1, atts);
            List<? extends Attachment> attsForRev = datastore.attachmentsForRevision(newRevision);
            Assert.assertEquals("Didn't get expected number of attachments", atts.size(), attsForRev.size());
        } catch (ConflictException ce){
            Assert.fail("ConflictException thrown: "+ce);
        } catch (IOException ioe) {
            Assert.fail("IOException thrown: "+ioe);
        }
    }

}
