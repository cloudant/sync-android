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

import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.util.Misc;
import com.cloudant.sync.util.TestUtils;

import static org.hamcrest.CoreMatchers.is;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.SQLException;
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
        File f = TestUtils.loadFixture("fixture/"+attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "text/plain");
        List<Attachment> atts = new ArrayList<Attachment>();
        atts.add(att);
        BasicDocumentRevision newRevision = null;
        try {
            newRevision = datastore.updateAttachments(rev_1, atts);
        } catch (ConflictException ce){
            Assert.fail("ConflictException thrown: "+ce);
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

    // check that the transaction gets rolled back if one file is dodgy
    @Test
    public void setBadAttachmentsTest() {
        BasicDocumentRevision rev_1 = datastore.createDocument(bodyOne);
        Attachment att1 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/attachment_1.txt"), "text/plain");
        Attachment att2 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/nonexistentfile"), "text/plain");
        Attachment att3 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/attachment_2.txt"), "text/plain");
        List<Attachment> atts = new ArrayList<Attachment>();
        atts.add(att1);
        atts.add(att2);
        atts.add(att3);
        BasicDocumentRevision newRevision = null;
        try {
            newRevision = datastore.updateAttachments(rev_1, atts);
            Assert.assertNull("newRevision not null", newRevision);
            // now check that things got rolled back
            Cursor c1 = datastore.getSQLDatabase().rawQuery("select sequence from attachments;", null);
            Assert.assertEquals("Attachments table not empty", c1.getCount(), 0);
            Cursor c2 = datastore.getSQLDatabase().rawQuery("select sequence from revs;", null);
            Assert.assertEquals("Revs table not empty", c2.getCount(), 1);
        } catch (ConflictException ce) {
            Assert.fail("ConflictException thrown: " + ce);
        } catch (SQLException sqe) {
            Assert.fail("SQLException thrown: " + sqe);
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
        File f = TestUtils.loadFixture("fixture/"+attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "text/plain");
        List<Attachment> atts = new ArrayList<Attachment>();
        atts.add(att);
        BasicDocumentRevision newRevision = null;
        newRevision = datastore.updateAttachments(rev_1, atts);
    }

    @Test
    public void createDeleteAttachmentsTest() throws Exception{

        BasicDocumentRevision rev_1 = datastore.createDocument(bodyOne);
        Attachment att1 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/attachment_1.txt"), "text/plain");
        Attachment att2 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/attachment_2.txt"), "text/plain");
        List<Attachment> atts = new ArrayList<Attachment>();
        atts.add(att1);
        atts.add(att2);
        BasicDocumentRevision rev2 = null;

        rev2 = datastore.updateAttachments(rev_1, atts);
        Assert.assertNotNull("Revision null", rev2);


        BasicDocumentRevision rev3 = null;

            rev3 = datastore.removeAttachments(rev2, new String[]{"attachment_1.txt"});
            datastore.compact();
            Assert.assertNotNull("Revision null", rev3);

            // 1st shouldn't exist
            Attachment savedAtt1 = datastore.getAttachment(rev3, "attachment_1.txt");
            Assert.assertNull("Att1 not null", savedAtt1);

            // check we can read from the 2nd attachment, it wasn't deleted
            Attachment savedAtt2 = datastore.getAttachment(rev3, "attachment_2.txt");
            Assert.assertNotNull("Att2 null", savedAtt2);
            int i = savedAtt2.getInputStream().read();
            Assert.assertTrue("Can't read from Att2", i >= 0);

            // now sneakily look for them on disk
            File attachments = new File(datastore.datastoreDir + "/extensions/com.cloudant.attachments");
            int count = attachments.listFiles().length;
            Assert.assertEquals("Did not find 1 file in blob store", 1, count);


    }

    // TODO test 2 revs pointing at the same file in the blob store, deleting one doesn't cause it to be deleted

    @Test
    public void deleteNonexistentAttachmentsTest() {

        String attachmentName = "attachment_1.txt";
        BasicDocumentRevision rev1 = datastore.createDocument(bodyOne);

        BasicDocumentRevision rev2 = null;
        try {
            rev2 = datastore.removeAttachments(rev1, new String[]{attachmentName});
            Assert.assertEquals("Revisions not the same", rev2, rev1);
        } catch (Exception e) {
            Assert.fail("Exception thrown: "+e);
        }

    }

    @Test
    public void createDeleteAttachmentsFailTest() throws Exception {
        // check that an attachment 'going missing' from the blob store doesn't stop us deleting it
        // from the database
        String attachmentName = "attachment_1.txt";
        BasicDocumentRevision rev_1 = datastore.createDocument(bodyOne);
        File f = TestUtils.loadFixture("fixture/"+attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "text/plain");
        List<Attachment> atts = new ArrayList<Attachment>();
        atts.add(att);
        BasicDocumentRevision rev2 = null;
        rev2 = datastore.updateAttachments(rev_1, atts);
        Assert.assertNotNull("Revision null", rev2);

        BasicDocumentRevision rev3 = null;
        // clear out the attachment directory
        File attachments = new File(datastore.datastoreDir + "/extensions/com.cloudant.attachments");
        for(File attFile : attachments.listFiles()) {
            attFile.delete();
        }
        rev3 = datastore.removeAttachments(rev2, new String[]{attachmentName});
        Assert.assertNotNull("Revision null", rev3);

        // check that there are no attachments now associated with this doc
        Assert.assertTrue("Revision should have 0 attachments", datastore.attachmentsForRevision(rev3).isEmpty());
    }


    @Test
    public void attachmentsForRevisionTest() {
        BasicDocumentRevision rev_1 = datastore.createDocument(bodyOne);

        Attachment att1 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/attachment_1.txt"), "text/plain");
        Attachment att2 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/attachment_2.txt"), "text/plain");
        List<Attachment> atts = new ArrayList<Attachment>();
        atts.add(att1);
        atts.add(att2);
        BasicDocumentRevision newRevision = null;
        try {
            newRevision = datastore.updateAttachments(rev_1, atts);
            List<? extends Attachment> attsForRev = datastore.attachmentsForRevision(newRevision);
            Assert.assertEquals("Didn't get expected number of attachments", atts.size(), attsForRev.size());
        } catch (ConflictException ce){
            Assert.fail("ConflictException thrown: "+ce);
        }
    }

    @Test
    public void duplicateAttachmentTest() {
        BasicDocumentRevision doc1Rev1 = datastore.createDocument(bodyOne);
        BasicDocumentRevision doc2Rev1 = datastore.createDocument(bodyTwo);

        File attachmentFile = TestUtils.loadFixture("fixture/attachment_1.txt");
        Attachment att1 = new UnsavedFileAttachment(attachmentFile, "text/plain");
        Attachment att2 = new UnsavedFileAttachment(attachmentFile, "text/plain");

        List<Attachment> doc1Atts = new ArrayList<Attachment>();
        List<Attachment> doc2Atts = new ArrayList<Attachment>();
        doc1Atts.add(att1);
        doc2Atts.add(att2);

        BasicDocumentRevision newRevisionDoc1 = null;
        BasicDocumentRevision newRevisionDoc2 = null;

        try {
            newRevisionDoc1 = datastore.updateAttachments(doc1Rev1, doc1Atts);
            Assert.assertNotNull("Doc1 revision is null", newRevisionDoc1);
            List<? extends Attachment> attsForRev = datastore
                .attachmentsForRevision(newRevisionDoc1);
            Assert.assertEquals("Didn't get expected number of attachments", doc1Atts.size(),
                attsForRev.size());

            newRevisionDoc2 = datastore.updateAttachments(doc2Rev1, doc2Atts);
            Assert.assertNotNull("Doc2 revision is null", newRevisionDoc2);
            attsForRev = datastore.attachmentsForRevision(newRevisionDoc2);
            Assert.assertEquals("Didn't get expected number of attachments", doc2Atts.size(),
                attsForRev.size());

        } catch (ConflictException conflictException) {
            Assert.fail("Conflict Exception thrown "+conflictException);
        }
    }

}
