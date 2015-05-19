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
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLQueueCallable;
import com.cloudant.sync.util.Misc;
import com.cloudant.sync.util.TestUtils;

import static org.hamcrest.CoreMatchers.is;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by tomblench on 12/03/2014.
 */
public class AttachmentTest extends BasicDatastoreTestBase {

    @Test
    public void setAndGetAttachmentsTest() throws Exception {
        String attachmentName = "attachment_1.txt";
        MutableDocumentRevision rev_1Mut = new MutableDocumentRevision();
        rev_1Mut.body = bodyOne;
        BasicDocumentRevision rev_1 = datastore.createDocumentFromRevision(rev_1Mut);
        File f = TestUtils.loadFixture("fixture/"+attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "text/plain");
        BasicDocumentRevision newRevision = null;
        try {
            MutableDocumentRevision rev1_mut = rev_1.mutableCopy();
            rev1_mut.attachments.put(attachmentName, att);
            newRevision = datastore.updateDocumentFromRevision(rev1_mut);
        } catch (ConflictException ce){
            Assert.fail("ConflictException thrown: "+ce);
        }
        // get attachment...
        FileInputStream fis = null;
        try {
            byte[] expectedSha1 = Misc.getSha1((fis = new FileInputStream(f)));

            SavedAttachment savedAtt = (SavedAttachment) datastore.getAttachment(newRevision,
                    attachmentName);
            Assert.assertArrayEquals(expectedSha1, savedAtt.key);

            SavedAttachment savedAtt2 = (SavedAttachment) datastore.attachmentsForRevision
                    (newRevision).get(0);
            Assert.assertArrayEquals(expectedSha1, savedAtt2.key);
        } catch (FileNotFoundException fnfe) {
            Assert.fail("FileNotFoundException thrown " + fnfe);
        } finally {
            IOUtils.closeQuietly(fis);
        }
    }

    // check that the transaction gets rolled back if one file is dodgy
    @Test
    public void setBadAttachmentsTest() throws Exception {
        MutableDocumentRevision rev_1Mut = new MutableDocumentRevision();
        rev_1Mut.body = bodyOne;
        BasicDocumentRevision rev_1 = datastore.createDocumentFromRevision(rev_1Mut);
        Attachment att1 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/attachment_1.txt"), "text/plain");
        Attachment att2 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/nonexistentfile"), "text/plain");
        Attachment att3 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/attachment_2.txt"), "text/plain");
        BasicDocumentRevision newRevision = null;
        try {
            MutableDocumentRevision rev1_mut = rev_1.mutableCopy();
            rev1_mut.attachments.put(att1.name, att1);
            rev1_mut.attachments.put(att2.name, att2);
            rev1_mut.attachments.put(att3.name, att3);
            newRevision = datastore.updateDocumentFromRevision(rev1_mut);
            Assert.fail("FileNotFoundException not thrown");
        } catch (AttachmentException ae) {
            // now check that things got rolled back
            datastore.runOnDbQueue(new SQLQueueCallable<Object>() {
                @Override
                public Object call(SQLDatabase db) throws Exception {
                    Cursor c1 = db.rawQuery("select sequence from attachments;", null);
                    Assert.assertEquals("Attachments table not empty", c1.getCount(), 0);
                    Cursor c2 = db.rawQuery("select sequence from revs;", null);
                    Assert.assertEquals("Revs table not empty", c2.getCount(), 1);
                    return null;
                }
            }).get();

        }
    }

    // this test should throw a conflictexception when we try to add attachments to an old revision
    @Test(expected = DocumentException.class)
    public void setAttachmentsConflictTest() throws Exception {
        String attachmentName = "attachment_1.txt";
        MutableDocumentRevision rev_1Mut = new MutableDocumentRevision();
        rev_1Mut.body = bodyOne;
        BasicDocumentRevision rev_1 = datastore.createDocumentFromRevision(rev_1Mut);

        BasicDocumentRevision rev_2;
        try {
            MutableDocumentRevision rev_1_mut = rev_1.mutableCopy();
            rev_1_mut.body = bodyTwo;
            rev_2 = datastore.updateDocumentFromRevision(rev_1_mut);
        } catch (ConflictException ce) {
            Assert.fail("ConflictException thrown: "+ce);
        }
        File f = TestUtils.loadFixture("fixture/"+attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "text/plain");
        MutableDocumentRevision newRevision = rev_1.mutableCopy();
        newRevision.attachments.put(attachmentName, att);
        datastore.updateDocumentFromRevision(newRevision);
    }

    @Test
    public void createDeleteAttachmentsTest() throws Exception{

        MutableDocumentRevision rev_1Mut = new MutableDocumentRevision();
        rev_1Mut.body = bodyOne;
        BasicDocumentRevision rev_1 = datastore.createDocumentFromRevision(rev_1Mut);
        Attachment att1 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/attachment_1.txt"), "text/plain");
        Attachment att2 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/attachment_2.txt"), "text/plain");
        Attachment att3 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/bonsai-boston.jpg"), "image/jpeg");
        BasicDocumentRevision rev2 = null;

        MutableDocumentRevision rev_1_mut = rev_1.mutableCopy();
        rev_1_mut.attachments.put(att1.name, att1);
        rev_1_mut.attachments.put(att2.name, att2);
        rev_1_mut.attachments.put(att3.name, att3);
        rev2 = datastore.updateDocumentFromRevision(rev_1_mut);
        Assert.assertNotNull("Revision null", rev2);

        BasicDocumentRevision rev3 = null;

        MutableDocumentRevision rev2_mut = rev2.mutableCopy();
        rev2_mut.attachments.remove(att1.name);
        rev3 = datastore.updateDocumentFromRevision(rev2_mut);
        datastore.compact();
        Assert.assertNotNull("Revision null", rev3);

        // 1st shouldn't exist
        Attachment savedAtt1 = datastore.getAttachment(rev3, "attachment_1.txt");
        Assert.assertNull("Att1 not null", savedAtt1);

        // check we can read from the 2nd attachment, it wasn't deleted
        Attachment savedAtt2 = datastore.getAttachment(rev3, "attachment_2.txt");
        Assert.assertNotNull("Att2 null", savedAtt2);
        int i2 = savedAtt2.getInputStream().read();
        Assert.assertTrue("Can't read from Att2", i2 >= 0);

        // check we can read from the 3rd attachment, it wasn't deleted
        Attachment savedAtt3 = datastore.getAttachment(rev3, "bonsai-boston.jpg");
        Assert.assertNotNull("Att3 null", savedAtt3);
        int i3 = savedAtt3.getInputStream().read();
        Assert.assertTrue("Can't read from Att2", i3 >= 0);

        // now sneakily look for them on disk
        File attachments = new File(datastore.datastoreDir + "/extensions/com.cloudant.attachments");
        int count = attachments.listFiles().length;
        Assert.assertEquals("Did not find 1 file in blob store", 2, count);
    }


    @Test
    public void createDeleteAttachmentsFailTest() throws Exception {
        // check that an attachment 'going missing' from the blob store doesn't stop us deleting it
        // from the database
        String attachmentName = "attachment_1.txt";
        MutableDocumentRevision rev_1Mut = new MutableDocumentRevision();
        rev_1Mut.body = bodyOne;
        BasicDocumentRevision rev_1 = datastore.createDocumentFromRevision(rev_1Mut);
        File f = TestUtils.loadFixture("fixture/"+attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "text/plain");
        BasicDocumentRevision rev2 = null;
        MutableDocumentRevision rev_1_mut = rev_1.mutableCopy();
        rev_1_mut.attachments.put(att.name, att);
        rev2 = datastore.updateDocumentFromRevision(rev_1_mut);
        Assert.assertNotNull("Revision null", rev2);

        BasicDocumentRevision rev3 = null;
        // clear out the attachment directory
        File attachments = new File(datastore.datastoreDir + "/extensions/com.cloudant.attachments");
        for(File attFile : attachments.listFiles()) {
            attFile.delete();
        }
        MutableDocumentRevision rev2_mut = rev2.mutableCopy();
        rev2_mut.attachments.remove(attachmentName);
        rev3 = datastore.updateDocumentFromRevision(rev2_mut);
        Assert.assertNotNull("Revision null", rev3);

        // check that there are no attachments now associated with this doc
        Assert.assertTrue("Revision should have 0 attachments", datastore.attachmentsForRevision(rev3).isEmpty());
    }


    @Test
    public void attachmentsForRevisionTest() throws Exception {
        MutableDocumentRevision rev_1Mut = new MutableDocumentRevision();
        rev_1Mut.body = bodyOne;
        BasicDocumentRevision rev_1 = datastore.createDocumentFromRevision(rev_1Mut);

        Attachment att1 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/attachment_1.txt"), "text/plain");
        Attachment att2 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/attachment_2.txt"), "text/plain");
        BasicDocumentRevision newRevision = null;
        try {
            MutableDocumentRevision rev_1_mut = rev_1.mutableCopy();
            rev_1_mut.attachments.put(att1.name, att1);
            rev_1_mut.attachments.put(att2.name, att2);
            newRevision = datastore.updateDocumentFromRevision(rev_1_mut);
            List<? extends Attachment> attsForRev = datastore.attachmentsForRevision(newRevision);
            Assert.assertEquals("Didn't get expected number of attachments", 2, attsForRev.size());
        } catch (ConflictException ce){
            Assert.fail("ConflictException thrown: "+ce);
        }
    }

    @Test
    public void duplicateAttachmentTest() throws Exception {

        MutableDocumentRevision doc1Rev1Mut = new MutableDocumentRevision();
        doc1Rev1Mut.body = bodyOne;
        BasicDocumentRevision doc1Rev1 = datastore.createDocumentFromRevision(doc1Rev1Mut);
        MutableDocumentRevision doc2Rev1Mut = new MutableDocumentRevision();
        doc2Rev1Mut.body = bodyTwo;
        BasicDocumentRevision doc2Rev1 = datastore.createDocumentFromRevision(doc2Rev1Mut);

        File attachmentFile = TestUtils.loadFixture("fixture/attachment_1.txt");
        Attachment att1 = new UnsavedFileAttachment(attachmentFile, "text/plain");
        Attachment att2 = new UnsavedFileAttachment(attachmentFile, "text/plain");

        BasicDocumentRevision newRevisionDoc1 = null;
        BasicDocumentRevision newRevisionDoc2 = null;

        try {
            MutableDocumentRevision doc1Rev1_mut = doc1Rev1.mutableCopy();
            doc1Rev1_mut.attachments.put(att1.name, att1);
            newRevisionDoc1 = datastore.updateDocumentFromRevision(doc1Rev1_mut);
            Assert.assertNotNull("Doc1 revision is null", newRevisionDoc1);
            List<? extends Attachment> attsForRev = datastore
                .attachmentsForRevision(newRevisionDoc1);
            Assert.assertEquals("Didn't get expected number of attachments", 1,
                attsForRev.size());

            MutableDocumentRevision doc2Rev1_mut = doc2Rev1.mutableCopy();
            doc2Rev1_mut.attachments.put(att2.name, att2);
            newRevisionDoc2 = datastore.updateDocumentFromRevision(doc2Rev1_mut);
            Assert.assertNotNull("Doc2 revision is null", newRevisionDoc2);
            attsForRev = datastore.attachmentsForRevision(newRevisionDoc2);
            Assert.assertEquals("Didn't get expected number of attachments", 1,
                attsForRev.size());

        } catch (ConflictException conflictException) {
            Assert.fail("Conflict Exception thrown "+conflictException);
        }
    }

}
