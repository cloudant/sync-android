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

package com.cloudant.sync.replication;

import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.mazha.Response;
import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.util.TestUtils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * Created by tomblench on 26/03/2014.
 */

@Category(RequireRunningCouchDB.class)
@RunWith(Parameterized.class)
public class AttachmentsPullTest extends ReplicationTestBase {

    String id;
    String rev;

    String attachmentName = "att1";
    String attachmentData = "this is an attachment";
    String attachmentData2 = "this is a different attachment";

    String bigAttachmentName = "bonsai-boston.jpg";
    String bigTextAttachmentName = "lorem_long.txt";

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {false}, {true}
        });
    }

    @Parameterized.Parameter
    public boolean pullAttachmentsInline;

    @Test
    public void pullRevisionsWithAttachments() throws Exception{
        createRevisionAndAttachment();
        try {
            pull();
        } catch (Exception e) {
            Assert.fail("Pull error " + e);
        }
        BasicDocumentRevision docRev = datastore.getDocument(id, rev);
        Attachment a = datastore.getAttachment(docRev, attachmentName);
        Assert.assertNotNull("Attachment is null", a);
        Assert.assertEquals(attachmentName, a.name);
        try {
            Assert.assertTrue("Streams not equal", TestUtils.streamsEqual(new ByteArrayInputStream(attachmentData.getBytes()), a.getInputStream()));
        } catch (IOException ioe) {
            Assert.fail("Exception thrown " + ioe);
        }
        // update revision and attachment on remote (same id) - this tests the other code path of updating the sequence on the rev
        updateRevisionAndAttachment();
        try {
            pull();
        } catch (Exception e) {
            Assert.fail("Pull error " + e);
        }
        BasicDocumentRevision docRev2 = datastore.getDocument(id, rev);
        Attachment a2 = datastore.getAttachment(docRev2, attachmentName);
        Assert.assertNotNull("Attachment is null", a2);
        Assert.assertEquals(attachmentName, a2.name);
        try {
            Assert.assertTrue("Streams not equal", TestUtils.streamsEqual(new ByteArrayInputStream(attachmentData2.getBytes()), a2.getInputStream()));
        } catch (IOException ioe) {
            Assert.fail("Exception thrown " + ioe);
        }
    }

    @Test
    public void pullRevisionsWithBigAttachments() throws Exception {
        try {
            createRevisionAndBigAttachment();
            pull();
        } catch (Exception e) {
            Assert.fail("Create/pull error " + e);
        }
        BasicDocumentRevision docRev = datastore.getDocument(id, rev);
        Attachment a = datastore.getAttachment(docRev, bigAttachmentName);
        Assert.assertNotNull("Attachment is null", a);
        Assert.assertEquals(bigAttachmentName, a.name);
        try {
            Assert.assertTrue("Streams not equal", TestUtils.streamsEqual(new FileInputStream(TestUtils.loadFixture("fixture/" + bigAttachmentName)), a.getInputStream()));
        } catch (IOException ioe) {
            Assert.fail("Exception thrown " + ioe);
        }
    }

    @Test
    public void pullRevisionsWithBigTextAttachments() throws Exception {
        try {
            createRevisionAndBigTextAttachment();
            pull();
        } catch (Exception e) {
            Assert.fail("Create/pull error " + e);
        }
        BasicDocumentRevision docRev = datastore.getDocument(id, rev);
        Attachment a = datastore.getAttachment(docRev, bigTextAttachmentName);
        Assert.assertNotNull("Attachment is null", a);
        Assert.assertEquals(bigTextAttachmentName, a.name);

        // check that the attachment is correctly saved to the blob store:
        // get the 1 and only file at the known location and look at the first 2 bytes
        // if it was saved uncompressed (pulled inline) will be first 2 bytes of text
        // if it was saved compressed (pulled separately) will be magic bytes of gzip file
        File attachments = new File(this.datastoreManagerPath + "/AttachmentsPullTest/extensions/com.cloudant.attachments");
        int count = attachments.listFiles().length;
        Assert.assertEquals("Did not find 1 file in blob store", 1, count);
        File attFile = attachments.listFiles()[0];
        FileInputStream fis = new FileInputStream(attFile);
        byte[] magic = new byte[2];
        int read = fis.read(magic);
        Assert.assertEquals(2, read);
        if (pullAttachmentsInline) {
            // ascii Lo
            Assert.assertEquals('L', magic[0]);
            Assert.assertEquals('o', magic[1]);
            Assert.assertEquals(a.encoding, Attachment.Encoding.Plain);
        } else {
            // 1f 8b
            Assert.assertEquals(31, magic[0]);
            Assert.assertEquals(-117, magic[1]);
            Assert.assertEquals(a.encoding, Attachment.Encoding.Gzip);
        }
        fis.close();
        try {
            Assert.assertTrue("Streams not equal", TestUtils.streamsEqual(new FileInputStream(TestUtils.loadFixture("fixture/"+ bigTextAttachmentName)), a.getInputStream()));
        } catch (IOException ioe) {
            Assert.fail("Exception thrown " + ioe);
        }
    }

    @Test
    public void dontPullAttachmentNoLongerExists() {
        try {
            // create a rev with an attachment, then update it without attachment
            // ensure updated version no longer has attachment associated with it locally
            createRevisionAndBigTextAttachment();
            pull();
            BasicDocumentRevision docRev1 = datastore.getDocument(id, rev);
            Attachment a1 = datastore.getAttachment(docRev1, bigTextAttachmentName);
            updateRevision();
            pull();
            BasicDocumentRevision docRev2 = datastore.getDocument(id, rev);
            Attachment a2 = datastore.getAttachment(docRev2, bigTextAttachmentName);
            Assert.assertNull(a2);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Create/pull error " + e);
        }
    }

    @Test
    public void dontPullAttachmentAlreadyPulled() {
        try {
            // create a rev with an attachment, then update it keeping attachment
            // TODO we need to somehow check the attachment wasn't re-downloaded
            createRevisionAndBigTextAttachment();
            pull();
            BasicDocumentRevision docRev1 = datastore.getDocument(id, rev);
            Attachment a1 = datastore.getAttachment(docRev1, bigTextAttachmentName);
            updateRevisionAndKeepAttachment();
            updateRevisionAndKeepAttachment();
            pull();
            BasicDocumentRevision docRev2 = datastore.getDocument(id, rev);
            Attachment a2 = datastore.getAttachment(docRev2, bigTextAttachmentName);
            Assert.assertNotNull(a2);

        } catch (Exception e) {
            Assert.fail("Create/pull error " + e);
        }
    }



    private void updateRevision() {
        BarWithAttachments bar = remoteDb.get(BarWithAttachments.class, id);

        bar.setName("Tom");
        bar.setAge(35);
        // clear out the attachment
        bar._attachments = null;

        Response res = remoteDb.update(id, bar);
        bar = remoteDb.get(BarWithAttachments.class, res.getId());

        rev = res.getRev();
    }

    private void updateRevisionAndKeepAttachment() {
        BarWithAttachments bar = remoteDb.get(BarWithAttachments.class, id);

        bar.setName("Tom");
        bar.setAge(35);

        Response res = remoteDb.update(id, bar);
        bar = remoteDb.get(BarWithAttachments.class, res.getId());

        rev = res.getRev();
    }

    private void createRevisionAndAttachment() {
        BarWithAttachments bar = new BarWithAttachments();
        bar.setName("Tom");
        bar.setAge(34);

        Response res = remoteDb.create(bar);
        bar = remoteDb.get(BarWithAttachments.class, res.getId());

        id = res.getId();
        rev = res.getRev();
        remoteDb.getCouchClient().putAttachmentStream(id, rev, attachmentName, "text/plain", attachmentData.getBytes());

        // putting attachment will have updated the rev
        bar = remoteDb.get(BarWithAttachments.class, res.getId());
        rev = bar.getRevision();
    }

    private void createRevisionAndBigAttachment() throws IOException {
        Bar bar = new Bar();
        bar.setName("Tom");
        bar.setAge(34);

        Response res = remoteDb.create(bar);
        bar = remoteDb.get(Bar.class, res.getId());

        File f = TestUtils.loadFixture("fixture/"+bigAttachmentName);
        FileInputStream fis = new FileInputStream(f);
        byte[] data = new byte[(int)f.length()];
        fis.read(data);

        id = res.getId();
        rev = res.getRev();
        remoteDb.getCouchClient().putAttachmentStream(id, rev, bigAttachmentName, "image/jpeg", data);

        // putting attachment will have updated the rev
        bar = remoteDb.get(Bar.class, res.getId());
        rev = bar.getRevision();
    }

    private void createRevisionAndBigTextAttachment() throws IOException {
        Bar bar = new Bar();
        bar.setName("Tom");
        bar.setAge(34);

        Response res = remoteDb.create(bar);
        bar = remoteDb.get(Bar.class, res.getId());

        File f = TestUtils.loadFixture("fixture/"+bigTextAttachmentName);
        FileInputStream fis = new FileInputStream(f);
        byte[] data = new byte[(int)f.length()];
        fis.read(data);

        id = res.getId();
        rev = res.getRev();
        remoteDb.getCouchClient().putAttachmentStream(id, rev, bigTextAttachmentName, "text/plain", data);

        // putting attachment will have updated the rev
        bar = remoteDb.get(Bar.class, res.getId());
        rev = bar.getRevision();
    }


    private void updateRevisionAndAttachment() {
        Bar bar = new Bar();
        bar.setName("Dick");
        bar.setAge(33);
        bar.setRevision(rev);

        Response res = remoteDb.update(id, bar);
        rev = res.getRev();
        remoteDb.getCouchClient().putAttachmentStream(id, rev, attachmentName, "text/plain", attachmentData2.getBytes());

        // putting attachment will have updated the rev
        bar = remoteDb.get(Bar.class, res.getId());
        rev = bar.getRevision();
    }

    // override so we can have custom value for pullAttachmentsInline
    @Override
    protected PullResult pull() throws Exception {
        TestStrategyListener listener = new TestStrategyListener();
        BasicReplicator pull = (BasicReplicator)getPullBuilder().pullAttachmentsInline(pullAttachmentsInline).build();
        pull.strategy.getEventBus().register(listener);
        pull.strategy.run();
        Assert.assertTrue(listener.finishCalled);
        Assert.assertFalse(listener.errorCalled);
        return new PullResult((BasicPullStrategy)pull.strategy, listener);
    }

}
