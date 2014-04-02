package com.cloudant.sync.replication;

import com.cloudant.mazha.Response;
import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.util.TestUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;

/**
 * Created by tomblench on 26/03/2014.
 */
public class AttachmentsPullTest extends ReplicationTestBase {

    String id;
    String rev;

    String attachmentName = "att1";
    String attachmentData = "this is an attachment";
    String attachmentData2 = "this is a different attachment";

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void pullRevisionsWithAttachments() {
        createRevisionAndAttachment();
        try {
            pull();
        } catch (Exception e) {
            Assert.fail("Pull error "+e);
        }
        DocumentRevision docRev = datastore.getDocument(id, rev);
        Attachment a = datastore.getAttachment(docRev, attachmentName);
        Assert.assertNotNull("Attachment is null", a);
        Assert.assertEquals(attachmentName, a.name);
        Assert.assertTrue("Streams not equal", TestUtils.streamsEqual(new ByteArrayInputStream(attachmentData.getBytes()), a.getInputStream()));

        // update revision and attachment on remote (same id) - this tests the other code path of updating the sequence on the rev
        updateRevisionAndAttachment();
        try {
            pull();
        } catch (Exception e) {
            Assert.fail("Pull error "+e);
        }
        DocumentRevision docRev2 = datastore.getDocument(id, rev);
        Attachment a2 = datastore.getAttachment(docRev2, attachmentName);
        Assert.assertNotNull("Attachment is null", a2);
        Assert.assertEquals(attachmentName, a2.name);
        Assert.assertTrue("Streams not equal", TestUtils.streamsEqual(new ByteArrayInputStream(attachmentData2.getBytes()), a2.getInputStream()));
    }

    private void createRevisionAndAttachment() {
        Bar bar = new Bar();
        bar.setName("Tom");
        bar.setAge(34);

        Response res = remoteDb.create(bar);
        bar = remoteDb.get(Bar.class, res.getId());

        id = res.getId();
        rev = res.getRev();
        remoteDb.getCouchClient().putAttachmentStream(id, rev, attachmentName, attachmentData);

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
        remoteDb.getCouchClient().putAttachmentStream(id, rev, attachmentName, attachmentData2);

        // putting attachment will have updated the rev
        bar = remoteDb.get(Bar.class, res.getId());
        rev = bar.getRevision();
    }

    private void pull() throws Exception {
        TestStrategyListener listener = new TestStrategyListener();
        BasicPullStrategy pull = new BasicPullStrategy(this.createPullReplication());
        pull.getEventBus().register(listener);

        Thread t = new Thread(pull);
        t.start();
        t.join();
        Assert.assertTrue(listener.finishCalled);
        Assert.assertFalse(listener.errorCalled);
    }



}
