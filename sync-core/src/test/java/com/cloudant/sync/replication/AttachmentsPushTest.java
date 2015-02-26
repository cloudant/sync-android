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
import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.ConflictException;
import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.DocumentBody;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentRevisionBuilder;
import com.cloudant.sync.datastore.MutableDocumentRevision;
import com.cloudant.sync.datastore.UnsavedFileAttachment;
import com.cloudant.sync.util.TestUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;

/**
 * Created by tomblench on 26/03/2014.
 */

@Category(RequireRunningCouchDB.class)
@RunWith(Parameterized.class)
public class AttachmentsPushTest extends ReplicationTestBase {

    String id1;
    String id2;
    String id3;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {PushAttachmentsInline.False}, {PushAttachmentsInline.Small}, {PushAttachmentsInline.True}
        });
    }

    @Parameterized.Parameter
    public PushAttachmentsInline pushAttachmentsInline;

    /**
     * After all documents are created:
     *
     * Doc 1: 1 -> 2 -> 3
     * Doc 2: 1 -> 2
     * Doc 3: 1 -> 2 -> 3
     */
    private void populateSomeDataInLocalDatastore() throws Exception {

        id1 = createDocInDatastore("Doc 1");
        Assert.assertNotNull(id1);
        updateDocInDatastore(id1, "Doc 1");
        updateDocInDatastore(id1, "Doc 1");

        id2 = createDocInDatastore("Doc 2");
        Assert.assertNotNull(id2);
        updateDocInDatastore(id2, "Doc 2");

        id3 = createDocInDatastore("Doc 3");
        Assert.assertNotNull(id3);
        updateDocInDatastore(id3, "Doc 3");
        updateDocInDatastore(id3, "Doc 3");
    }

    public String createDocInDatastore(String d) throws Exception {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        Map<String, String> m = new HashMap<String, String>();
        m.put("data", d);
        rev.body = DocumentBodyFactory.create(m);
        return datastore.createDocumentFromRevision(rev).getId();
    }


    public String updateDocInDatastore(String id, String data) throws Exception {
        MutableDocumentRevision rev = datastore.getDocument(id).mutableCopy();
        Map<String, String> m = new HashMap<String, String>();
        m.put("data", data);
        rev.body = DocumentBodyFactory.create(m);
        return datastore.updateDocumentFromRevision(rev).getId();
    }

    @Test
    public void pushAttachmentsTest() throws Exception {
        Assume.assumeFalse(IGNORE_MULTIPART_ATTACHMENTS);
        // simple 1-rev attachment
        String attachmentName = "attachment_1.txt";
        populateSomeDataInLocalDatastore();
        File f = TestUtils.loadFixture("fixture/"+attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "text/plain");
        BasicDocumentRevision oldRevision = datastore.getDocument(id1);
        BasicDocumentRevision newRevision = null;
        // set attachment
        MutableDocumentRevision oldRevision_mut = oldRevision.mutableCopy();
        oldRevision_mut.attachments.put(attachmentName, att);
        newRevision = datastore.updateDocumentFromRevision(oldRevision_mut);


        // push replication
        push();

        // check it's in the DB
        InputStream is1 = this.couchClient.getAttachmentStreamUncompressed(id1, attachmentName);
        InputStream is2 = new FileInputStream(f);
        Assert.assertTrue("Attachment not the same", TestUtils.streamsEqual(is1, is2));
    }

    @Test
    public void pushBigAttachmentsTest() throws Exception {
        Assume.assumeFalse(IGNORE_MULTIPART_ATTACHMENTS);
        // simple 1-rev attachment
        String attachmentName = "bonsai-boston.jpg";
        populateSomeDataInLocalDatastore();
        File f = TestUtils.loadFixture("fixture/"+ attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "image/jpeg");
        BasicDocumentRevision oldRevision = datastore.getDocument(id1);
        BasicDocumentRevision newRevision = null;
        // set attachment
        MutableDocumentRevision oldRevision_mut = oldRevision.mutableCopy();
        oldRevision_mut.attachments.put(attachmentName, att);
        newRevision = datastore.updateDocumentFromRevision(oldRevision_mut);


        // push replication
        push();

        // check it's in the DB
        InputStream is1 = this.couchClient.getAttachmentStreamUncompressed(id1, attachmentName);
        InputStream is2 = new FileInputStream(f);
        Assert.assertTrue("Attachment not the same", TestUtils.streamsEqual(is1, is2));
    }

    @Test
    public void pushAttachmentsTest2() throws Exception {
        Assume.assumeFalse(IGNORE_MULTIPART_ATTACHMENTS);
        // more complex test with attachments changing between revisions
        String attachmentName1 = "attachment_1.txt";
        String attachmentName2 = "attachment_2.txt";
        populateSomeDataInLocalDatastore();
        File f1 = TestUtils.loadFixture("fixture/"+ attachmentName1);
        File f2 = TestUtils.loadFixture("fixture/"+ attachmentName2);
        Attachment att1 = new UnsavedFileAttachment(f1, "text/plain");
        Attachment att2 = new UnsavedFileAttachment(f2, "text/plain");
        BasicDocumentRevision rev1 = datastore.getDocument(id1);
        BasicDocumentRevision rev2 = null;
        // set attachment
        MutableDocumentRevision rev1_mut = rev1.mutableCopy();
        rev1_mut.attachments.put(attachmentName1, att1);
        rev2 = datastore.updateDocumentFromRevision(rev1_mut);

        // push replication - att1 should be uploaded
        push();

        MutableDocumentRevision rev2_mut = rev2.mutableCopy();
        BasicDocumentRevision rev3 = datastore.updateDocumentFromRevision(rev2_mut);

        // push replication - no atts should be uploaded
        push();

        BasicDocumentRevision rev4 = null;
        // set attachment
        MutableDocumentRevision rev3_mut = rev3.mutableCopy();
        rev3_mut.attachments.put(attachmentName2, att2);
        rev4 = datastore.updateDocumentFromRevision(rev3_mut);

        // push replication - att2 should be uploaded
        push();

        InputStream isOriginal1;
        InputStream isOriginal2;

        InputStream isRev2 = this.couchClient.getAttachmentStreamUncompressed(id1, rev2.getRevision(), attachmentName1);
        InputStream isRev3 = this.couchClient.getAttachmentStreamUncompressed(id1, rev3.getRevision(), attachmentName1);
        InputStream isRev4Att1 = this.couchClient.getAttachmentStreamUncompressed(id1, rev4.getRevision(), attachmentName1);
        InputStream isRev4Att2 = this.couchClient.getAttachmentStreamUncompressed(id1, rev4.getRevision(), attachmentName2);

        isOriginal1 = new FileInputStream(f1);
        Assert.assertTrue("Attachment not the same", TestUtils.streamsEqual(isRev2, isOriginal1));

        isOriginal1 = new FileInputStream(f1);
        Assert.assertTrue("Attachment not the same", TestUtils.streamsEqual(isRev3, isOriginal1));

        isOriginal1 = new FileInputStream(f1);
        isOriginal2 = new FileInputStream(f2);
        Assert.assertTrue("Attachment not the same", TestUtils.streamsEqual(isRev4Att1, isOriginal1));
        Assert.assertTrue("Attachment not the same", TestUtils.streamsEqual(isRev4Att2, isOriginal2));

    }

    private void push() throws Exception {
        TestStrategyListener listener = new TestStrategyListener();
        BasicPushStrategy push = new BasicPushStrategy(this.createPushReplication(),
                new PushConfiguration(PushConfiguration.DEFAULT_CHANGES_LIMIT_PER_BATCH,
                        PushConfiguration.DEFAULT_MAX_BATCH_COUNTER_PER_RUN,
                        PushConfiguration.DEFAULT_BULK_INSERT_SIZE,
                        pushAttachmentsInline));
        push.eventBus.register(listener);

        Thread t = new Thread(push);
        t.start();
        t.join();
        Assert.assertTrue(listener.finishCalled);
        Assert.assertFalse(listener.errorCalled);
    }

}
