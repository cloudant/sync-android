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
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.datastore.UnsavedFileAttachment;
import com.cloudant.sync.util.TestUtils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
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
        DocumentRevision rev = new DocumentRevision();
        Map<String, String> m = new HashMap<String, String>();
        m.put("data", d);
        rev.setBody(DocumentBodyFactory.create(m));
        return datastore.createDocumentFromRevision(rev).getId();
    }


    public String updateDocInDatastore(String id, String data) throws Exception {
        DocumentRevision rev = datastore.getDocument(id);
        Map<String, String> m = new HashMap<String, String>();
        m.put("data", data);
        rev.setBody(DocumentBodyFactory.create(m));
        return datastore.updateDocumentFromRevision(rev).getId();
    }

    @Test
    public void pushAttachmentsTest() throws Exception {
        // simple 1-rev attachment
        String attachmentName = "attachment_1.txt";
        populateSomeDataInLocalDatastore();
        File f = TestUtils.loadFixture("fixture/"+attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "text/plain");
        DocumentRevision oldRevision = datastore.getDocument(id1);
        DocumentRevision newRevision = null;
        // set attachment
        DocumentRevision oldRevision_mut = oldRevision;
        oldRevision_mut.getAttachments().put(attachmentName, att);
        newRevision = datastore.updateDocumentFromRevision(oldRevision_mut);


        // push replication
        push();

        // check it's in the DB
        InputStream is1 = this.couchClient.getAttachmentStream(id1, newRevision.getRevision(), attachmentName, false);
        InputStream is2 = new FileInputStream(f);
        Assert.assertTrue("Attachment not the same", TestUtils.streamsEqual(is1, is2));
    }

    @Test
    public void pushBigAttachmentsTest() throws Exception {
        // simple 1-rev attachment
        String attachmentName = "bonsai-boston.jpg";
        populateSomeDataInLocalDatastore();
        File f = TestUtils.loadFixture("fixture/"+ attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "image/jpeg");
        DocumentRevision oldRevision = datastore.getDocument(id1);
        DocumentRevision newRevision = null;
        // set attachment
        DocumentRevision oldRevision_mut = oldRevision;
        oldRevision_mut.getAttachments().put(attachmentName, att);
        newRevision = datastore.updateDocumentFromRevision(oldRevision_mut);


        // push replication
        push();

        // check it's in the DB
        InputStream is1 = this.couchClient.getAttachmentStream(id1, newRevision.getRevision(), attachmentName, false);
        InputStream is2 = new FileInputStream(f);
        Assert.assertTrue("Attachment not the same", TestUtils.streamsEqual(is1, is2));
    }

    @Test
    public void pushAttachmentsTest2() throws Exception {
        // more complex test with attachments changing between revisions
        String attachmentName1 = "attachment_1.txt";
        String attachmentName2 = "attachment_2.txt";
        populateSomeDataInLocalDatastore();
        File f1 = TestUtils.loadFixture("fixture/"+ attachmentName1);
        File f2 = TestUtils.loadFixture("fixture/"+ attachmentName2);
        Attachment att1 = new UnsavedFileAttachment(f1, "text/plain");
        Attachment att2 = new UnsavedFileAttachment(f2, "text/plain");
        DocumentRevision rev1 = datastore.getDocument(id1);
        DocumentRevision rev2 = null;
        // set attachment
        DocumentRevision rev1_mut = rev1;
        rev1_mut.getAttachments().put(attachmentName1, att1);
        rev2 = datastore.updateDocumentFromRevision(rev1_mut);

        // push replication - att1 should be uploaded
        push();

        DocumentRevision rev2_mut = rev2;
        DocumentRevision rev3 = datastore.updateDocumentFromRevision(rev2_mut);

        // push replication - no atts should be uploaded
        push();

        DocumentRevision rev4 = null;
        // set attachment
        DocumentRevision rev3_mut = rev3;
        rev3_mut.getAttachments().put(attachmentName2, att2);
        rev4 = datastore.updateDocumentFromRevision(rev3_mut);

        // push replication - att2 should be uploaded
        push();

        InputStream isOriginal1;
        InputStream isOriginal2;

        InputStream isRev2 = this.couchClient.getAttachmentStream(id1, rev2.getRevision(), attachmentName1, false);
        InputStream isRev3 = this.couchClient.getAttachmentStream(id1, rev3.getRevision(), attachmentName1, false);
        InputStream isRev4Att1 = this.couchClient.getAttachmentStream(id1, rev4.getRevision(), attachmentName1, false);
        InputStream isRev4Att2 = this.couchClient.getAttachmentStream(id1, rev4.getRevision(), attachmentName2, false);

        isOriginal1 = new FileInputStream(f1);
        Assert.assertTrue("Attachment not the same", TestUtils.streamsEqual(isRev2, isOriginal1));

        isOriginal1 = new FileInputStream(f1);
        Assert.assertTrue("Attachment not the same", TestUtils.streamsEqual(isRev3, isOriginal1));

        isOriginal1 = new FileInputStream(f1);
        isOriginal2 = new FileInputStream(f2);
        Assert.assertTrue("Attachment not the same", TestUtils.streamsEqual(isRev4Att1, isOriginal1));
        Assert.assertTrue("Attachment not the same", TestUtils.streamsEqual(isRev4Att2, isOriginal2));

    }

    // regression test for FB 46326 - see
    // https://groups.google.com/forum/#!topic/cloudant-sync/xAgWtuSsrk8 for details
    @Test
    public void pushAttachmentsStubsCorrectlySent() throws Exception {
        // more complex test with attachments changing between revisions
        String attachmentName1 = "attachment_1.txt";
        String attachmentName2 = "attachment_2.txt";
        populateSomeDataInLocalDatastore();
        File f1 = TestUtils.loadFixture("fixture/"+ attachmentName1);
        File f2 = TestUtils.loadFixture("fixture/"+ attachmentName2);
        Attachment att1 = new UnsavedFileAttachment(f1, "text/plain");
        Attachment att2 = new UnsavedFileAttachment(f2, "text/plain");
        DocumentRevision rev1 = datastore.getDocument(id1);
        DocumentRevision rev2 = null;
        // set attachment
        DocumentRevision rev1_mut = rev1;
        rev1_mut.getAttachments().put(attachmentName1, att1);
        rev2 = datastore.updateDocumentFromRevision(rev1_mut);

        DocumentRevision rev2_mut = rev2;
        DocumentRevision rev3 = datastore.updateDocumentFromRevision(rev2_mut);

        DocumentRevision rev4 = null;
        // set attachment
        DocumentRevision rev3_mut = rev3;
        rev3_mut.getAttachments().put(attachmentName2, att2);
        rev4 = datastore.updateDocumentFromRevision(rev3_mut);

        // push replication - att1 & att2 should be uploaded
        push();

        InputStream isOriginal1;
        InputStream isOriginal2;

        InputStream isRev4Att1 = this.couchClient.getAttachmentStream(id1, rev4.getRevision(), attachmentName1, false);
        InputStream isRev4Att2 = this.couchClient.getAttachmentStream(id1, rev4.getRevision(), attachmentName2, false);

        isOriginal1 = new FileInputStream(f1);
        isOriginal2 = new FileInputStream(f2);
        Assert.assertTrue("Attachment not the same", TestUtils.streamsEqual(isRev4Att1, isOriginal1));
        Assert.assertTrue("Attachment not the same", TestUtils.streamsEqual(isRev4Att2, isOriginal2));

    }

}
