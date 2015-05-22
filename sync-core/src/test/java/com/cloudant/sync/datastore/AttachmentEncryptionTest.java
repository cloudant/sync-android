package com.cloudant.sync.datastore;

import com.cloudant.sync.datastore.encryption.SimpleKeyProvider;
import com.cloudant.sync.sqlite.android.encryption.EncryptedUnsavedFileAttachment;
import com.cloudant.sync.util.Misc;
import com.cloudant.sync.util.TestUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 */
public class AttachmentEncryptionTest extends BasicDatastoreTestBase {

    @Test
    public void setAndGetAttachmentsTest() throws Exception {
        byte[] password = new byte[]{'t','e','s','t','K','e','y'};
        SimpleKeyProvider keyProvider = new SimpleKeyProvider(password);

        String attachmentName = "attachment_1.txt";
        MutableDocumentRevision rev_1Mut = new MutableDocumentRevision();
        rev_1Mut.body = bodyOne;
        BasicDocumentRevision rev_1 = datastore.createDocumentFromRevision(rev_1Mut);
        File f = TestUtils.loadFixture("fixture/" + attachmentName);
        //Attachment att = new UnsavedFileAttachment(f, "text/plain");
        Attachment att = new EncryptedUnsavedFileAttachment(f, "text/plain", keyProvider);
        BasicDocumentRevision newRevision = null;
        try {
            MutableDocumentRevision rev1_mut = rev_1.mutableCopy();
            rev1_mut.attachments.put(attachmentName, att);
            newRevision = datastore.updateDocumentFromRevision(rev1_mut);
        } catch (ConflictException ce) {
            Assert.fail("ConflictException thrown: " + ce);
        }
        // get attachment...
        try {
            byte[] expectedSha1 = Misc.getSha1(new FileInputStream(f));
            //EncryptedSavedAttachment encryptAtt = new EncryptedSavedAttachment();
            SavedAttachment savedAtt = (SavedAttachment) datastore.getAttachment(newRevision, attachmentName);
            Assert.assertArrayEquals(expectedSha1, savedAtt.key);

            SavedAttachment savedAtt2 = (SavedAttachment) datastore.attachmentsForRevision(newRevision).get(0);
            Assert.assertArrayEquals(expectedSha1, savedAtt2.key);
        } catch (FileNotFoundException fnfe) {
            Assert.fail("FileNotFoundException thrown "+fnfe);
        }
    }
}
