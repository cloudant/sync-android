package com.cloudant.sync.datastore;

import com.cloudant.sync.util.Misc;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
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
            newRevision = datastore.setAttachments(rev_1, atts);
        } catch (ConflictException ce){
            Assert.fail("ConflictException thrown: "+ce);
        } catch (IOException ioe) {
            Assert.fail("IOException thrown: "+ioe);
        }
        // get attachment...
        byte[] expectedSha1 = Misc.getSha1(f);

        SavedAttachment savedAtt = (SavedAttachment)datastore.getAttachment(newRevision, attachmentName);
        Assert.assertArrayEquals(expectedSha1, savedAtt.key);

        SavedAttachment savedAtt2 = (SavedAttachment)datastore.getAttachments(newRevision).get(0);
        Assert.assertArrayEquals(expectedSha1, savedAtt2.key);


    }



}
