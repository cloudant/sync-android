package com.cloudant.sync.datastore;

import junit.framework.Assert;

import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by tomblench on 27/02/2014.
 */
public class MultipartAttachmentReaderTests {

    // test multipart with 1000 attachments at various chunk sizes
    @Test
    public void TestReader1000Attachments() throws FileNotFoundException, IOException {

        int[] sizes = {1,4,5,7,12,127,1270};

        for (int i=0;i<sizes.length;i++) {
            MultipartAttachmentReader mpr = new MultipartAttachmentReader("--nftjykeoeiyhopgldlwuzaimzahcdvlh".getBytes(), "/tmp");
            File f = new File("fixture/multipart_1000_atts.mime");
            FileInputStream fis = new FileInputStream(f);
            int bufSize = sizes[i];
            byte[] buf = new byte[bufSize];
            int nRead = 0;
            while ((nRead = fis.read(buf)) > 0) {
                mpr.write(buf, 0, nRead);
            }
            Assert.assertEquals(1002, mpr.getBoundaryCount());
            Assert.assertEquals(1002, mpr.sections.size());
            Assert.assertEquals(mpr.getActualAttachmentCount(), mpr.getSignalledAttachmentCount());
            System.out.println(mpr.sections);
        }
    }

    // test with a real response from couch including a gzipped part
    @Test
    public void TestReaderAttachmentsFromCouch() throws FileNotFoundException, IOException {

        int bufSize = 1024;

        MultipartAttachmentReader mpr = new MultipartAttachmentReader("--eb2a76a538bace45331bda0f2bb92a18".getBytes(), "/tmp");
        File f = new File("fixture/multipart_from_couch_with_gzip.mime");

        FileInputStream fis = new FileInputStream(f);
        byte[] buf = new byte[bufSize];
        int nRead = 0;
        while ((nRead = fis.read(buf)) > 0) {
            mpr.write(buf, 0, nRead);
        }
        for (MultipartAttachmentReader.Section s : mpr.sections) {
            Assert.assertNull(s.error);
        }
        System.out.println(mpr.sections);
    }


}
