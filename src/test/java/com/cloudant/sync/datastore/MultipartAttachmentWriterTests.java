package com.cloudant.sync.datastore;

import com.cloudant.sync.util.TestUtils;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

/**
 * Created by tomblench on 24/02/2014.
 */

@RunWith(Parameterized.class)
public class MultipartAttachmentWriterTests {

    String datastore_manager_dir;
    DatastoreManager datastoreManager;
    Datastore datastore = null;

    byte[] jsonData = null;
    DocumentBody bodyOne = null;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {3}, {99}, {1024}
        });
    }

    @Parameterized.Parameter
    public int chunkSize;

    @Before
    public void setUp() throws Exception {
        datastore_manager_dir = TestUtils.createTempTestingDir(this.getClass().getName());
        datastoreManager = new DatastoreManager(this.datastore_manager_dir);
        datastore = (this.datastoreManager.openDatastore(getClass().getSimpleName()));
        jsonData = "{\"body\":\"This is a body.\"}".getBytes();
        bodyOne = BasicDocumentBody.bodyWith(jsonData);
    }

    @After
    public void tearDown() throws Exception {
        TestUtils.deleteTempTestingDir(datastore_manager_dir);
    }

    @Test
    public void Add1000TextAttachmentsTest() {
        try {
            DocumentRevision doc = datastore.createDocument(bodyOne);
            ArrayList<Attachment> attachments = new ArrayList<Attachment>();

            MultipartAttachmentWriter mpw = new MultipartAttachmentWriter();
            mpw.setBody(doc);

            for (int i=0; i<1000; i++) {
                String name = "attachment" + UUID.randomUUID();
                StringBuilder s = new StringBuilder();
                s.append("this is some data for ");
                s.append(name);
                for (int c=0;c<Math.random()*100;c++) {
                    s.append("+");
                }
                byte[] bytes = (s.toString()).getBytes();
                Attachment att0 = new UnsavedStreamAttachment(new ByteArrayInputStream(bytes), name, "text/plain");
                mpw.addAttachment(att0);
            }
            mpw.close();

            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            int amountRead = 0;
            int totalRead = 0;

            do {
                byte buf[] = new byte[chunkSize];
                amountRead = mpw.read(buf);
                if (amountRead > 0) {
                    totalRead += amountRead;
                    System.out.print(new String(buf, 0, amountRead));
                }
            } while(amountRead > 0);

            Assert.assertEquals(totalRead, mpw.getContentLength());

        } catch (Exception e) {
            System.out.println("aarg "+e);
        }
    }


    @Test
    public void AddImageAttachmentTest() {
        try {
            DocumentRevision doc = datastore.createDocument(bodyOne);
            ArrayList<Attachment> attachments = new ArrayList<Attachment>();

            MultipartAttachmentWriter mpw = new MultipartAttachmentWriter();
            mpw.setBody(doc);

            Attachment att0 = new UnsavedFileAttachment(new File("fixture", "bonsai-boston.jpg"), "image/jpeg");
            mpw.addAttachment(att0);
            mpw.close();

            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            int amountRead = 0;
            int totalRead = 0;

            do {
                byte buf[] = new byte[chunkSize];
                amountRead = mpw.read(buf);
                if (amountRead > 0) {
                    totalRead += amountRead;
                }
            } while(amountRead > 0);

            Assert.assertEquals(totalRead, mpw.getContentLength());

        } catch (Exception e) {
            System.out.println("aarg "+e);
        }
    }

}
