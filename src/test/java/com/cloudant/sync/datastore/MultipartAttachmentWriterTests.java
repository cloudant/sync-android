package com.cloudant.sync.datastore;

import com.cloudant.sync.util.TestUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

/**
 * Created by tomblench on 24/02/2014.
 */
public class MultipartAttachmentWriterTests {

    String documentOneFile = "fixture/document_1.json";
    String documentTwoFile = "fixture/document_2.json";


    String datastore_manager_dir;
    DatastoreManager datastoreManager;
    Datastore datastore = null;


    byte[] jsonData = null;
    DocumentBody bodyOne = null;

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
    public void Test1() {
        try {
            // TODO some asserts etc
            DocumentRevision doc = datastore.createDocument(bodyOne);
            ArrayList<Attachment> attachments = new ArrayList<Attachment>();
            for (int i=0; i<1000; i++) {
                String name = "attachment" + UUID.randomUUID();
                StringBuilder s = new StringBuilder();
                s.append("this is some data for ");
                s.append(name);
                for (int c=0;c<Math.random()*100;c++) {
                    s.append("+");
                }
                byte[] bytes = (s.toString()).getBytes();
                Attachment att0 = new UnsavedStreamAttachment(new ByteArrayInputStream(bytes), name, "image/jpeg");
                attachments.add(att0);
            }

            MultipartAttachmentWriter mpw = new MultipartAttachmentWriter(datastore, doc, attachments);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            int chunkSize = 3;
            int amountRead = 0;

            do {
                byte buf[] = new byte[chunkSize];
                amountRead = mpw.read(buf);
                if (amountRead > 0) {
                    System.out.print(new String(buf, 0, amountRead));
                }
            } while(amountRead > 0);
        } catch (Exception e) {
            System.out.println("aarg "+e);
        }
    }

}
