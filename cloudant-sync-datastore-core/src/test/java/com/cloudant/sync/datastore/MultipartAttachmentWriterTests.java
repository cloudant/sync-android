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

import com.cloudant.sync.util.TestUtils;

import org.junit.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

/**
 * Created by tomblench on 24/02/2014.
 */

@RunWith(Parameterized.class)
public class MultipartAttachmentWriterTests {

    String datastore_manager_dir;
    Database database = null;
    DocumentStore documentStore = null;

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
        documentStore = DocumentStore.getInstance(new File(datastore_manager_dir, this.getClass().getName()));
        database = documentStore.database;
        jsonData = "{\"body\":\"This is a body.\"}".getBytes();
        bodyOne = DocumentBodyImpl.bodyWith(jsonData);
    }

    @After
    public void tearDown() throws Exception {
        documentStore.close();
        TestUtils.deleteTempTestingDir(datastore_manager_dir);
    }

    @Test
    public void Add1000TextAttachmentsTest() throws Exception {
        DocumentRevision docMut = new DocumentRevision();
        docMut.setBody(bodyOne);
        DocumentRevision doc = database.createDocumentFromRevision(docMut);

        MultipartAttachmentWriter mpw = new MultipartAttachmentWriter();
        mpw.setBody(doc.asMap());

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
            mpw.addAttachment(att0, bytes.length);
        }
        InputStream is = mpw.makeInputStream();

        int amountRead = 0;
        int totalRead = 0;

        do {
            byte buf[] = new byte[chunkSize];
            amountRead = is.read(buf);
            if (amountRead > 0) {
                totalRead += amountRead;
                System.out.print(new String(buf, 0, amountRead));
            }
        } while(amountRead > 0);

        Assert.assertEquals(totalRead, mpw.getContentLength());
    }


    @Test
    public void AddImageAttachmentTest() throws Exception {
        DocumentRevision docMut = new DocumentRevision();
        docMut.setBody(bodyOne);
        DocumentRevision doc = database.createDocumentFromRevision(docMut);

        MultipartAttachmentWriter mpw = new MultipartAttachmentWriter();
        mpw.setBody(doc.asMap());

        File f = TestUtils.loadFixture("fixture/bonsai-boston.jpg");
        Attachment att0 = new UnsavedFileAttachment(f, "image/jpeg");
        mpw.addAttachment(att0, f.length());
        InputStream is = mpw.makeInputStream();

        int amountRead = 0;
        int totalRead = 0;

        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        do {
            byte buf[] = new byte[chunkSize];
            amountRead = is.read(buf);
            if (amountRead > 0) {
                bos.write(buf, 0, amountRead);
                totalRead += amountRead;
            }
        } while(amountRead > 0);

        bos.flush();
        bos.close();
        Assert.assertEquals(totalRead, mpw.getContentLength());
        FileInputStream fis = new FileInputStream(TestUtils.loadFixture("fixture/AddImageAttachmentTest_expected.mime"));
        Assert.assertTrue(TestUtils.streamsEqual(fis, new ByteArrayInputStream(bos.toByteArray())));
    }

}
