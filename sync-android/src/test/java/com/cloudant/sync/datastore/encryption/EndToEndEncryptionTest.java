/**
 * Copyright (c) 2015 IBM Cloudant. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.datastore.encryption;

import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.DatastoreNotCreatedException;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentException;
import com.cloudant.sync.datastore.MutableDocumentRevision;
import com.cloudant.sync.datastore.UnsavedFileAttachment;
import com.cloudant.sync.query.IndexManager;

import net.sqlcipher.database.SQLiteDatabase;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsNot;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;


/**
 * Test that when you open a database with an encryption key, the content on disk
 * is all encrypted.
 */
@RunWith(Parameterized.class)
public class EndToEndEncryptionTest {

    static {
        // Load SQLCipher libraries
        SQLiteDatabase.loadLibs(ProviderTestUtil.getContext());
    }

    @Parameterized.Parameters(name = "{index}: encryption={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {false}, {true},
        });
    }

    @Parameterized.Parameter
    public boolean dataShouldBeEncrypted;

    String datastoreManagerDir;
    DatastoreManager datastoreManager;
    Datastore datastore;

    // Magic bytes are "SQLite format 3" + null-terminator
    byte[] sqlCipherMagicBytes = hexStringToByteArray("53514c69746520666f726d6174203300");
    byte[] expectedFirstAttachmentByte = new byte[]{ 1 };

    @Before
    public void setUp() throws DatastoreNotCreatedException {
        datastoreManagerDir = TestUtils.createTempTestingDir(this.getClass().getName());
        datastoreManager = new DatastoreManager(this.datastoreManagerDir);

        if(dataShouldBeEncrypted) {
            this.datastore = this.datastoreManager.openDatastore(getClass().getSimpleName(),
                    new SimpleKeyProvider(new byte[] { -123, 53, -22, -15, -123, 53, -22, -15, 53, -22, -15,
                            -123, -22, -15, 53, -22, -123, 53, -22, -15, -123, 53, -22, -15, 53, -22,
                            -15, -123, -22, -15, 53, -22 }));
        } else {
            this.datastore = this.datastoreManager.openDatastore(getClass().getSimpleName());
        }
    }

    @After
    public void tearDown() {
        TestUtils.deleteTempTestingDir(datastoreManagerDir);
    }

    @Test
    public void jsonDataEncrypted() throws IOException {
        File jsonDatabase = new File(datastoreManagerDir
                + File.separator + "EndToEndEncryptionTest"
                + File.separator + "db.sync");

        // Database creation happens in the background, so we need to call a blocking
        // database operation to ensure the database exists on disk before we look at
        // it.

        IndexManager im = new IndexManager(this.datastore);
        im.ensureIndexed(Arrays.<Object>asList("name", "age"));


        InputStream in = new FileInputStream(jsonDatabase);
        byte[] magicBytesBuffer = new byte[sqlCipherMagicBytes.length];
        int readLength = in.read(magicBytesBuffer);

        assertEquals("Didn't read full buffer", magicBytesBuffer.length, readLength);

        if(dataShouldBeEncrypted) {
            assertThat("SQLite magic bytes found in file that should be encrypted",
                    sqlCipherMagicBytes, IsNot.not(IsEqual.equalTo(magicBytesBuffer)));
        } else {
            assertThat("SQLite magic bytes not found in file that should not be encrypted",
                    sqlCipherMagicBytes, IsEqual.equalTo(magicBytesBuffer));
        }
    }

    @Test
    public void indexDataEncrypted() throws IOException {

        IndexManager im = new IndexManager(this.datastore);
        im.ensureIndexed(Arrays.<Object>asList("name", "age"));

        File jsonDatabase = new File(datastoreManagerDir
                + File.separator + "EndToEndEncryptionTest"
                + File.separator + "extensions"
                + File.separator + "com.cloudant.sync.query"
                + File.separator + "indexes.sqlite");

        InputStream in = new FileInputStream(jsonDatabase);
        byte[] magicBytesBuffer = new byte[sqlCipherMagicBytes.length];
        int readLength = in.read(magicBytesBuffer);

        assertEquals("Didn't read full buffer", magicBytesBuffer.length, readLength);

        if(dataShouldBeEncrypted) {
            assertThat("SQLite magic bytes found in file that should be encrypted",
                    sqlCipherMagicBytes, IsNot.not(IsEqual.equalTo(magicBytesBuffer)));
        } else {
            assertThat("SQLite magic bytes not found in file that should not be encrypted",
                    sqlCipherMagicBytes, IsEqual.equalTo(magicBytesBuffer));
        }
    }

    @Test
    public void attachmentsDataEncrypted() throws IOException, DocumentException {

        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.body = DocumentBodyFactory.create(new HashMap<String, String>());

        File expectedPlainText = TestUtils.loadFixture("fixture/EncryptedAttachmentTest_plainText");
        assertNotNull(expectedPlainText);

        UnsavedFileAttachment attachment = new UnsavedFileAttachment(
                expectedPlainText, "text/plain");
        rev.attachments.put("EncryptedAttachmentTest_plainText", attachment);

        datastore.createDocumentFromRevision(rev);

        File attachmentsFolder = new File(datastoreManagerDir
                + File.separator + "EndToEndEncryptionTest"
                + File.separator + "extensions"
                + File.separator + "com.cloudant.attachments");

        File[] contents = attachmentsFolder.listFiles();
        assertNotNull("Didn't find expected attachments folder", contents);
        assertThat("Didn't find expected file in attachments", contents.length, IsEqual.equalTo(1));
        InputStream in = new FileInputStream(contents[0]);

        if(dataShouldBeEncrypted) {

            byte[] actualContent = new byte[expectedFirstAttachmentByte.length];
            int readLength = in.read(actualContent);
            assertEquals("Didn't read full buffer", actualContent.length, readLength);
            assertArrayEquals("First byte not version byte", expectedFirstAttachmentByte, actualContent);

        } else {
            assertTrue(IOUtils.contentEquals(new FileInputStream(expectedPlainText), in));
        }
    }

    public static byte[] hexStringToByteArray(String s) {
        try {
            return Hex.decodeHex(s.toCharArray());
        } catch (DecoderException ex) {
            // Crash the tests at this point, we've input bad data in our hard-coded values
            throw new RuntimeException("Error decoding hex data: " + s);
        }
    }

}
