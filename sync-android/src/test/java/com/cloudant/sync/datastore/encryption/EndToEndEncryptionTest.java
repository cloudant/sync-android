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

import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.ConflictException;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.DatastoreNotCreatedException;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentException;
import com.cloudant.sync.datastore.MutableDocumentRevision;
import com.cloudant.sync.datastore.UnsavedFileAttachment;
import com.cloudant.sync.datastore.UnsavedStreamAttachment;
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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;


/**
 * Test that when you open a database with an encryption key, the content on disk
 * is all encrypted.
 */
@RunWith(Parameterized.class)
public class EndToEndEncryptionTest {

    public static final byte[] KEY = new byte[]{-123, 53, -22, -15, -123, 53, -22, -15, 53, -22, -15,
            -123, -22, -15, 53, -22, -123, 53, -22, -15, -123, 53, -22, -15, 53, -22,
            -15, -123, -22, -15, 53, -22};

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
                    new SimpleKeyProvider(KEY));
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
    public void attachmentsDataEncrypted() throws IOException, DocumentException, InvalidKeyException {

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

            assertTrue("Encrypted attachment did not decrypt correctly", IOUtils.contentEquals(
                    new EncryptedAttachmentInputStream(new FileInputStream(contents[0]), KEY),
                    new FileInputStream(expectedPlainText)));

        } else {
            assertTrue("Unencrypted attachment did not read correctly",
                    IOUtils.contentEquals(new FileInputStream(expectedPlainText), in));
        }
    }

    /**
     * A basic check things round trip successfully.
     */
    @Test
    public void readAndWriteDocument() throws DocumentException, IOException {

        String documentId = "a-test-document";
        final String nonAsciiText = "摇;摃:xx\uD83D\uDC79⌚️\uD83D\uDC7D";

        HashMap<String, String> documentBody = new HashMap<String,String>();
        documentBody.put("name", "mike");
        documentBody.put("pet", "cat");
        documentBody.put("non-ascii", nonAsciiText);

        // Create
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = documentId;
        rev.body = DocumentBodyFactory.create(documentBody);
        BasicDocumentRevision saved = datastore.createDocumentFromRevision(rev);
        assertNotNull(saved);

        // Read
        BasicDocumentRevision retrieved = datastore.getDocument(documentId);
        assertNotNull(retrieved);
        Map<String, Object> retrievedBody = retrieved.getBody().asMap();
        assertEquals("mike", retrievedBody.get("name"));
        assertEquals("cat", retrievedBody.get("pet"));
        assertEquals(nonAsciiText, retrievedBody.get("non-ascii"));
        assertEquals(3, retrievedBody.size());

        // Update
        MutableDocumentRevision update = retrieved.mutableCopy();
        Map<String, Object> updateBody = retrieved.getBody().asMap();
        updateBody.put("name", "fred");
        update.body = DocumentBodyFactory.create(updateBody);
        BasicDocumentRevision updated = datastore.updateDocumentFromRevision(update);
        assertNotNull(updated);
        Map<String, Object> updatedBody = updated.getBody().asMap();
        assertEquals("fred", updatedBody.get("name"));
        assertEquals("cat", updatedBody.get("pet"));
        assertEquals(nonAsciiText, updatedBody.get("non-ascii"));
        assertEquals(3, updatedBody.size());

        // Update with attachments, one from file, one a non-ascii string test
        final String attachmentName = "EncryptedAttachmentTest_plainText";
        File expectedPlainText = TestUtils.loadFixture("fixture/EncryptedAttachmentTest_plainText");
        assertNotNull(expectedPlainText);
        MutableDocumentRevision attachmentRevision = updated.mutableCopy();
        final Map<String, Attachment> atts = attachmentRevision.attachments;
        atts.put(attachmentName, new UnsavedFileAttachment(expectedPlainText, "text/plain"));
        atts.put("non-ascii", new UnsavedStreamAttachment(
                new ByteArrayInputStream(nonAsciiText.getBytes()),
                "non-ascii", "text/plain"));
        BasicDocumentRevision updatedWithAttachment = datastore.updateDocumentFromRevision(attachmentRevision);
        InputStream in = updatedWithAttachment.getAttachments().get(attachmentName).getInputStream();
        assertTrue("Saved attachment did not read correctly",
                IOUtils.contentEquals(new FileInputStream(expectedPlainText), in));
        in = updatedWithAttachment.getAttachments().get("non-ascii").getInputStream();
        assertTrue("Saved attachment did not read correctly",
                IOUtils.contentEquals(new ByteArrayInputStream(nonAsciiText.getBytes()), in));

        // Delete
        try {
            datastore.deleteDocumentFromRevision(saved);
            fail("Deleting document from old revision succeeded");
        } catch (ConflictException ex) {
            // Expected exception
        }
        BasicDocumentRevision deleted = datastore.deleteDocumentFromRevision(updatedWithAttachment);
        assertNotNull(deleted);
        assertEquals(true, deleted.isDeleted());
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
