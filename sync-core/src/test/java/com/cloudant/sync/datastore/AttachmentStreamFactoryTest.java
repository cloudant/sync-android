/**
 * Copyright (c) 2015 IBM Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.datastore;

import com.cloudant.sync.datastore.encryption.EncryptedAttachmentInputStream;
import com.cloudant.sync.datastore.encryption.EncryptionTestConstants;
import com.cloudant.sync.datastore.encryption.NullKeyProvider;
import com.cloudant.sync.util.TestUtils;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

/**
 * Test the AttachmentStreamFactory for encrypted and unencrypted streams.
 */
public class AttachmentStreamFactoryTest extends BasicDatastoreTestBase {

    // =======================
    // Unencrypted, un-encoded
    // =======================

    @Test
    /**
     * Assert passing an AttachmentStreamFactory with no key writes an unencrypted file to disk.
     */
    public void testPreparedAttachmentWritesUnencryptedUnencodedStream()
            throws AttachmentException, IOException {

        AttachmentStreamFactory asf = new AttachmentStreamFactory(new NullKeyProvider());

        File plainText = f("fixture/EncryptedAttachmentTest_plainText");

        UnsavedFileAttachment usf = new UnsavedFileAttachment(plainText, "text/plain");
        PreparedAttachment preparedAttachment = new PreparedAttachment(
                usf, datastore_manager_dir, 0, asf);

        Assert.assertTrue("Writing to unencrypted, un-encoded stream didn't give correct output",
                IOUtils.contentEquals(
                        new FileInputStream(preparedAttachment.tempFile),
                        new FileInputStream(plainText)));

    }

    @Test
    /**
     * Assert passing an AttachmentStreamFactory with no key reads an unencrypted file correctly.
     */
    public void testSavedAttachmentReadsUnencryptedUnencodedStream()
            throws AttachmentException, IOException {

        AttachmentStreamFactory asf = new AttachmentStreamFactory(new NullKeyProvider());

        File plainText = f("fixture/EncryptedAttachmentTest_plainText");
        SavedAttachment savedAttachment = new SavedAttachment(0, "test", null, "text/plain", Attachment.Encoding.Plain, 0, 0, 0,
                plainText, asf);

        Assert.assertTrue("Reading unencrypted, un-encoded stream didn't give correct output",
                IOUtils.contentEquals(
                        savedAttachment.getInputStream(),
                        new FileInputStream(plainText)));

    }

    @Test
    /**
     * Assert passing an AttachmentStreamFactory with no key to both a PreparedAttachment (writer)
     * and SavedAttachment (reader) round trips the data correctly.
     */
    public void testSavedAttachmentReadsPreparedAttachmentUnencryptedUnencodedStream()
            throws AttachmentException, IOException {

        AttachmentStreamFactory asf = new AttachmentStreamFactory(new NullKeyProvider());

        File plainText = f("fixture/EncryptedAttachmentTest_plainText");

        UnsavedFileAttachment usf = new UnsavedFileAttachment(plainText, "text/plain");
        PreparedAttachment preparedAttachment = new PreparedAttachment(
                usf, datastore_manager_dir, 0, asf);

        Assert.assertTrue("Writing to unencrypted, unencoded stream didn't give correct output",
                IOUtils.contentEquals(new FileInputStream(preparedAttachment.tempFile),
                        new FileInputStream(plainText)));

        SavedAttachment savedAttachment = new SavedAttachment(0, "test", null, "text/plain", Attachment.Encoding.Plain, 0, 0, 0,
                preparedAttachment.tempFile, asf);

        Assert.assertTrue("Reading from written unencrypted, unencoded blob didn't give correct output",
                IOUtils.contentEquals(
                        savedAttachment.getInputStream(),
                        new FileInputStream(plainText)));
    }

    // =======================
    // Unencrypted, encoded
    // =======================

    @Test
    /**
     * Assert passing an AttachmentStreamFactory with no key writes an unencrypted and
     * gzipped file to disk.
     */
    public void testPreparedAttachmentWritesUnencryptedEncodedStream()
            throws AttachmentException, IOException {

        // We can't create a GZipped file right now using the attachment classes,
        // so test using factory directly.

        AttachmentStreamFactory asf = new AttachmentStreamFactory(new NullKeyProvider());

        File plainText = f("fixture/EncryptedAttachmentTest_plainText");
        File zippedPlainText = f("fixture/EncryptedAttachmentTest_plainText.gz");

        File actualOutput = new File(datastore_manager_dir, "temp" + UUID.randomUUID());
        OutputStream out = asf.getOutputStream(actualOutput, Attachment.Encoding.Gzip);

        IOUtils.copy(new FileInputStream(plainText), out);
        out.close();

        Assert.assertTrue("Writing to unencrypted, encoded stream didn't give correct output",
                IOUtils.contentEquals(
                        new FileInputStream(actualOutput),
                        new FileInputStream(zippedPlainText)));

    }

    @Test
    /**
     * Assert passing an AttachmentStreamFactory with no key reads an unencrypted and
     * gzipped file correctly.
     */
    public void testSavedAttachmentReadsUnencryptedEncodedStream()
            throws AttachmentException, IOException {

        AttachmentStreamFactory asf = new AttachmentStreamFactory(new NullKeyProvider());

        File plainText = f("fixture/EncryptedAttachmentTest_plainText");
        File zippedPlainText = f("fixture/EncryptedAttachmentTest_plainText.gz");

        SavedAttachment savedAttachment = new SavedAttachment(0, "test", null, "text/plain", Attachment.Encoding.Gzip, 0, 0, 0,
                zippedPlainText, asf);

        Assert.assertTrue("Reading unencrypted, encoded stream didn't give correct output",
                IOUtils.contentEquals(
                        savedAttachment.getInputStream(),
                        new FileInputStream(plainText)));

    }

    // =====================
    // Encrypted, un-encoded
    // =====================

    @Test
    /**
     * Assert passing an AttachmentStreamFactory with known key writes an encrypted file to disk.
     */
    public void testPreparedAttachmentWritesEncryptedUnencodedStream() throws AttachmentException,
            IOException, InvalidKeyException {

        AttachmentStreamFactory asf = new AttachmentStreamFactory(
                EncryptionTestConstants.keyProvider16Byte
        );

        File plainText = f("fixture/EncryptedAttachmentTest_plainText");

        UnsavedFileAttachment usf = new UnsavedFileAttachment(plainText, "text/plain");
        PreparedAttachment preparedAttachment = new PreparedAttachment(
                usf, datastore_manager_dir, 0, asf);

        // We check content using EncryptedAttachmentInputStream. It seems a bit circular,
        // but the aim of this test is to make sure AttachmentStreamFactory is providing
        // encrypted output readable by EncryptedAttachmentInputStream rather than exactly
        // what that output is (EncryptedAttachmentOutputStream's own tests do that).
        Assert.assertTrue("Writing to encrypted, un-encoded stream didn't give correct output",
                IOUtils.contentEquals(
                        new EncryptedAttachmentInputStream(
                                new FileInputStream(preparedAttachment.tempFile),
                                EncryptionTestConstants.key16Byte),
                        new FileInputStream(plainText)));

    }

    @Test
    /**
     * Assert passing an AttachmentStreamFactory with known key reads an encrypted file correctly.
     */
    public void testSavedAttachmentReadsEncryptedUnencodedStream() throws AttachmentException, IOException {

        AttachmentStreamFactory asf = new AttachmentStreamFactory(
                EncryptionTestConstants.keyProvider16Byte
        );

        File plainText = f("fixture/EncryptedAttachmentTest_plainText");
        File cipherAttachmentBlob = f("fixture/EncryptedAttachmentTest_cipherText_aes128");

        SavedAttachment savedAttachment = new SavedAttachment(0, "test", null, "text/plain", Attachment.Encoding.Plain, 0, 0, 0,
                cipherAttachmentBlob, asf);

        Assert.assertTrue("Reading encrypted, un-encoded stream didn't give correct output",
                IOUtils.contentEquals(
                        savedAttachment.getInputStream(),
                        new FileInputStream(plainText)));

    }

    @Test
    /**
     * Assert passing an AttachmentStreamFactory with known key to both a PreparedAttachment
     * (writer) and SavedAttachment (reader) round trips the data correctly.
     */
    public void testSavedAttachmentReadsPreparedAttachmentEncryptedUnencodedStream()
            throws AttachmentException, IOException, InvalidKeyException {

        AttachmentStreamFactory asf = new AttachmentStreamFactory(
                EncryptionTestConstants.keyProvider16Byte
        );

        File plainText = f("fixture/EncryptedAttachmentTest_plainText");

        UnsavedFileAttachment usf = new UnsavedFileAttachment(plainText, "text/plain");
        PreparedAttachment preparedAttachment = new PreparedAttachment(
                usf, datastore_manager_dir, 0, asf);

        Assert.assertTrue("Writing to encrypted stream didn't give correct output",
                IOUtils.contentEquals(
                        new EncryptedAttachmentInputStream(
                                new FileInputStream(preparedAttachment.tempFile),
                                EncryptionTestConstants.key16Byte),
                        new FileInputStream(plainText)));

        SavedAttachment savedAttachment = new SavedAttachment(0, "test", null, "text/plain", Attachment.Encoding.Plain, 0, 0, 0,
                preparedAttachment.tempFile, asf);

        Assert.assertTrue("Reading from written encrypted, un-encoded blob didn't give correct output",
                IOUtils.contentEquals(
                savedAttachment.getInputStream(),
                new FileInputStream(plainText)));

    }

    @Test
    /**
     * Assert that using a different key to write using a PreparedAttachment than the one used to
     * read using a SavedAttachment doesn't give us the original plaintext. That is, make sure
     * using the wrong key does what you'd expect: fails to decrypt the attachment.
     *
     * Note the decryption process will still work as AES doesn't check the plaintext decrypted
     * is correct or not, you just get garbage data out.
     */
    public void testSavedAttachmentCannotReadPreparedAttachmentEncryptedUsingDifferentKey()
            throws AttachmentException, IOException, InvalidKeyException {

        //
        // Encrypt attachment blob using well known key
        //

        AttachmentStreamFactory asf = new AttachmentStreamFactory(
                EncryptionTestConstants.keyProvider16Byte
        );

        File plainText = f("fixture/EncryptedAttachmentTest_plainText");

        UnsavedFileAttachment usf = new UnsavedFileAttachment(plainText, "text/plain");
        PreparedAttachment preparedAttachment = new PreparedAttachment(
                usf, datastore_manager_dir, 0, asf);

        Assert.assertTrue("Writing to encrypted, un-encoded stream didn't give correct output",
                IOUtils.contentEquals(
                        new EncryptedAttachmentInputStream(
                                new FileInputStream(preparedAttachment.tempFile),
                                EncryptionTestConstants.key16Byte),
                        new FileInputStream(plainText)));

        //
        // Attempt to read using a different key
        //

        byte[] otherKey = Arrays.copyOf(EncryptionTestConstants.key16Byte, 16);
        otherKey[12] = (byte) (otherKey[12] - 1);
        AttachmentStreamFactory asf2 = new AttachmentStreamFactory(
                EncryptionTestConstants.keyProviderUsingBytesAsKey(otherKey)
        );
        SavedAttachment savedAttachment = new SavedAttachment(0, "test", null, "text/plain", Attachment.Encoding.Plain, 0, 0, 0,
                preparedAttachment.tempFile, asf2);

        Assert.assertFalse("Reading with different key shouldn't produce plaintext content",
                IOUtils.contentEquals(
                        savedAttachment.getInputStream(),
                        new FileInputStream(plainText)));

    }

    // =====================
    // Encrypted, encoded
    // =====================

    @Test
    /**
     * Assert passing an AttachmentStreamFactory with known key writes an encrypted and
     * gzipped file to disk.
     */
    public void testPreparedAttachmentWritesEncryptedEncodedStream() throws AttachmentException,
            IOException, InvalidKeyException {

        AttachmentStreamFactory asf = new AttachmentStreamFactory(
                EncryptionTestConstants.keyProvider16Byte
        );

        File plainText = f("fixture/EncryptedAttachmentTest_plainText");

        File actualOutput = new File(datastore_manager_dir, "temp" + UUID.randomUUID());
        OutputStream out = asf.getOutputStream(actualOutput, Attachment.Encoding.Gzip);

        IOUtils.copy(new FileInputStream(plainText), out);
        out.close();

        // We check content using EncryptedAttachmentInputStream. It seems a bit circular,
        // but the aim of this test is to make sure AttachmentStreamFactory is providing
        // encrypted output readable by EncryptedAttachmentInputStream rather than exactly
        // what that output is (EncryptedAttachmentOutputStream's own tests do that).
        Assert.assertTrue("Writing to encrypted, encoded stream didn't give correct output",
                IOUtils.contentEquals(
                        new GZIPInputStream(
                                new EncryptedAttachmentInputStream(
                                    new FileInputStream(actualOutput),
                                    EncryptionTestConstants.key16Byte)),
                        new FileInputStream(plainText)));

    }

    @Test
    /**
     * Assert passing an AttachmentStreamFactory with known key reads an encrypted and
     * gzipped file correctly.
     */
    public void testSavedAttachmentReadsEncryptedEncodedStream()
            throws AttachmentException, IOException {

        AttachmentStreamFactory asf = new AttachmentStreamFactory(
                EncryptionTestConstants.keyProvider16Byte
        );

        File plainText = f("fixture/EncryptedAttachmentTest_plainText");
        File cipherAttachmentBlob = f("fixture/EncryptedAttachmentTest_gzip_cipherText_aes128");


        SavedAttachment savedAttachment = new SavedAttachment(0, "test", null, "text/plain", Attachment.Encoding.Gzip, 0, 0, 0,
                cipherAttachmentBlob, asf);

        Assert.assertTrue("Reading encrypted, encoded stream didn't give correct output",
                IOUtils.contentEquals(
                        savedAttachment.getInputStream(),
                        new FileInputStream(plainText)));

    }

    private static File f(String filename) {
        return TestUtils.loadFixture(filename);
    }

}
