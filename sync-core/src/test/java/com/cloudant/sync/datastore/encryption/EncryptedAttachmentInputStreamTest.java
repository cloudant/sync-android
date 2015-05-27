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

package com.cloudant.sync.datastore.encryption;

import com.cloudant.sync.matcher.CauseMatcher;
import com.cloudant.sync.util.TestUtils;

import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Assert;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;

import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

/**
 * Test encrypting an attachment to check correct on disk format is read.
 */
public class EncryptedAttachmentInputStreamTest {

    /** Allow us to check exception messages */
    @Rule
    public ExpectedException exception = ExpectedException.none();

    /**
     * JavaSE has default 16-byte key length limit, so we use that in testing to make
     * CI set up easier. Given this is just a key, it shouldn't affect underlying testing.
     */
    private byte[] key = hexStringToByteArray("F4F57C148B3A836EC6D74D0672DB4FC2");

    byte[] keyLength31 = new byte[] { -123, 53, -22, -15, -123, 53, -22, -15, 53, -22, -15,
            -123, -22, -15, 53, -22, -123, 53, -22, -15, -123, 53, -22, -15, 53, -22,
            -15, -123, -22, -15, 53 };

    @Test
    public void testReadingValidFile() throws IOException, InvalidKeyException {
        File encryptedAttachmentBlob = TestUtils.loadFixture(
                "fixture/EncryptedAttachmentTest_cipherText_aes128");
        File expectedPlainText = TestUtils.loadFixture(
                "fixture/EncryptedAttachmentTest_plainText");

        InputStream encryptedInputStream = new EncryptedAttachmentInputStream(
                new FileInputStream(encryptedAttachmentBlob), key);

        InputStream plainTextInputStream = new FileInputStream(expectedPlainText);

        Assert.assertTrue("Reading encrypted stream didn't give expected plain text",
                IOUtils.contentEquals(encryptedInputStream, plainTextInputStream));
    }

    /**
     * Test we reject on-disk versions newer than latest.
     */
    @Test
    public void testReadingTooNewVersionFile() throws IOException, InvalidKeyException {
        exception.expect(IOException.class);
        exception.expectMessage("Unsupported on-disk version for attachment decryption.");

        File encryptedAttachmentBlob = TestUtils.loadFixture(
                "fixture/EncryptedAttachmentTest_badOnDiskVersion");

        InputStream encryptedInputStream = new EncryptedAttachmentInputStream(
                new FileInputStream(encryptedAttachmentBlob), key);
    }

    /**
     * Test when we read file shorter than IV that we fail during construction.
     */
    @Test
    public void testReadingTruncatedIVFile() throws IOException, InvalidKeyException {
        exception.expect(IOException.class);
        exception.expectMessage("Could not read initialisation vector from file header.");

        File encryptedAttachmentBlob = TestUtils.loadFixture(
                "fixture/EncryptedAttachmentTest_truncatedIV");
        InputStream encryptedInputStream = new EncryptedAttachmentInputStream(
                new FileInputStream(encryptedAttachmentBlob), key);
    }

    /**
     * Test when we read file shorter than block multiple that an IOException escapes.
     */
    @Test
    public void testReadingTruncatedDataFile() throws IOException, InvalidKeyException {
        exception.expect(IOException.class);
        exception.expectCause(new CauseMatcher(IllegalBlockSizeException.class));

        File encryptedAttachmentBlob = TestUtils.loadFixture(
                "fixture/EncryptedAttachmentTest_truncatedData");
        InputStream encryptedInputStream = new EncryptedAttachmentInputStream(
                new FileInputStream(encryptedAttachmentBlob), key);

        //noinspection StatementWithEmptyBody
        while (encryptedInputStream.read(new byte[1024]) != -1) {
            // just read the data
        }
    }

    @Test(expected=InvalidKeyException.class)
    public void Test31ByteKey() throws IOException, InvalidKeyException {
        new EncryptedAttachmentInputStream(null, keyLength31);
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }

}
