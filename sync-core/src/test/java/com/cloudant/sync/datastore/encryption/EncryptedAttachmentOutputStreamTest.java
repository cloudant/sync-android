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

import com.cloudant.sync.util.TestUtils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.NoSuchPaddingException;

/**
 * Test encrypting an attachment to check correct on disk format is written.
 */
public class EncryptedAttachmentOutputStreamTest {

    /**
     * JavaSE has default 16-byte key length limit, so we use that in testing to make
     * CI set up easier. Given this is just a key, it shouldn't affect underlying testing.
     */
    private byte[] key = EncryptedAttachmentInputStreamTest.hexStringToByteArray(
            "F4F57C148B3A836EC6D74D0672DB4FC2");
    private byte[] iv = EncryptedAttachmentInputStreamTest.hexStringToByteArray(
            "CA7806E5DA7F83ACE8C0BB92BBF76326");  // Always 16 bytes

    @Test
    public void testWritingValidFile() throws IOException, InvalidAlgorithmParameterException,
            NoSuchAlgorithmException, InvalidKeyException, NoSuchPaddingException {
        File expectedCipherText = TestUtils.loadFixture(
                "fixture/EncryptedAttachmentTest_cipherText_aes128");
        File plainText = TestUtils.loadFixture(
                "fixture/EncryptedAttachmentTest_plainText");

        ByteArrayOutputStream actualEncryptedOutput = new ByteArrayOutputStream();

        OutputStream encryptedOutputStream = new EncryptedAttachmentOutputStream(
                actualEncryptedOutput, key, iv);
        IOUtils.copy(new FileInputStream(plainText), encryptedOutputStream);
        encryptedOutputStream.close();
        actualEncryptedOutput.close();

        Assert.assertTrue("Writing to encrypted stream didn't give expected cipher text",
                IOUtils.contentEquals(
                        new ByteArrayInputStream(actualEncryptedOutput.toByteArray()),
                        new FileInputStream(expectedCipherText)));
    }

    @Test(expected=InvalidKeyException.class)
    public void Test31ByteKey() throws NoSuchPaddingException, NoSuchAlgorithmException,
            InvalidAlgorithmParameterException, InvalidKeyException, IOException {
        new EncryptedAttachmentOutputStream(null, keyLength31, ivLength16);
    }

    byte[] keyLength31 = new byte[] { -123, 53, -22, -15, -123, 53, -22, -15, 53, -22, -15,
            -123, -22, -15, 53, -22, -123, 53, -22, -15, -123, 53, -22, -15, 53, -22,
            -15, -123, -22, -15, 53 };

    byte[] ivLength16 = new byte[] { -123, 53, -22, -15, -123, 53, -22, -15, 53, -22, -15,
            -123, -22, -15, 53, -22 };
}
