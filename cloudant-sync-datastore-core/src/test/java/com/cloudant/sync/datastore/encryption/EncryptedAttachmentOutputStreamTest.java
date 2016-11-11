/*
 * Copyright © 2015 IBM Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.datastore.encryption;

import com.cloudant.sync.internal.datastore.encryption.EncryptedAttachmentOutputStream;
import com.cloudant.sync.util.TestUtils;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.NoSuchPaddingException;

/**
 * Test encrypting an attachment to check correct on disk format is written.
 */
public class EncryptedAttachmentOutputStreamTest {

    @Test
    public void testWritingValidFile() throws IOException, InvalidAlgorithmParameterException,
            NoSuchAlgorithmException, InvalidKeyException, NoSuchPaddingException {
        File expectedCipherText = TestUtils.loadFixture(
                "fixture/EncryptedAttachmentTest_cipherText_aes128");
        File plainText = TestUtils.loadFixture(
                "fixture/EncryptedAttachmentTest_plainText");

        ByteArrayOutputStream actualEncryptedOutput = new ByteArrayOutputStream();

        OutputStream encryptedOutputStream = new EncryptedAttachmentOutputStream(
                actualEncryptedOutput, EncryptionTestConstants.key16Byte, EncryptionTestConstants.iv);
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
        new EncryptedAttachmentOutputStream(
                null, EncryptionTestConstants.keyLength31, EncryptionTestConstants.ivLength16);
    }

}
