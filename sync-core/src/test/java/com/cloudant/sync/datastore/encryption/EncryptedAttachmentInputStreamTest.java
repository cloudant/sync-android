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
import org.junit.Test;
import org.junit.Assert;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.NoSuchPaddingException;

/**
 * Test encrypting an attachment to check correct on disk format is read.
 */
public class EncryptedAttachmentInputStreamTest {

    /**
     * JavaSE has default 16-byte key length limit, so we use that in testing to make
     * CI set up easier. Given this is just a key, it shouldn't affect underlying testing.
     */
    private byte[] key = hexStringToByteArray("F4F57C148B3A836EC6D74D0672DB4FC2");

    @Test
    public void testReadingValidFile() throws IOException, InvalidAlgorithmParameterException,
            NoSuchAlgorithmException, InvalidKeyException, NoSuchPaddingException {
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
