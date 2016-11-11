/**
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

import com.cloudant.sync.documentstore.encryption.EncryptionKey;
import com.cloudant.sync.documentstore.encryption.KeyProvider;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/**
 * Hold constants used for encryption tests, including well-known keys.
 */
public class EncryptionTestConstants {

    public static byte[] keyLength32 = new byte[] { -123, 53, -22, -15, -123, 53, -22, -15, 53, -22, -15,
            -123, -22, -15, 53, -22, -123, 53, -22, -15, -123, 53, -22, -15, 53, -22,
            -15, -123, -22, -15, 53, -22 };
    public static byte[] keyLength31 = new byte[] { -123, 53, -22, -15, -123, 53, -22, -15, 53, -22, -15,
            -123, -22, -15, 53, -22, -123, 53, -22, -15, -123, 53, -22, -15, 53, -22,
            -15, -123, -22, -15, 53 };
    public static byte[] keyLength33 = new byte[] { -123, 53, -22, -15, -123, 53, -22, -15, 53, -22, -15,
            -123, -22, -15, 53, -22, -123, 53, -22, -15, -123, 53, -22, -15, 53, -22,
            -15, -123, -22, -15, 53, -22, -123 };
    public static byte[] keyLength16 = new byte[] { -123, 53, -22, -15, -123, 53, -22, -15, 53, -22, -15,
            -123, -22, -15, 53, -22 };

    public static byte[] ivLength16 = new byte[] { -123, 53, -22, -15, -123, 53, -22, -15, 53, -22, -15,
            -123, -22, -15, 53, -22 };

    /**
     * Well-known 16-byte AES key used for fixture encrypted files.
     *
     * JavaSE has default 16-byte key length limit, so we use that in testing to make
     * CI set up easier. Given this is just a key, it shouldn't affect underlying testing.
     */
    public static byte[] key16Byte = hexStringToByteArray("F4F57C148B3A836EC6D74D0672DB4FC2");

    /**
     * A KeyProvider which returns {@link #key16Byte} as its key, allowing use on Java SE.
     */
    public static KeyProvider keyProvider16Byte = new ShortEncryptionKeyProvider(key16Byte);

    /**
     * Well-known 16-byte AES IV used for fixture encrypted files.
     */
    public static byte[] iv = hexStringToByteArray("CA7806E5DA7F83ACE8C0BB92BBF76326");

    public static KeyProvider keyProviderUsingBytesAsKey(byte[] key) {
        return new ShortEncryptionKeyProvider(key);
    }

    private static byte[] hexStringToByteArray(String s) {
        try {
            return Hex.decodeHex(s.toCharArray());
        } catch (DecoderException ex) {
            // Crash the tests at this point, we've input bad data in our hard-coded values
            throw new RuntimeException("Error decoding hex data: " + s);
        }
    }

    /**
     * A KeyProvider which allows returning 16-byte (or any length keys). Used to allow us
     * to provide shorter keys during testing to respect Java SE's 16-byte AES key policy
     * imposed limit.
     */
    private static class ShortEncryptionKeyProvider implements KeyProvider {

        private class ShortEncryptionKey extends EncryptionKey {

            private final byte[] shortKey;

            public ShortEncryptionKey(byte[] key) {
                super(new byte[32]);  // which we don't use.
                this.shortKey = key;
            }

            @Override
            public byte[] getKey() {
                return this.shortKey;
            }

        }

        private final ShortEncryptionKey shortKey;

        public ShortEncryptionKeyProvider(byte[] keyBytes) {
            shortKey = new ShortEncryptionKey(keyBytes);
        }

        @Override
        public EncryptionKey getEncryptionKey() {
            return shortKey;
        }
    }
}
