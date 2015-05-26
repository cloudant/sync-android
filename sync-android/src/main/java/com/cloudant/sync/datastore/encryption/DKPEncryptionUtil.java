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

import android.os.Build;

import org.json.JSONException;
import org.json.JSONObject;

import java.security.SecureRandom;
import java.util.Formatter;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;

/**
 * A utility to aid in encrypting/decrypting the DPK
 */
class DKPEncryptionUtil {

    /**
     * Convert a String to a hex byte array
     *
     * @param s The string
     * @return The hex byte array
     */
    public static final byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    /**
     * Convert a hex byte array back to a String
     *
     * @param bytes The hex byte array
     * @return The string
     */
    public static final String byteArrayToHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);

        Formatter formatter = new Formatter(sb);
        for (byte b : bytes) {
            formatter.format("%02x", b);
        }
        formatter.close();
        return sb.toString();
    }

    /**
     * AES Encrypt a byte array
     *
     * @param key              The encryption key
     * @param iv               The iv
     * @param unencryptedBytes The data to encrypt
     * @return The encrypted data
     * @throws Exception
     */
    public static byte[] encryptAES(SecretKey key, byte[] iv, byte[] unencryptedBytes) throws
            Exception {
        Cipher aesCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        IvParameterSpec ivParameter = new IvParameterSpec(iv);
        aesCipher.init(Cipher.ENCRYPT_MODE, key, ivParameter);
        return aesCipher.doFinal(unencryptedBytes);
    }

    /**
     * Decrypt an AES encrypted byte array
     *
     * @param key            The encryption key
     * @param iv             The iv
     * @param encryptedBytes The data to decrypt
     * @return The decrypted data
     * @throws Exception
     */
    public static byte[] decryptAES(SecretKey key, byte[] iv, byte[] encryptedBytes) throws
            Exception {
        Cipher aesCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        IvParameterSpec ivParameter = new IvParameterSpec(iv);
        aesCipher.init(Cipher.DECRYPT_MODE, key, ivParameter);
        return aesCipher.doFinal(encryptedBytes);
    }
}