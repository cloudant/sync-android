/**
 * Copyright (c) 2015 IBM Cloudant, Inc. All rights reserved.
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

import android.os.Build;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.json.JSONException;
import org.json.JSONObject;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Formatter;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;

/**
 * A utility to aid in encrypting/decrypting the DPK
 */
class DPKEncryptionUtil {

    /**
     * Convert a String to a hex byte array
     *
     * @param s The string
     * @return The hex byte array
     */
    public static final byte[] hexStringToByteArray(String s) throws DecoderException {
        return Hex.decodeHex(s.toCharArray());
    }

    /**
     * Convert a hex byte array back to a String
     *
     * @param bytes The hex byte array
     * @return The string
     */
    public static final String byteArrayToHexString(byte[] bytes) {
        return new String(new Hex().encode(bytes));
    }

    /**
     * AES Encrypt a byte array
     *
     * @param key              The encryption key
     * @param iv               The iv
     * @param unencryptedBytes The data to encrypt
     * @return The encrypted data
     * @throws NoSuchPaddingException
     * @throws NoSuchAlgorithmException
     * @throws InvalidAlgorithmParameterException
     * @throws InvalidKeyException
     * @throws BadPaddingException
     * @throws IllegalBlockSizeException
     */
    public static byte[] encryptAES(SecretKey key, byte[] iv, byte[] unencryptedBytes) throws
            NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException,
            InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
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
     * @throws NoSuchPaddingException
     * @throws NoSuchAlgorithmException
     * @throws InvalidAlgorithmParameterException
     * @throws InvalidKeyException
     * @throws BadPaddingException
     * @throws IllegalBlockSizeException
     */
    public static byte[] decryptAES(SecretKey key, byte[] iv, byte[] encryptedBytes) throws
            NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException,
            InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
        Cipher aesCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        IvParameterSpec ivParameter = new IvParameterSpec(iv);
        aesCipher.init(Cipher.DECRYPT_MODE, key, ivParameter);
        return aesCipher.doFinal(encryptedBytes);
    }
}
