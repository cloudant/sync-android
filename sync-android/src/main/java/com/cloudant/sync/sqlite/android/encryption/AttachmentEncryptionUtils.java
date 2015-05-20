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

package com.cloudant.sync.sqlite.android.encryption;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Formatter;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Created by estebanmlaver on 5/13/15.
 */
public class AttachmentEncryptionUtils {

    private static final String UTF_8 = "UTF-8"; //$NON-NLS-1$
    private static final int PBKDF2_LENGTH_BYTE_SIZE = 32;
    private static final int IV_NUM_BYTES = 16;
    private static final int BYTES_FOR_SALT = 32;

    public static final String CYPHER_TEXT_LABEL = "ct"; //$NON-NLS-1$
    public static final String IV_LABEL = "iv"; //$NON-NLS-1$
    public static final String ENCRYPTION_SOURCE_LABEL = "src"; //$NON-NLS-1$
    public static final String VERSION_LABEL = "v"; //$NON-NLS-1$

    private static final String ENCRYPTION_SOURCE = "java"; //$NON-NLS-1$

    private static com.cloudant.sync.sqlite.android.encryption.AttachmentEncryptionUtils singleton;

    private AttachmentEncryptionUtils(){ }

    /**
     * Only return one instance of this factory
     * @return
     */
    public static synchronized AttachmentEncryptionUtils getInstance() {
        if (singleton == null) {
            singleton = new AttachmentEncryptionUtils();
        }
        return singleton;
    }

    public Cipher getCipher(String key, File attachFile, int mode, byte[] iv) throws SecurityUtilsException {
        if(key == null || key.length() <= 0){
            throw new SecurityUtilsException("Key cannot be null or empty.");
        }

        if(attachFile == null || attachFile.length() <= 0){
            throw new SecurityUtilsException("Attachement cannot be null or empty.");
        }

        Cipher cipher = null;
        try {
            //Args for mode (encrypt or decrypt), sqlcipher key in bytes, and IV in bytes
            cipher = initCipher(mode, hexStringToByteArray(key), iv);

            //byte[] inputBytes = new byte[(int) attachFile.length()];
            //byte[] outputBytes = cipher.doFinal(inputBytes);
        } catch (InvalidKeyException e1) {
            throw new SecurityUtilsException("Problem occured while encrypting. Make sure the given key is valid.", e1);
        } catch (NoSuchAlgorithmException e1) {
            throw new SecurityUtilsException("Problem occured while encrypting. Make sure the given key is valid.", e1);
        } catch (NoSuchPaddingException e1) {
            throw new SecurityUtilsException("Problem occured while encrypting. Make sure the given key is valid.", e1);
        } catch (InvalidAlgorithmParameterException e1) {
            throw new SecurityUtilsException("Problem occured while encrypting. Make sure the given key is valid.", e1);
        }

        return cipher;
    }

    /**
     * Encrypt the given text with the given key. The encrypted text is included in the JSONObject that is returned.
     *
     * @param key the key with which to encrypt the text (hexadecimal encoded string)
     * @param decryptedAttachFile the attachment file to encrypt
     * @return JSONObject that contains the result
     * @throws SecurityUtilsException if the key or plainText is empty or null, or if the key is invalid and there is a problem while encrypting
     */
    public void encrypt(String key, File decryptedAttachFile) throws SecurityUtilsException, IOException {
        if(key == null || key.length() <= 0){
            throw new SecurityUtilsException("Key cannot be null or empty.");
        }

        if(decryptedAttachFile == null && !decryptedAttachFile.exists()){
            throw new SecurityUtilsException("Attachement cannot be null or empty.");
        }

        Cipher cipher = null;
        byte[] encryptedBytes = null;

        //Generate IV
        byte[] iv = generateIV(IV_NUM_BYTES);

        try {
            cipher = getCipher(key, decryptedAttachFile, Cipher.ENCRYPT_MODE, iv);

            byte[] inputBytes = new byte[(int) decryptedAttachFile.length()];

            encryptedBytes = cipher.doFinal(inputBytes);
        } catch (BadPaddingException e) {
            throw new SecurityUtilsException("Padding problem occured while encrypting. Make sure the given key is valid.", e);
        } catch (IllegalBlockSizeException e) {
            throw new SecurityUtilsException("Problem occured while encrypting. Make sure the given key is valid.", e);
        }

        String hexEncodedCypherText = byteArrayToHexString(encryptedBytes);

        //Attachment bean to save encryption variables in Android storage which will be used to decrypt

        /*try {
            AttachmentKeyBean keyBean = new AttachmentKeyBean(hexEncodedCypherText, iv)
        } catch (JSONException e) {
            e.printStackTrace();
        }*/

        JSONObject encryptedObject = new JSONObject();

        try{
            encryptedObject.put(CYPHER_TEXT_LABEL, hexEncodedCypherText);
            encryptedObject.put(IV_LABEL, iv);
            encryptedObject.put(ENCRYPTION_SOURCE_LABEL, ENCRYPTION_SOURCE);
            //encryptedObject.put(VERSION_LABEL, CURRENT_VERSION);

            //return encryptedObject;
        }
        catch(JSONException e){
            //Should not happen, as only new properties are being added to the JSONObject
            throw new SecurityUtilsException("There was a problem while adding properties to the returned JSONObject.", e);
        }
    }

    /**
     * Decrypt the given encrypted object with the given key. The encrypted object must be the output of the encrypt function.
     *
     * @param key the key that is used to encrypt the object (hexadecimal encoded string)
     * @param encryptedFile the object to be decrypted
     * @return String the decrypted text, in UTF-8 encoding
     * @throws SecurityUtilsException if the key is null or empty, or if the encryptedObject is null or invalid, or was encrypted in another environment
     */
    public String decrypt(String key, File encryptedFile) throws SecurityUtilsException {
        if(key == null || key.length() <= 0){
            throw new SecurityUtilsException("Key cannot be null or empty.");
        }
        /*if(attach == null || attach.length() <= 0 || !attach.has(CYPHER_TEXT_LABEL) || !attach.has(IV_LABEL) || !attach.has(ENCRYPTION_SOURCE_LABEL) || !encryptedObject.has(VERSION_LABEL)){
            throw new SecurityUtilsException("The given encrypted object is invalid or null.");
        }*/
        if(encryptedFile == null) {
            throw new SecurityUtilsException("The given encrypted object is invalid or null.");
        }

        String cipherText = "";
        //Generate IV
        byte[] iv = generateIV(IV_NUM_BYTES);

        try {
            /*if(!encryptedObject.getString(ENCRYPTION_SOURCE_LABEL).equals(ENCRYPTION_SOURCE)){
                throw new SecurityUtilsException("The encrypted object was generated in another environment. Cannot decrypt in this environment.");
            }

            cipherText = encryptedObject.getString(CYPHER_TEXT_LABEL);
            String iv = encryptedObject.getString(IV_LABEL);
            */
            byte[] bytes = hexStringToByteArray(cipherText);

            Cipher cipher = initCipher(Cipher.DECRYPT_MODE, hexStringToByteArray(key), iv);

            byte[] original = cipher.doFinal(bytes);

            return new String(original, UTF_8);
        } catch (Exception e) {
            throw new SecurityUtilsException("There was a problem while adding properties to the returned JSONObject.", e);
        }
    }

    /**
     * Generate a hexadecimal-encoded random byte string locally with the given byte length.
     * @param byteLength the length of the string to be generated, in byte size
     * @return hexadecimal-encoded string of the generated random string
     */
    public static String getRandomString(int byteLength){
        byte[] randomByteArray = generateLocalKey(byteLength);
        return encodeBytesAsHexString(randomByteArray);
    }

    public static final byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }

    //Called if the user requests to generate the key locally, we gen all 256 bits,
    //so no need to use PBKDF2 like the method above
    public static byte[] generateLocalKey(int numBytes){
        byte[] randBytes = new byte[numBytes];
        new SecureRandom().nextBytes (randBytes);
        return randBytes;
    }

    public static String encodeBytesAsHexString (byte bytes[]) {
        StringBuilder result = new StringBuilder();

        if (bytes != null) {
            for (byte curByte : bytes) {
                result.append (String.format ("%02X", curByte)); //$NON-NLS-1$
            }
        }

        return result.toString();
    }


    //TODO Encryption/cipher algorithm and details should be same as key management
    static Cipher initCipher(int mode, byte[] key, byte[] iv) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException {

        IvParameterSpec ivspec = new IvParameterSpec(iv);
        SecretKeySpec skeySpec = new SecretKeySpec(key, "AES"); //$NON-NLS-1$

        // initialize the cipher for encrypt mode
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding"); //$NON-NLS-1$
        cipher.init(mode, skeySpec, ivspec);

        return cipher;
    }

    public static final String byteArrayToHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);

        Formatter formatter = new Formatter(sb);
        for (byte b : bytes) {
            formatter.format("%02x", b);
        }
        formatter.close();
        return sb.toString();
    }

    public static void encryptSourceFile(File file) {
    }

    //Method from JSONStore to generate IV as part of sqlcipher key encryption
    public static byte[] generateIV (int numBytes) {
        byte[] iv = new byte[numBytes];
        new SecureRandom().nextBytes (iv);
        return iv;
    }
}
