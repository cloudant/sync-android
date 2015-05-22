/*
 * IBM Confidential OCO Source Materials
 * 
 * 5725-I43 Copyright IBM Corp. 2006, 2013
 * 
 * The source code for this program is not published or otherwise
 * divested of its trade secrets, irrespective of what has
 * been deposited with the U.S. Copyright Office.
 * 
*/

package com.cloudant.sync.sqlite.android.encryption;

import android.os.Build;

import org.apache.commons.codec.binary.Hex;

import java.security.SecureRandom;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

public class SecurityUtils {
     private static final int BYTES_TO_BITS = 8;
     private static final int KEY_SIZE_AES256 = 32;
     protected static final int PBKDF2_ITERATIONS = 10000;
     private static final int NUM_BYTES_FOR_SALT = 32;

     private SecurityUtils () {
     }
     
     public static byte[] decode (String key, String value, String iv)
          throws Exception {

          //TODO removed FIPS class - implementation in progress
          String decryptedString = "";
          //String decryptedString = .decryptAES(key, iv, EncryptionUtils.hexStringToByteArray(value));
          // If decryption failed, we mimic what used to happen when we used javax.crypto.Cipher and decryption 
          // failed; we thrown a similar exception.
          if (decryptedString == null || decryptedString.length () == 0) {
               throw new javax.crypto.BadPaddingException ("Decryption failed");
          } 
         return decryptedString.getBytes();
     }
     
     public static String encodeKeyAsHexString (SecretKey key) {
          return new String(Hex.encodeHex(key.getEncoded()));
     }
     
     public static byte[] encrypt (String key, String value, String iv)
          throws Exception {
          //TODO removed FIPS class - implementation in progress
          return null;
     }

     public static byte[] encrypt (String key, byte[] value, String iv)
             throws Exception {
          //TODO removed FIPS class - implementation in progress
          return null;
     }
     
     public static byte[] generateIV (int numBytes) {
          byte[] iv = new byte[numBytes];
          new SecureRandom().nextBytes (iv);
          return iv;
     }

     public static SecretKey generateKey (String password, String salt) throws Exception{
    	 return generateKey(password, salt, SecurityUtils.PBKDF2_ITERATIONS, SecurityUtils.KEY_SIZE_AES256);
     }
     
     public static SecretKey generateKey (String password, String salt, int iterations, int keyLength) throws Exception {
    	 SecretKeyFactory pbkdf2Factory;
    	 if (Build.VERSION.SDK_INT >= 19) {//Build.VERSION_CODES.KITKAT) {
    	    // Use compatibility key factory required for backwards compatibility in API 19 and up.
    	    pbkdf2Factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1And8bit"); //$NON-NLS-1$
    	 } else {
    	    // Traditional key factory.
    	    pbkdf2Factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1"); //$NON-NLS-1$
    	 }
    	 
    	 PBEKeySpec keySpec = new PBEKeySpec(password.toCharArray(), salt.getBytes("UTF-8"), iterations, keyLength * BYTES_TO_BITS); //$NON-NLS-1$
    	 return pbkdf2Factory.generateSecret(keySpec);
     }
     
     //Called if the user requests to generate the key locally, we gen all 256 bits,
     //so no need to use PBKDF2 like the method above
     public static byte[] generateLocalKey(int numBytes){
          byte[] randBytes = new byte[numBytes];
          new SecureRandom().nextBytes (randBytes);
          return randBytes;
     }

     /**
      * Generate a hexadecimal-encoded random byte string locally with the given byte length.
      * @param byteLength the length of the string to be generated, in byte size
      * @return hexadecimal-encoded string of the generated random string
      */
     public static String getRandomString(int byteLength) {
          byte[] randomByteArray = generateLocalKey(byteLength);
          return new String(Hex.encodeHex(randomByteArray));
     }
}
