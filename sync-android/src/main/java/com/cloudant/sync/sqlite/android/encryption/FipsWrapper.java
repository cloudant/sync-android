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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Arrays;


public class FipsWrapper {

     // Load library
     static {
          System.loadLibrary ("openssl_fips"); //$NON-NLS-1$
     }

     // Declare native method (and make it public to expose it directly)
     private static native int _enableFips ();

     private static native int _disableFips ();

     private static native int _setFips (int mode);

     private static native int _getFipsMode ();

     private static native byte[] _encryptAES (byte[] key, int keylen,
          byte[] iv, int ivlen, String to_encrypt, int to_encryptLen);

     private static native byte[] _decryptAES (byte[] key, int keylen,
          byte[] iv, int ivlen, byte[] encryptedData, int encryptedDataLen);

     // Provide additional functionality, that &quot;extends&quot; the native
     // method
     public static int enableFips () {
          return _enableFips();
     }

     public static int disableFips () {
          return _disableFips();
     }

     public static int setFips (int mode) {
          return _setFips(mode);
     }

     public static int getFipsMode () {
          return _getFipsMode();
     }

     public static byte[] encryptAES (String key, String iv, String clearText) {
          byte[] keyByteArray = EncryptionUtils.hexStringToByteArray(key);
          byte[] ivByteArray = EncryptionUtils.hexStringToByteArray(iv);
          byte[] encBytes = _encryptAES (keyByteArray, keyByteArray.length,
               ivByteArray, ivByteArray.length, clearText, clearText.length ());
          return encBytes;
     }


     // Throws exception if can't decode
     public static String decryptAES (String key, String iv,
          byte[] encryptedBytes) {
          byte[] keyByteArray = EncryptionUtils.hexStringToByteArray(key);
          byte[] ivByteArray = EncryptionUtils.hexStringToByteArray(iv);
          byte[] decryptedBytes = _decryptAES (keyByteArray,
               keyByteArray.length, ivByteArray, ivByteArray.length,
               encryptedBytes, encryptedBytes.length);
          String decryptedText;
          try {
               CharsetDecoder charsetDecoder = Charset.forName ("UTF-8") //$NON-NLS-1$
                    .newDecoder ();
               CharBuffer charBuffer = charsetDecoder.decode (ByteBuffer.wrap (decryptedBytes));
               decryptedText = new String (decryptedBytes, "UTF-8"); // in case the default charset is not UTF-8 //$NON-NLS-1$
          }
          catch (Throwable t) {
               decryptedText = null;
          }
          Arrays.fill (decryptedBytes, (byte) 0);
          return decryptedText;
     }

     public static byte[] encryptAES(String key, String iv, byte[] value) throws IOException {
          byte[] keyByteArray = EncryptionUtils.hexStringToByteArray(key);
          //Read bytes of file then convert into String
          String fileByteArray = EncryptionUtils.byteArrayToHexString(value);

          byte[] ivByteArray = EncryptionUtils.hexStringToByteArray(iv);

          byte[] encBytes = _encryptAES(keyByteArray, keyByteArray.length,
                  ivByteArray, ivByteArray.length, fileByteArray, fileByteArray.length());

          //Return encrypted bytes of the attachment file
          return encBytes;

     }
}