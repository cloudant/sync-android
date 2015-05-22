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

import android.content.Context;
import android.util.Base64;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.io.FileUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class SecurityManager {
     private static final int IV_NUM_BYTES = 16;
     private static final int LOCAL_KEY_NUM_BYTES = 32;

     private static SecurityManager instance;

     private Keychain keychain;

     private  Context context;

     private SecurityManager (Context context) {
          this.context = context;
          this.keychain = new Keychain (context);
     }

     private SecurityManager() {
          this.keychain = new Keychain (this.context);
     }

     /*private SecurityManager (Context context, String identifier) {
          this.keychain = new Keychain (context, identifier);
     }*/

     public static synchronized SecurityManager getInstance (Context context) {
          if (SecurityManager.instance == null) {
               SecurityManager.instance = new SecurityManager(context);
          }

          return SecurityManager.instance;
     }

     public static synchronized SecurityManager getInstance () throws Exception {
          if (SecurityManager.instance == null) {
               //SecurityManager.instance = new SecurityManager();
               throw new Exception("Instance does not exist.  First initialize SecurityManager with context arg.");
          }

          return SecurityManager.instance;
     }

     public String getDPK (String password, String identifier) throws Exception {
          String decodedDPK;
          DPKBean dpkBean = this.keychain.getDPKBean (identifier);
          String pwKey = SecurityUtils.encodeKeyAsHexString (SecurityUtils
               .generateKey (password, dpkBean.getSalt ()));

          // The DPK is base-64 encoded, so decode it before decrypting.

          decodedDPK = new String (Base64.decode (dpkBean.getEncryptedDPK (),
               Base64.DEFAULT));

          return new String (SecurityUtils.decode (pwKey, decodedDPK,
               dpkBean.getIV ()));
     }

     public String getSalt (String identifier) throws Exception {
          DPKBean dpkBean = this.keychain.getDPKBean (identifier);

          if (dpkBean == null) {
               return null;
          }

          return dpkBean.getSalt();
     }

     public boolean isDPKAvailable (String identifier) {
          return this.keychain.isDPKAvailable(identifier);
     }

     public void destroyKeychain () {
          this.keychain.destroy ();
     }

     //Store with DPK with a unique identfier - this allows for multiple databases
     //The identifier will allow for multiple keys to be saved in the Shared Preferences
     //TODO possibly add check to make sure DPK is saved
     //TODO isUpdate boolean - use to update password in later FB ticket
     public void storeDPK (String password, String identifier, boolean isUpdate)
             throws Exception {
          String dpk;

          DPKBean dpkBean;
          String encryptedDPK;
          String iv;
          String pwKey;

          //Number of bytes for salt is 32
          String salt = SecurityUtils.getRandomString(32);

          dpk = encodeBytesAsHexString (SecurityUtils
                  .generateLocalKey(SecurityManager.LOCAL_KEY_NUM_BYTES));

          iv = encodeBytesAsHexString(SecurityUtils
                  .generateIV(SecurityManager.IV_NUM_BYTES));
          pwKey = SecurityUtils.encodeKeyAsHexString (SecurityUtils
                  .generateKey (password, salt));

          // Encrypt the DPK and store everything in a bean that can be stored
          // in the keychain.

          encryptedDPK = Base64.encodeToString (
                  encodeBytesAsHexString (SecurityUtils.encrypt (pwKey, dpk, iv))
                  .getBytes (), Base64.DEFAULT);

          dpkBean = new DPKBean (encryptedDPK, iv, salt,
                  SecurityUtils.PBKDF2_ITERATIONS);

          // Finally, save everything in the keychain.

          this.keychain.setDPKBean (identifier, dpkBean);
     }

     public void encryptAttachment (String password, String identifier, File attachmentFile, byte[] sourceByteArray)
             throws Exception {
          //String dpk;

          DPKBean dpkBean;
          byte[] encryptedAttachmentByteArray;
          String encryptedAttachmentIv;
          String pwKey;
          String encryptedAttachmentDPK;
          String attachmentDpk;

          //Grab sqlcipher DPK that will be used for encrypting attachments
          DPKBean sqlcipherDpkBean = this.keychain.getDPKBean(identifier);

          //TODO use decrypted DPK
          String decryptedDPK = getDPK(password, null);

          //Grab existing dpk bean from Android keychain storage and add random salt
          //dpkBean = this.keychain.getDPKBean(identifier);

          attachmentDpk = encodeBytesAsHexString (SecurityUtils
                  .generateLocalKey(SecurityManager.LOCAL_KEY_NUM_BYTES));

          /*pwKey = SecurityUtils.encodeKeyAsHexString(SecurityUtils
                  .generateKey(password, sqlcipherDpkBean.getSalt()));*/

          //Create a new iv for attachment
          encryptedAttachmentIv = encodeBytesAsHexString(SecurityUtils
                  .generateIV(SecurityManager.IV_NUM_BYTES));

          encryptedAttachmentByteArray = Base64.encode(
                  encodeBytesAsHexString(SecurityUtils.encrypt(decryptedDPK, sourceByteArray,
                          encryptedAttachmentIv))
                  .getBytes(), Base64.DEFAULT);


          /*dpkBean = new DPKBean (encryptedDPK, iv, salt,
                  SecurityUtils.PBKDF2_ITERATIONS);
                  */

          // Save everything to a combined byte array starting with IV

          //Add IV to header of file - needs review
          byte[] IvAndEncryptFileByteArray = null;

          byte[] ivByteArray = hexStringToBytes(encryptedAttachmentIv);

          System.arraycopy(ivByteArray,0,IvAndEncryptFileByteArray,0,ivByteArray.length);
          System.arraycopy(encryptedAttachmentByteArray,0,IvAndEncryptFileByteArray,ivByteArray.length
                  ,encryptedAttachmentByteArray.length);



          //TODO check performance on large files
          FileUtils.writeByteArrayToFile(attachmentFile, encryptedAttachmentByteArray);
     }

     public InputStream decryptAttachmentFileStream (String password, String attachmentFileName, File encryptedAttachmentFile)
             throws Exception {

          byte[] decryptedAttachmentByteArray;
          String AttachmentIv;
          String pwKey;

          //Use getDPK to get the IV and salt to decrypt the file attachment
          String decodedDPK;
          //Identifier is the attachment file name
          DPKBean dpkBean = this.keychain.getDPKBean (attachmentFileName);
          pwKey = SecurityUtils.encodeKeyAsHexString (SecurityUtils
                  .generateKey (password, dpkBean.getSalt ()));

          // The DPK is base-64 encoded, so decode it before decrypting.

          decodedDPK = new String (Base64.decode (dpkBean.getEncryptedDPK (),
                  Base64.DEFAULT));

          //TODO take IV from encrypted file
          byte[] decryptedFileByteArray = SecurityUtils.decode (pwKey, decodedDPK,
                  dpkBean.getIV ());

         // return EncryptionInputStreamUtils.decryptInputStream()

          //return new ByteArrayInputStream(decryptedString.getBytes(StandardCharsets.UTF_8));
          return null;
     }

     //TODO
     public InputStream decryptAttachmentGzipFileStream (String password, String identifier, File encryptedAttachmentFile)
             throws Exception {
          //String dpk;

          DPKBean dpkBean;
          String pwKey;

          //Use getDPK to get the IV and salt to decrypt the file attachment
          String decodedDPK;
          //Identifier is the attachment file name
          dpkBean = this.keychain.getDPKBean (identifier);
          pwKey = SecurityUtils.encodeKeyAsHexString(SecurityUtils
                  .generateKey(password, dpkBean.getSalt()));

          // The DPK is base-64 encoded, so decode it before decrypting.

          decodedDPK = new String (Base64.decode (dpkBean.getEncryptedDPK (),
                  Base64.DEFAULT));

          String decryptedString = new String (SecurityUtils.decode (pwKey, decodedDPK,
                  dpkBean.getIV ()));

          return new ByteArrayInputStream(decryptedString.getBytes(StandardCharsets.UTF_8));
     }

     /*public static String encodeBytesAsHexString (byte bytes[]) {
          StringBuilder result = new StringBuilder();

          if (bytes != null) {
               for (byte curByte : bytes) {
                    result.append (String.format ("%02X", curByte)); //$NON-NLS-1$
               }
          }

          return result.toString();
     }*/

     private String encodeBytesAsHexString(byte[] bytes) {
          return new String(Hex.encodeHexString(bytes));
     }

     private byte[] hexStringToBytes(String hexString) {
          return StringUtils.getBytesUnchecked(hexString, String.valueOf(StandardCharsets.UTF_8));
     }

}