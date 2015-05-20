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

/**
 * This class provides methods for saving the encrypted DPK key into the Android Preference
 * local storage.
 */

package com.cloudant.sync.sqlite.android.encryption;

import com.cloudant.sync.sqlite.android.encryption.jackson.JacksonSerializedJSONObject;
import com.cloudant.sync.sqlite.android.encryption.jackson.JsonOrgModule;

import org.json.JSONException;
import org.json.JSONObject;

// TODO: in the future, do a better job of hiding the fact that JSON is being
// used under the covers (i.e., don't throw JSON exceptions, etc.).

public class DPKBean {
     private static final String KEY_DPK = "dpk"; //$NON-NLS-1$
     private static final String KEY_ITERATIONS = "iterations"; //$NON-NLS-1$
     private static final String KEY_IV = "iv"; //$NON-NLS-1$
     private static final String KEY_SALT = "jsonSalt"; //$NON-NLS-1$
     private static final String KEY_VERSION = "version"; //$NON-NLS-1$
     private static final String VERSION_NUM = "1.0"; //$NON-NLS-1$
     
     private JSONObject obj;

     
     protected DPKBean (String json) throws JSONException {
          try {
               //TODO implement deserialize
               this.obj = JsonOrgModule.deserializeJSONObject(json);

          }
          
          catch (Throwable e) {
               throw new JSONException (e.toString());
          }
     }
     
     protected DPKBean (String encryptedDPK, String iv, String salt,
          int iterations) throws JSONException {
          this.obj = new JacksonSerializedJSONObject();
          
          // Fill in the DPK object fields.

          if(encryptedDPK != null) {
             this.obj.put(DPKBean.KEY_DPK, encryptedDPK);
          } else {
              this.obj.put(DPKBean.KEY_DPK, "");
          }
          this.obj.put (DPKBean.KEY_ITERATIONS, iterations);
          this.obj.put (DPKBean.KEY_IV, iv);
          this.obj.put (DPKBean.KEY_SALT, salt);

          //this.obj.put (DPKBean.KEY_VERSION, DPKBean.VERSION_NUM);
     }

    public String getEncryptedDPK () throws JSONException {
          return this.obj.getString (DPKBean.KEY_DPK);
     }
     
     public int getIterations () throws JSONException {
          return this.obj.getInt (DPKBean.KEY_ITERATIONS);
     }
     
     public String getIV () throws JSONException {
          return this.obj.getString (DPKBean.KEY_IV);
     }
     
     public String getSalt () throws JSONException {
          return this.obj.getString (DPKBean.KEY_SALT);
     }
     
     public String getVersion () throws JSONException {
          return this.obj.getString (DPKBean.KEY_VERSION);
     }
     
     public String toString () {
          return this.obj.toString();
     }
}
