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
import android.content.SharedPreferences;

import org.json.JSONException;



public class Keychain {

     private static final String PREF_NAME_DPK = "dpk"; //$NON-NLS-1$
     private static final String PREFS_NAME_DPK = "dpkPrefs"; //$NON-NLS-1$
     private static final String DEFAULT_IDENTIFIER = "jsonstore";
     private SharedPreferences prefs;
     
     protected Keychain (Context context) {
          this.prefs = context.getSharedPreferences (Keychain.PREFS_NAME_DPK,
               Context.MODE_PRIVATE);
     }

     /*protected Keychain (Context context, String identifier) {
          this.prefs = context.getSharedPreferences (Keychain.PREFS_NAME_DPK,
                  Context.MODE_PRIVATE);
     }*/
     
     public DPKBean getDPKBean (String identifier) throws JSONException {
          String dpkJSON = this.prefs.getString (buildTag(identifier), null);
          
          if (dpkJSON == null) {
               return null;
          }
          
          return new DPKBean (dpkJSON);
     }
     
     public boolean isDPKAvailable (String identifier) {
          return (this.prefs.getString (buildTag(identifier), null) != null);
     }
     
     public void setDPKBean (String identifier, DPKBean dpkBean) {
          SharedPreferences.Editor editor = this.prefs.edit();
          
          editor.putString (buildTag(identifier), dpkBean.toString());
          
          editor.commit();
     }
     
     public void destroy () {
          
          SharedPreferences.Editor editor = this.prefs.edit();
          
          editor.clear();
                    
          editor.commit();
     }
     
     //Builds tags like: dpk-[identifier]
     private String buildTag (String tag) {
          if (tag.equals (DEFAULT_IDENTIFIER)) {
               //Use pre-2.0 jsonstore dpk keychain key
               return Keychain.PREF_NAME_DPK;
          }
          return Keychain.PREF_NAME_DPK + "-" + tag; //$NON-NLS-1$
     }
}
