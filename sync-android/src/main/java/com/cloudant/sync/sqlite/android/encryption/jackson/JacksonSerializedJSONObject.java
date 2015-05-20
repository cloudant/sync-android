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
package com.cloudant.sync.sqlite.android.encryption.jackson;

import org.json.JSONException;
import org.json.JSONObject;

public class JacksonSerializedJSONObject extends JSONObject {
     private JSONObject wrappedObject;
     
     public JacksonSerializedJSONObject () {
          super();
     }
     
     public JacksonSerializedJSONObject (JSONObject obj) {
          this.wrappedObject = obj;
     }
     
     @Override
     public String toString () {
          return JsonOrgModule.serialize ((this.wrappedObject == null) ? this :
               this.wrappedObject);
     }
     
     @Override
     public String toString (int indentFactor) throws JSONException {
          // We don't directly make use of this method, but if anyone does,
          // just use the default toString() method for performance.
          
          return toString();
     }
}
