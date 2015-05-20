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

import org.json.JSONArray;
import org.json.JSONException;

public class JacksonSerializedJSONArray extends JSONArray {
     private JSONArray wrappedArray;
     
     public JacksonSerializedJSONArray () {
          super();
     }
     
     public JacksonSerializedJSONArray (JSONArray array) {
          this.wrappedArray = array;
     }
     
     @Override
     public String toString () {
          return JsonOrgModule.serialize ((this.wrappedArray == null) ? this :
               this.wrappedArray);
     }
     
     @Override
     public String toString (int indentFactor) throws JSONException {
          // We don't directly make use of this method, but if anyone does,
          // just use the default toString() method for performance.
          
          return toString();
     }
}
