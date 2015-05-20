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

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.StringWriter;

public class JsonOrgModule extends SimpleModule {
     private static final ObjectMapper mapper = new ObjectMapper();
     
     static {
          // Register the Jackson -> json.org serializers and deserializers.
          
          JsonOrgModule.mapper.registerModule (new JsonOrgModule());
     }
     
     private JsonOrgModule () {
          super ("JsonOrgModule", new Version(1, 0, 0, null)); //$NON-NLS-1$

          //Key storage and management only requires JSON object serializer and deserializer

          //addDeserializer (JSONArray.class,
           //    JsonOrgJSONArrayDeserializer.instance);
          addDeserializer (JSONObject.class,
               JsonOrgJSONObjectDeserializer.instance);
          
          //addSerializer (JSONArray.class, JsonOrgJSONArraySerializer.instance);
          addSerializer (JSONObject.class, JsonOrgJSONObjectSerializer.instance);
     }
     
     public static JSONArray deserializeJSONArray (String json)
          throws Throwable {
          return JsonOrgModule.mapper.readValue (json, JSONArray.class);
     }
     
     public static JSONObject deserializeJSONObject (String json)
          throws Throwable {
          return JsonOrgModule.mapper.readValue (json, JSONObject.class);
     }
     
     public static String serialize (JSONArray array) {
          try {
               StringWriter writer = new StringWriter();
               
               JsonOrgModule.mapper.writeValue (writer, array);
               
               writer.close();
               
               return writer.toString();
          }
          
          catch (Throwable e) {
               return null;
          }
     }
     
     public static String serialize (JSONObject obj) {
          try {
               StringWriter writer = new StringWriter();
               
               JsonOrgModule.mapper.writeValue (writer, obj);
               
               writer.close();
               
               return writer.toString();
          }
          
          catch (Throwable e) {
               return null;
          }
     }
}
