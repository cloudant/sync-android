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


import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Iterator;

public class JsonOrgJSONObjectSerializer extends JsonSerializer<JSONObject> //extends SerializerBase<JSONObject>
{
     public final static JsonOrgJSONObjectSerializer instance =
          new JsonOrgJSONObjectSerializer();

     public JsonOrgJSONObjectSerializer () {

          //super (JSONObject.class);
     }

     /*@Override
     public JsonNode getSchema (SerializerProvider provider, Type typeHint)
          throws JsonMappingException {
     //     return createSchemaNode ("object", true); //$NON-NLS-1$
     }*/
     
     @Override
     public void serialize(JSONObject value, JsonGenerator jgen,
          SerializerProvider provider) throws IOException,
             JsonGenerationException {
          jgen.writeStartObject();
          
          serializeContents (value, jgen, provider);
          
          jgen.writeEndObject();
     }
     
     protected void serializeContents(JSONObject value, JsonGenerator jgen,
          SerializerProvider provider) throws IOException,
          JsonGenerationException {
          Iterator<?> i  = value.keys();
          
          while (i.hasNext()) {
               String key = (String) i.next();
               Class<?> cls;
               Object obj;
               
               try {
                    obj = value.get (key);
               }
               
               catch (JSONException e) {
                    throw new JsonGenerationException (e);
               }
               
               if ((obj == null) || (obj == JSONObject.NULL)) {
                    jgen.writeNullField (key);
                    
                    continue;
               }
               
               jgen.writeFieldName (key);
               
               cls = obj.getClass();
               
               if ((cls == JSONObject.class) ||
                    JSONObject.class.isAssignableFrom (cls)) {
                    serialize ((JSONObject) obj, jgen, provider);
               }
               
               else if ((cls == JSONArray.class) ||
                    JSONArray.class.isAssignableFrom (cls)) {
                    JsonOrgJSONArraySerializer.instance.serialize
                         ((JSONArray) obj, jgen, provider);
               }
               
               else  if (cls == String.class) {
                    jgen.writeString ((String) obj);
               }
               
               else  if (cls == Integer.class) {
                    jgen.writeNumber (((Integer) obj).intValue());
               }
               
               else  if (cls == Long.class) {
                    jgen.writeNumber (((Long) obj).longValue());
               }
               
               else  if (cls == Boolean.class) {
                    jgen.writeBoolean (((Boolean) obj).booleanValue());
               }
               
               else  if (cls == Double.class) {
                    jgen.writeNumber(((Double) obj).doubleValue());
               }
               
               else {
                    provider.defaultSerializeValue (obj, jgen);
               }
          }
     }
     
     @Override
     public void serializeWithType (JSONObject value, JsonGenerator jgen,
          SerializerProvider provider, TypeSerializer typeSer)
          throws IOException, JsonGenerationException {
          typeSer.writeTypePrefixForObject (value, jgen);
          
          serializeContents (value, jgen, provider);
          
          typeSer.writeTypeSuffixForObject (value, jgen);
     }
}
