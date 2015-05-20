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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

public class JsonOrgJSONObjectDeserializer extends StdDeserializer<JSONObject> {
     protected static final JsonOrgJSONObjectDeserializer instance =
          new JsonOrgJSONObjectDeserializer();
     
     protected JsonOrgJSONObjectDeserializer () {
          super (JSONObject.class);
     }
     
     @Override
     public JSONObject deserialize (JsonParser parser,
          DeserializationContext context) throws IOException,
          JsonProcessingException {
          JSONObject result = new JacksonSerializedJSONObject();
          JsonToken token = parser.getCurrentToken();
          
          if (token == JsonToken.START_OBJECT) {
               token = parser.nextToken();
          }
          
          try {
               while (token != JsonToken.END_OBJECT) {
                    String name;

                    if (token != JsonToken.FIELD_NAME) {
                         throw context.wrongTokenException (parser,
                              JsonToken.FIELD_NAME, ""); //$NON-NLS-1$
                    }

                    name = parser.getCurrentName();
                    token = parser.nextToken();

                    switch (token) {
                         case START_ARRAY: {
                              result.put (name,
                                   JsonOrgJSONArrayDeserializer.instance.deserialize
                                        (parser, context));

                              break;
                         }

                         case START_OBJECT: {
                              result.put (name, deserialize (parser, context));

                              break;
                         }

                         case VALUE_EMBEDDED_OBJECT: {
                              result.put (name, parser.getEmbeddedObject());

                              break;
                         }

                         case VALUE_FALSE: {
                              result.put (name, Boolean.FALSE);

                              break;
                         }

                         case VALUE_NULL: {
                              result.put (name, JSONObject.NULL);

                              break;
                         }

                         case VALUE_NUMBER_FLOAT: {
                              result.put (name, parser.getNumberValue());

                              break;
                         }

                         case VALUE_NUMBER_INT: {
                              result.put (name, parser.getNumberValue());

                              break;
                         }

                         case VALUE_STRING: {
                              result.put (name, parser.getText());

                              break;
                         }

                         case VALUE_TRUE: {
                              result.put (name, Boolean.TRUE);

                              break;
                         }

                         case END_ARRAY: case END_OBJECT: case FIELD_NAME:
                         case NOT_AVAILABLE: {
                              break;
                         }
                    }
                    
                    token = parser.nextToken();
               }
          }
          
          catch (JSONException e) {
               throw context.mappingException (e.getMessage());
          }
          
          return result;
     }
}
