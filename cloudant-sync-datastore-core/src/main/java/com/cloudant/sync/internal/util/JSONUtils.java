/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.internal.util;

import com.cloudant.sync.internal.common.CouchConstants;
import com.cloudant.sync.internal.common.PropertyFilterMixIn;
import com.cloudant.sync.internal.mazha.CouchClient;
import com.cloudant.sync.internal.mazha.Document;
import com.cloudant.sync.internal.mazha.DocumentRevs;
import com.cloudant.sync.internal.mazha.OpenRevision;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Internal utility class
 * @api_private
 */
public class JSONUtils {

    private static final String LOG_TAG = "JSONUtils";


    private static final byte[] EMPTY_JSON = "{}".getBytes(Charset.forName("UTF-8"));


    public final static TypeReference<List<String>> STRING_LIST_TYPE_DEF =
            new TypeReference<List<String>>() {};

    public final static TypeReference<Map<String, Object>> STRING_MAP_TYPE_DEF =
            new TypeReference<Map<String, Object>>() {};


    private static final ObjectMapper sMapper = new ObjectMapper();

    static {
        sMapper.registerModule(new JacksonModule());
        sMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        // Should only disable this in production, cause we do want to see them in development?
        sMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static byte[] emptyJSONObjectAsBytes(){
        return Arrays.copyOf(EMPTY_JSON, EMPTY_JSON.length);
    }

    // These reserved string is used to construct a filter. The filter is used by Jackson to
    // ignore these field when serialized a generic object before put the serialized result
    // into SQLite as a BLOB.
    private static final FilterProvider sCouchWordsFilter =
            new SimpleFilterProvider().addFilter(PropertyFilterMixIn.SIMPLE_FILTER_NAME,
                    SimpleBeanPropertyFilter.serializeAllExcept(
                            CouchConstants._id,
                            CouchConstants._rev,
                            CouchConstants._deleted));

    private static final FilterProvider sEmptyProvider =
            new SimpleFilterProvider().addFilter("simpleFilter",
                    SimpleBeanPropertyFilter.serializeAllExcept()).setFailOnUnknownId(false);


    private static FilterProvider getFilterProvider(boolean usingFilter) {
        return usingFilter ? sCouchWordsFilter : sEmptyProvider;
    }

    private static ObjectMapper getsMapper() {
        return sMapper;
    }

    private static ObjectWriter getWriter(boolean usingFilter) {
        return sMapper.writer(getFilterProvider(usingFilter));
    }

    public static boolean isValidJSON(Map object) {
        try {
            getWriter(false).writeValueAsString(object);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean isValidJSON(final String json) {
        try {
            getsMapper().readValue(json, Map.class);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean isValidJSON(final byte[] json) {
        try {
            getsMapper().readValue(json, Map.class);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static byte[] serializeAsBytes(Map object) {
        return serializeAsBytes(object, true);
    }

    public static byte[] serializeAsBytes(Document document) {
        return serializeAsBytes(document, true);
    }

    static byte[] serializeAsBytes(Object object, boolean usingFilter) {
        try {
             return getWriter(usingFilter).writeValueAsBytes(object);
        } catch (Exception e) {
            throw new IllegalStateException("Error converting object to byte[]: " + object);
        }
    }

    public static String serializeAsString(Map object) {
        return serializeAsString(object, true);
    }

    public static String serializeAsString(Document document) {
        return serializeAsString(document, true);
    }

    static String serializeAsString(Object object, boolean usingFilter) {
        try {
            return getWriter(usingFilter).writeValueAsString(object);
        } catch (Exception e) {
            throw new IllegalStateException("Error converting object to String: " + object);
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> deserialize(byte[] json) {
        try {
            return getsMapper().readValue(json, Map.class);
        } catch (RuntimeException e){
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Error converting byte[] to map object: " +
                    bytesToString(json));
        }
    }

    public static <T> T deserialize(byte[] json, Class<T> clazz) {
        try {
            return getsMapper().readValue(json, clazz);
        } catch (Exception e) {
            throw new IllegalStateException("Error converting byte[] to object: " +
                    bytesToString(json));
        }
    }

    public static String toPrettyJson(Object object) {
        try {
            return sMapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String bytesToString(byte[] bytes) {
        // TODO: this is using system default charset, how to handle all unicode?
        // TODO: what is the correct way to deal with bytes == null?
        if (bytes != null) {
            return new String(bytes, Charset.forName("UTF-8"));
        } else {
            return "";
        }
    }

    public static <T> T fromJson(Reader reader, TypeReference<T> typeRef) {
        try {
            return sMapper.readValue(reader, typeRef);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T fromJson(Reader reader, JavaType type) {
        try {
            return sMapper.readValue(reader, type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T fromJson(Reader reader, Class<T> clazz) {
        try {
            return sMapper.readValue(reader, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Object> fromJson(Reader reader) {
        return fromJson(reader, STRING_MAP_TYPE_DEF);
    }

    public static <T> T fromJsonToList(Reader reader, TypeReference<T> typeReference) {
        return fromJson(reader, typeReference);
    }

    public static String toJson(Object object) {
        try {
            return getWriter(false).writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static TypeFactory getTypeFactory(){
        return sMapper.getTypeFactory();
    }

    public static JavaType mapStringToObject(){
        return JSONUtils.getTypeFactory().constructParametricType(Map.class,String.class,Object.class);
    }

    public static JavaType mapStringMissingRevisions(){
        return JSONUtils.getTypeFactory().constructParametricType(Map.class, String.class, CouchClient
                .MissingRevisions.class);
    }

    public static JavaType openRevisionList() {
        return JSONUtils.getTypeFactory().constructParametricType(List.class, OpenRevision.class);
    }

    public static JavaType documentRevs() {
        return JSONUtils.getTypeFactory().constructType(DocumentRevs.class);
    }
}
