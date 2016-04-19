/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.util;

import com.cloudant.common.CouchConstants;
import com.cloudant.common.PropertyFilterMixIn;
import com.cloudant.mazha.Document;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;

/**
 * Internal utility class
 * @api_private
 */
public class JSONUtils {

    private static final String LOG_TAG = "JSONUtils";


    private static final byte[] EMPTY_JSON = "{}".getBytes(Charset.forName("UTF-8"));

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
                    SimpleBeanPropertyFilter.serializeAllExcept());

    private static final ObjectMapper sMapper = new ObjectMapper();

    static {
        sMapper.registerModule(new JacksonModule());
    }

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

    public static Map<String, Object> deserialize(byte[] json) {
        try {
            return getsMapper().readValue(json, Map.class);
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

    public static String toPrettyJson(Object rev) {
        try {
            return getsMapper().writerWithDefaultPrettyPrinter().writeValueAsString(rev);
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
}
