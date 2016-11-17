/*
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

package com.cloudant.sync.internal.mazha.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;

/**
 * @api_private
 */
public class JSONHelper {

    private final ObjectMapper objectMapper;

    public final static TypeReference<List<String>> STRING_LIST_TYPE_DEF =
            new TypeReference<List<String>>() {};

    public final static TypeReference<Map<String, Object>> STRING_MAP_TYPE_DEF =
            new TypeReference<Map<String, Object>>() {};

    public JSONHelper() {
        objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        // Should only disable this in production, cause we do want to see them in development?
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public <T> T fromJson(Reader reader, TypeReference<T> typeRef) {
        try {
            return objectMapper.readValue(reader, typeRef);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T fromJson(Reader reader, JavaType type) {
        try {
            return objectMapper.readValue(reader, type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T fromJson(Reader reader, Class<T> clazz) {
        try {
            return objectMapper.readValue(reader, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, Object> fromJson(Reader reader) {
        return fromJson(reader, STRING_MAP_TYPE_DEF);
    }

    public <T> T fromJsonToList(Reader reader, TypeReference<T> typeReference) {
        return fromJson(reader, typeReference);
    }

    public String toJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public String toPrettyJson(Object object) {
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public TypeFactory getTypeFactory(){
        return objectMapper.getTypeFactory();
    }

    public JavaType mapStringToObject(){
        return this.getTypeFactory().constructParametricType(Map.class,String.class,Object.class);
    }

}
