//  Copyright (c) 2015 Cloudant. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.cloudant.sync.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class provides functionality to manage an index
 */
class Index {

    private static final Logger logger = Logger.getLogger(Index.class.getCanonicalName());

    private static final String TEXT_TOKENIZE = "tokenize";

    private static final String TEXT_DEFAULT_TOKENIZER = "simple";

    private static final List<String> validSettings = Arrays.asList(TEXT_TOKENIZE);

    protected final List<FieldSort> fieldNames;

    protected final String indexName;

    protected final IndexType indexType;

    protected final Map<String, String> indexSettings;

    private ObjectMapper objectMapper;

    private Index(List<FieldSort> fieldNames,
                  String indexName,
                  IndexType indexType,
                  Map<String, String> indexSettings) {
        this.fieldNames = fieldNames;
        this.indexName = indexName;
        this.indexType = indexType;
        this.indexSettings = indexSettings;
    }

    /**
     * This method sets the index type to the default setting of "json"
     *
     * @param fieldNames the field names in the index
     * @param indexName the index name or null
     * @return the Index object or null if arguments passed in were invalid.
     */
    public static Index getInstance(List<FieldSort> fieldNames, String indexName) {
        return getInstance(fieldNames, indexName, IndexType.JSON);
    }

    public static Index getInstance(List<FieldSort> fieldNames, String indexName, IndexType indexType) {
        return getInstance(fieldNames, indexName, indexType, null);
    }

    /**
     * This method handles index specific validation and ensures that the constructed
     * Index object is valid.
     *
     * @param fieldNames the field names in the index
     * @param indexName the index name or null
     * @param indexType the index type (json or text)
     * @param indexSettings the optional settings used to configure the index.
     *                      Only supported parameter is 'tokenize' for text indexes only.
     * @return the Index object or null if arguments passed in were invalid.
     */
    public static Index getInstance(List<FieldSort> fieldNames,
                              String indexName,
                              IndexType indexType,
                              Map<String, String> indexSettings) {
        if (fieldNames == null || fieldNames.isEmpty()) {
            logger.log(Level.SEVERE, "No field names were provided.");
            return null;
        }

        if(indexName != null && indexName.isEmpty()){
            return null;
        }

        if (indexType == IndexType.JSON && indexSettings != null) {
            logger.log(Level.WARNING, String.format("Index type is %s, index settings %s ignored.",
                                                    indexType,
                                                    indexSettings.toString()));
            indexSettings = null;
        } else if (indexType == IndexType.TEXT) {
            if (indexSettings == null) {
                indexSettings = new HashMap<String, String>();
                indexSettings.put(TEXT_TOKENIZE, TEXT_DEFAULT_TOKENIZER);
                logger.log(Level.FINE, String.format("Index type is %s, defaulting settings to %s.",
                        indexType,
                        indexSettings.toString()));
            } else {
                for (String parameter : indexSettings.keySet()) {
                    if (!validSettings.contains(parameter.toLowerCase())) {
                        String msg = String.format("Invalid parameter %s in index settings %s.",
                                                   parameter,
                                                   indexSettings);
                        logger.log(Level.SEVERE, msg);
                        return null;
                    }
                }
            }
        }

        return new Index(fieldNames, indexName, indexType, indexSettings);
    }

    /**
     * Compares the index type and accompanying settings with the passed in arguments.
     *
     * @param indexType the index type to compare to
     * @param indexSettings the indexSettings to compare to
     * @return true/false - whether there is a match
     */
    protected boolean compareIndexTypeTo(IndexType indexType, String indexSettings) {
        if (this.indexType != indexType) {
            return false;
        }

        if (this.indexSettings == null && indexSettings == null) {
            return true;
        } else if (this.indexSettings == null || indexSettings == null) {
            return false;
        }

        Map<String, Object> settings;
        try {
            settings = getObjectMapper().readValue(indexSettings,
                    new TypeReference<Map<String, Object>>() {
                    });
        } catch (IOException e) {
            String msg = String.format("Error processing index settings %s",
                                       this.indexSettings.toString());
            logger.log(Level.SEVERE, msg, e);
            return false;
        }

        // We perform a deep comparison of hash maps to ensure that both objects
        // and any sub-objects are equal regardless of order within the maps.
        return this.indexSettings.equals(settings);
    }

    /**
     * Converts the index settings to a JSON string
     *
     * @return the JSON representation of the index settings
     */
    protected String settingsAsJSON() {
        String json = null;
        if (indexSettings != null) {
            try {
                json = getObjectMapper().writeValueAsString(indexSettings);
            } catch (JsonProcessingException e) {
                String msg = String.format("Error processing index settings %s",
                                           this.indexSettings.toString());
                logger.log(Level.SEVERE, msg, e);
            }
        }
        return json;
    }

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }

}
