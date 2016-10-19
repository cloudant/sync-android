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


import com.cloudant.sync.util.Misc;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class provides functionality to manage an index
 */
class Index {

    private static final Logger logger = Logger.getLogger(Index.class.getCanonicalName());

    private static final String TEXT_DEFAULT_TOKENIZER = "simple";

    public final List<FieldSort> fieldNames;

    public final String indexName;

    public final IndexType indexType;

    public final String tokenize;

    /**
     * This method sets the index type to the default setting of "json"
     *
     * @param fieldNames the field names in the index
     * @param indexName the index name or null
     * @return the Index object or null if arguments passed in were invalid.
     */
    public Index (List<FieldSort> fieldNames, String indexName) {
        this(fieldNames, indexName, IndexType.JSON);
    }

    /**
     * This method handles index specific validation and ensures that the constructed
     * Index object is valid.
     *
     * @param fieldNames the field names in the index
     * @param indexName the index name or null
     * @param indexType the index type (json or text)
     * @return the Index object or null if arguments passed in were invalid.
     */
    public Index (List<FieldSort> fieldNames, String indexName, IndexType indexType) {
        this(fieldNames, indexName, indexType, null);
    }

    /**
     * This method handles index specific validation and ensures that the constructed
     * Index object is valid.
     *
     * @param fieldNames the field names in the index
     * @param indexName the index name or null
     * @param indexType the index type (json or text)
     * @param tokenize  for text indexes only.
     * @return the Index object or null if arguments passed in were invalid.
     */
    public Index(List<FieldSort> fieldNames,
                 String indexName,
                 IndexType indexType,
                 String tokenize) {

        Misc.checkNotNull(fieldNames, "fieldNames");
        Misc.checkArgument(!fieldNames.isEmpty(), "fieldNames isEmpty()");
        // NB indexName can be null (IndexCreator will generate one if needed) but not empty
        Misc.checkArgument((indexName == null || !indexName.isEmpty()), "indexName");

        this.fieldNames = new ArrayList<FieldSort>(fieldNames);
        this.indexName = indexName;
        this.indexType = indexType;

        if (indexType == IndexType.TEXT) {
            if (tokenize == null) {
                // set default tokenizer if one wasn't set
                this.tokenize = TEXT_DEFAULT_TOKENIZER;
            } else {
                this.tokenize = tokenize;
            }
        } else {
            // tokenize isn't valid if we're not doing text indexing
            Misc.checkArgument(tokenize == null, "tokenize must not be null if indexType is JSON");
            this.tokenize = tokenize;
        }

    }

    /**
     * Converts the index settings to a JSON string
     *
     * @return the JSON representation of the index settings
     */
    protected String settingsAsJSON() {
        // this is a trivial enough operation that we don't need a JSON serializer
        if (tokenize == null) {
            return "{}";
        }
        return "{\"tokenize\":\""+tokenize+"\"}";
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Index index = (Index) o;

        // field order shouldn't matter
        if (!(fieldNames.size() == index.fieldNames.size() && fieldNames.containsAll(index.fieldNames))) {
            return false;
        }
        if (!indexName.equals(index.indexName)) {
            return false;
        }
        if (indexType != index.indexType) {
            return false;
        }
        return tokenize == null ? index.tokenize == null : tokenize.equals(index.tokenize);

    }

    @Override
    public int hashCode() {
        int result = fieldNames.hashCode();
        result = 31 * result + indexName.hashCode();
        result = 31 * result + indexType.hashCode();
        result = 31 * result + (tokenize != null ? tokenize.hashCode() : 0);
        return result;
    }
}
