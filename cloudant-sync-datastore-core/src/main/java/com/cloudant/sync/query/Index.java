/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2015 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.query;


import com.cloudant.sync.internal.util.Misc;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * <p>
 * A class representing a query index.
 * </p>
 * <p>
 * Comprehensive documentation for the query feature can be found in the project
 * <a target="_blank" href="https://github.com/cloudant/sync-android/blob/master/doc/query.md">
 * markdown document.</a>
 * </p>
 *
 * @api_public
 */
public class Index {

    private static final Logger logger = Logger.getLogger(Index.class.getCanonicalName());

    /**
     * The json field names to index
     */
    public final List<FieldSort> fieldNames;

    /**
     * The unique name of the index. The index name is used to look up indexes for certain
     * {@link Query} methods.
     */
    public final String indexName;

    /**
     * The index type: "json" or "text".
     */
    public final IndexType indexType;

    /**
     * <p>
     * For "text" indexes. The SQLite FTS tokenizer to use when searching text.
     * </p>
     * <p>
     * For "JSON" indexes this will be null.
     * </p>
     * <p>
     * For more information about tokenizers, see
     * <a target="_blank" href="https://www.sqlite.org/fts3.html#tokenizer">the SQLite documentation.</a>
     * </p>
     */
    public final Tokenizer tokenizer;

    /**
     * This method sets the index type to the default setting of "json"
     *
     * @param fieldNames the field names in the index
     * @param indexName the index name or null
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
     * @param tokenizer for text indexes only.
     */
    public Index(List<FieldSort> fieldNames,
                 String indexName,
                 IndexType indexType,
                 Tokenizer tokenizer) {

        Misc.checkNotNull(fieldNames, "fieldNames");
        Misc.checkArgument(!fieldNames.isEmpty(), "fieldNames isEmpty()");
        // NB indexName can be null (IndexCreator will generate one if needed) but not empty
        Misc.checkArgument((indexName == null || !indexName.isEmpty()), "indexName");

        this.fieldNames = new ArrayList<FieldSort>(fieldNames);
        this.indexName = indexName;
        this.indexType = indexType;

        if (indexType == IndexType.TEXT) {
            if (tokenizer == null) {
                // set default tokenizer if one wasn't set
                this.tokenizer = Tokenizer.DEFAULT;
            } else {
                this.tokenizer = tokenizer;
            }
        } else {
            // tokenize isn't valid if we're not doing text indexing
            Misc.checkArgument(tokenizer == null, "tokenizer must be null if indexType is JSON");
            this.tokenizer = null;
        }

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
        return tokenizer == null ? index.tokenizer == null : tokenizer.equals(index.tokenizer);

    }

    @Override
    public int hashCode() {
        int result = fieldNames.hashCode();
        result = 31 * result + indexName.hashCode();
        result = 31 * result + indexType.hashCode();
        result = 31 * result + (tokenizer != null ? tokenizer.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Index{" +
                "fieldNames=" + fieldNames +
                ", indexName='" + indexName + '\'' +
                ", indexType=" + indexType +
                ", tokenizer=" + tokenizer +
                '}';
    }

}
