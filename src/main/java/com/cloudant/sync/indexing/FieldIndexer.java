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

package com.cloudant.sync.indexing;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * FieldIndexer is a simple {@link Indexer} which extracts
 * values from a document based on the fieldName given to it.
 * </p>
 * <p>
 * It will return the value from the first level of the document
 * in the field specified verbatim. For example, for the document:
 * </p>
 * <pre>
 *     {
 *         "hello": "world",
 *         "foo": ["bar", "baz"]
 *     }
 * </pre>
 *
 * <p>
 * If the the constructor is passed "hello" as a field name,
 * {@code indexedValues} will return ["world"]. For "foo", it
 * will return ["bar", "baz"].
 */
public class FieldIndexer implements Indexer<Object> {

    private String fieldName;

    /**
     * Creates a {@code FieldIndexer} object that will return the
     * value for {@code fieldName}.
     */
    public FieldIndexer(String fieldName) {
        this.fieldName = fieldName;
    }

    /**
     * Returns the values to index for {@code map}, using the instance's
     * {@code fieldName}.
     */
    @Override
    public List<Object> indexedValues(String indexName, Map map) {
        if (map.containsKey(this.fieldName)) {
            Object value = map.get(this.fieldName);
            if(value instanceof List) {
                return (List)value;
            } else {
                return Arrays.asList(value);
            }
        }
        return null;
    }
}
