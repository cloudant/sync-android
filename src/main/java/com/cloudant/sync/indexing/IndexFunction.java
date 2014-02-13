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

import java.util.List;
import java.util.Map;

/**
 * <p>
 * This interface defines the IndexFunction object used to
 * map from document to indexed values by the
 * {@link IndexManager} object.
 * </p>
 * <p>
 * The Index function is called for every document as it's
 * either inserted into the index for the first time, or its
 * value changes and it needs to be updated.
 * </p>
 *
 * <p>
 * For an example, see {@link FieldIndexFunction}.
 * </p>
 */
public interface IndexFunction<T> {

    /**
     * <p>
     * Returns the values from the passed document that should be indexed.
     * </p>
     *
     * <p>
     * This method is assumed to be a <em>pure function</em>:
     * </p>
     * <ul>
     *     <li>Calling it multiple times on the same document should produce
     *     the same output. For example, don't emit the current time.</li>
     *     <li>It should not produce externally visible changes in program
     *     state.</li>
     * </ul>
     *
     * <p>If it's not, behaviour is undefined.</p>
     *
     * @param indexName The name of the index the values are destined for
     * @param map The document being indexed
     * @return An object representing the value to be indexed. This must match the index's defined type.
     */
    List<T> indexedValues(String indexName, Map map);
}
