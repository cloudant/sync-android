/*
 * Copyright © 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.internal.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 * A map implementation that stores a list of values for each key. To remain consistent
 * with existing Map interfaces the put* methods perform replacement of keys with new
 * lists of values, the addValue* methods should be used to add new values to the existing keys.
 * </p>
 * <p>
 * Note that when using put to add a list of values for a key that later calls to addValue* will
 * modify the original list. Consequently if the list is not modifiable then these operations will
 * fail (typically with an UnsupportedOperationException).
 * </p>
 * <p>
 * This implementation does not currently support removing some values from a key.
 * </p>
 *
 * @param <K> the type of key
 * @param <V> the type of values
 * @api_private
 */
public class ValueListMap<K, V> extends ConcurrentHashMap<K, List<V>> {


    private static final long serialVersionUID = 5775249920237785452L;

    /**
     * Add a value to the map under the existing key or creating a new key if it does not
     * yet exist.
     *
     * @param key   the key to associate the values with
     * @param value the value to add to the key
     */
    public void addValueToKey(K key, V value) {
        this.addValuesToKey(key, Collections.singletonList(value));
    }

    /**
     * Add a collection of one or more values to the map under the existing key or creating a new key
     * if it does not yet exist in the map.
     *
     * @param key             the key to associate the values with
     * @param valueCollection a collection of values to add to the key
     */
    public void addValuesToKey(K key, Collection<V> valueCollection) {
        if (!valueCollection.isEmpty()) {
            // Create a new collection to store the values (will be changed to internal type by call
            // to putIfAbsent anyway)
            List<V> collectionToAppendValuesOn = new ArrayList<V>();
            List<V> existing = this.putIfAbsent(key, collectionToAppendValuesOn);
            if (existing != null) {
                // Already had a collection, discard the new one and use existing
                collectionToAppendValuesOn = existing;
            }
            collectionToAppendValuesOn.addAll(valueCollection);
        }
    }
}
