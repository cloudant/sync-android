/*
 * Copyright (c) 2016 IBM Corp. All rights reserved.
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

package com.cloudant.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CollectionFactory {

    public static final StaticListGenerator LIST = new StaticListGenerator();
    public static final StaticSetGenerator SET = new StaticSetGenerator();
    public static final StaticMapGenerator MAP = new StaticMapGenerator();

    public static class StaticListGenerator {
        public static <E> List<E> of(E... elements) {
            return new ListGenerator<E>().of(elements);
        }
    }

    public static class StaticSetGenerator {
        public static <E> Set<E> of(E... elements) {
            return new SetGenerator<E>().of(elements);
        }
    }

    public static class StaticMapGenerator {

        public static <K, V> Map<K, V> of(Map.Entry<K, V>... entries) {
            return new MapGenerator<K, V>().of(entries);
        }

        public static <K, V> Map<K, V> of(K key, V value) {
            return of(entry(key, value));
        }

        public static <K, V> Map<K, V> of(K key1, V value1, K key2, V value2) {
            List<Map.Entry<K, V>> entries = new ArrayList<Map.Entry<K, V>>(2);
            entries.add(entry(key1, value1));
            entries.add(entry(key2, value2));
            return of(entries.toArray(new Map.Entry[entries.size()]));
        }

        public static <T> Map<T, T> of(T... alternatingKeysAndValues) {
            if (alternatingKeysAndValues.length % 2 == 0) {
                List<Map.Entry<T, T>> entries = new ArrayList<Map.Entry<T, T>>
                        (alternatingKeysAndValues.length / 2);
                T key = null;
                for (T value : alternatingKeysAndValues) {
                    if (key == null) {
                        key = value;
                    } else {
                        // Alternating key/value, use key from previous loop iteration with value
                        // from this iteration.
                        entries.add(entry(key, value));
                        // Reset key to null for next iteration.
                        key = null;
                    }
                }
                return of(entries.toArray(new Map.Entry[entries.size()]));
            } else {
                throw new IllegalArgumentException("Must have an equal number of keys and values.");
            }
        }

        public static <K, V> Map.Entry<K, V> entry(final K key, final V value) {
            return new Map.Entry<K, V>() {

                @Override
                public K getKey() {
                    return key;
                }

                @Override
                public V getValue() {
                    return value;
                }

                @Override
                public Object setValue(Object value) {
                    throw new UnsupportedOperationException("Read only entry.");
                }
            };
        }
    }

    private static abstract class Generator<T, E> {
        public T of(E... entries) {
            T collection = newInstance(entries.length);
            for (E entry : entries) {
                addEntry(collection, entry);
            }
            return unmodifiable(collection);
        }

        protected abstract T newInstance(int size);

        protected abstract void addEntry(T collection, E entry);

        protected abstract T unmodifiable(T original);
    }

    private static abstract class CollectionGenerator<T extends Collection<E>, E> extends
            Generator<T, E> {

        @Override
        protected void addEntry(T collection, E entry) {
            collection.add(entry);
        }
    }

    private static final class ListGenerator<E> extends CollectionGenerator<List<E>, E> {

        @Override
        protected List<E> newInstance(int size) {
            return new ArrayList<E>(size);
        }

        @Override
        protected List<E> unmodifiable(List<E> original) {
            return Collections.unmodifiableList(original);
        }

    }

    private static final class SetGenerator<E> extends CollectionGenerator<Set<E>, E> {

        @Override
        protected Set<E> newInstance(int size) {
            return new HashSet<E>(size);
        }

        @Override
        protected Set<E> unmodifiable(Set<E> original) {
            return Collections.unmodifiableSet(original);
        }

    }

    private static final class MapGenerator<K, V> extends Generator<Map<K, V>, Map.Entry<K, V>> {

        @Override
        protected Map<K, V> newInstance(int size) {
            return new HashMap<K, V>(size);
        }

        @Override
        protected Map<K, V> unmodifiable(Map<K, V> original) {
            return Collections.unmodifiableMap(original);
        }

        @Override
        protected void addEntry(Map<K, V> collection, Map.Entry<K, V> entry) {
            collection.put(entry.getKey(), entry.getValue());
        }

    }
}
