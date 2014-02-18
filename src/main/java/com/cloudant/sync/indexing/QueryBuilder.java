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
import java.util.Map;
import java.util.HashMap;

/**
 * <p>
 * {@code QueryBuilder} builds a query, represented by a {@code Map},
 * which can be used in {@link com.cloudant.sync.indexing.IndexManager#query(java.util.Map)}.
 * The following query types are supported:
 * </p>
 * <ol>
 *   <li>Match a given field to a specified value.</li>
 *   <li>Match given field to multiple values.</li>
 *   <li>Range query.</li>
 * </ol>
 * <p>
 * Each index can have a single criterion defined in one overarching query.
 * The query can have criteria defined over multiple indexes. The result
 * is an intersection of the documents matching each query criterion.
 * </p>
 * <p>
 * {@code QueryBuilder} instances are not thread safe.
 * </p>
 *
 * @see IndexManager
 * @see com.cloudant.sync.datastore.Datastore
 *
 */
public class QueryBuilder {

    Map<String, Object> query = new HashMap<String, Object>();
    Map<String, Object> options = new HashMap<String, Object>();
    String indexName = null;

    public QueryBuilder() {
    }

    /**
     * Returns the current state of the query in a format suitable for
     * passing to {@link com.cloudant.sync.indexing.IndexManager#query(java.util.Map)}.
     *
     * <p>The return value will be updated if further calls are made to this
     * builder object.</p>
     */
    public Map<String, Map<String, Object>> build() {
        Map<String, Map<String, Object>> queryWithOptions = new HashMap<String, Map<String, Object>>();
        queryWithOptions.put("query", query);
        queryWithOptions.put("options", options);
        return queryWithOptions;
    }

    /**
     * Defines the index upon which future calls will operate.
     *
     * <p>This method would typically be called several times when building
     * a query, once for each index that's being used in the query.</p>
     *
     * @return this object for method chaining.
     */
    public QueryBuilder index(String indexName) {
        this.indexName = indexName;
        return this;
    }

    /**
     * The value in the previous {@code index} call must match this
     * {@code value}.
     *
     * @return this object for method chaining.
     */
    public QueryBuilder equalTo(String value) {
        return equalToInternal(value);
    }

    /**
     * The value in the previous {@code index} call must match this
     * {@code value}.
     *
     * @return this object for method chaining.
     */
    public QueryBuilder equalTo(Long value) {
        return equalToInternal(value);
    }

    /**
     * The value in the previous {@code index} call must match this
     * {@code value}.
     *
     * @return this object for method chaining.
     */
    public QueryBuilder equalTo(Integer value) {
        return equalToInternal(value);
    }

    private QueryBuilder equalToInternal(Object value) {
        if(indexName == null) {
            throw new IllegalStateException("indexName() must be called before equalTo() is called.");
        }
        this.query.put(this.indexName, value);
        return clearIndexName();
    }

    /**
     * The value in the previous {@code index} call must be one of
     * {@code values}.
     *
     * @return this object for method chaining.
     */
    public QueryBuilder oneOf(String... values) {
        return oneOfInternal((Object[])values);
    }

    /**
     * The value in the previous {@code index} call must be one of
     * {@code values}.
     *
     * @return this object for method chaining.
     */
    public QueryBuilder oneOf(Long... values) {
        return oneOfInternal((Object[])values);
    }

    private QueryBuilder oneOfInternal(Object... values) {
        if(indexName == null) {
            throw new IllegalStateException("indexName() must be called directly before oneOf() is called.");
        }
        this.query.put(this.indexName, Arrays.asList(values));
        return clearIndexName();
    }

    /**
     * The value in the previous {@code index} call must be greater than
     * {@code value}.
     *
     * @return this object for method chaining.
     */
    public QueryBuilder greaterThan(Long value) {
        return greaterThanInternal(value);
    }

    /**
     * The value in the previous {@code index} call must be greater than
     * {@code value}.
     *
     * @return this object for method chaining.
     */
    public QueryBuilder greaterThan(String value) {
        return greaterThanInternal(value);
    }

    private QueryBuilder greaterThanInternal(Object value) {
        if(indexName == null) {
            throw new IllegalStateException("indexName() must be called directly before greaterThan() is called.");
        }

        Map g = new HashMap();
        if(this.query.containsKey(this.indexName)) {
            if(this.query.get(this.indexName) instanceof Map) {
                g = (Map)this.query.get(this.indexName);
                if(g.containsKey("min")) {
                    throw new IllegalStateException("Min value already set for indexName:" + this.indexName);
                }
            } else {
                throw new IllegalStateException("Index " + indexName + " already used in the query");
            }
        }
        g.put("min", value);
        this.query.put(this.indexName, g);
        return clearIndexName();
    }

    /**
     * The value in the previous {@code index} call must be less than
     * {@code value}.
     *
     * @return this object for method chaining.
     */
    public QueryBuilder lessThan(Long value) {
        return lessThanInternal(value);
    }

    /**
     * The value in the previous {@code index} call must be less than
     * {@code value}.
     *
     * @return this object for method chaining.
     */
    public QueryBuilder lessThan(String value) {
        return lessThanInternal(value);
    }

    private QueryBuilder lessThanInternal(Object value) {
        if(indexName == null) {
            throw new IllegalStateException("indexName() must be called before lessThan() is called.");
        }

        Map g = new HashMap();
        if(this.query.containsKey(this.indexName)) {
            if(this.query.get(this.indexName) instanceof Map) {
                g = (Map)this.query.get(this.indexName);
                if(g.containsKey("max")) {
                    throw new IllegalStateException("Max value already set for indexName:" + this.indexName);
                }
            } else {
                throw new IllegalStateException("Index " + indexName + " already used in the query");
            }
        }
        g.put("max", value);
        this.query.put(this.indexName, g);
        return clearIndexName();
    }

    /**
     * Limit the number of results returned by the query to {@code value}
     *
     * @return this object for method chaining.
     */
    public QueryBuilder limit(int value) {
        return limitInternal(value);
    }

    private QueryBuilder limitInternal(int value) {
        options.put("limit", value);
        return this;
    }

    /**
     * Returned results from the query will start at offset specified by {@code value}
     *
     * @return this object for method chaining.
     */
    public QueryBuilder offset(int value) {
        return offsetInternal(value);
    }

    private QueryBuilder offsetInternal(int value) {
        options.put("offset", value);
        return this;
    }

    /**
     * Sort results according to the index specified by {@code value}
     *
     * @return this object for method chaining.
     */
    public QueryBuilder sortBy(String value) {
        return sortByInternal(value);
    }

    private QueryBuilder sortByInternal(String value) {
        // TODO error checking
        options.put("sort_by", value);
        return this;
    }

    /**
     * Clears the currently selected index name.
     *
     * @return this object for method chaining.
     */
    public QueryBuilder clearIndexName() {
        this.indexName = null;
        return this;
    }
}
