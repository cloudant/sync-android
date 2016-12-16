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

package com.cloudant.sync.query;

import java.util.List;
import java.util.Map;

/**
 * Created by tomblench on 28/09/2016.
 */

/**
 * <p>
 * The {@code Query} class provides a local implementation of the Cloudant Query feature. It
 * includes methods to do the following:
 * </p>
 *
 * <ul>
 * <li>create indexes</li>
 * <li>delete indexes</li>
 * <li>execute queries</li>
 * <li>update indexes</li>
 * </ul>
 *
 * <p>
 * Comprehensive documentation for the query feature can be found in the project
 * <a target="_blank" href="https://github.com/cloudant/sync-android/blob/master/doc/query.md">
 * markdown document.</a>
 * </p>
 *
 * @api_public
 */
public interface Query {

    /**
     * List all indexes
     * @return List of {@link Index}
     * @throws QueryException if there was a problem listing indexes
     */
    List<Index> listIndexes() throws QueryException;

    /**
     * <p>
     * Create an {@link Index} with an automatically generated name and default options.
     * </p>
     * <p>
     * The default options are {@code indexType=JSON, tokenize=null}.
     * </p>
     * <p>
     * If an index with these fields and options already exists, then no new index is created, and
     * the existing equivalent index name is returned.
     * </p>
     * @param fieldNames the fields to index
     * @return the generated index name, or the existing index name (see above)
     * @throws QueryException if there was a problem creating the index
     */
    String ensureIndexed(List<FieldSort> fieldNames) throws QueryException;

    /**
     * <p>
     * Create an {@link Index} with a given name and default options.
     * </p>
     * <p>
     * The default options are {@code indexType=JSON, tokenize=null}.
     * </p>
     * <p>
     * If an index with these fields and options already exists, then no new index is created, and
     * the existing equivalent index name is returned.
     * </p>
     * @param fieldNames the fields to index
     * @param indexName the name of the index to be created
     * @return the requested index name, or the existing index name (see above)
     * @throws QueryException if there was a problem creating the index
     */
    String ensureIndexed(List<FieldSort> fieldNames, String indexName) throws QueryException;

    /**
     * <p>
     * Create an {@link Index} with a given name and of a given type.
     * </p>
     * <p>
     * If {@code indexType=TEXT}, then the default tokenizer will be used.
     * </p>
     * <p>
     * If an index with these fields and options already exists, then no new index is created, and
     * the existing equivalent index name is returned.
     * </p>
     * @param fieldNames the fields to index
     * @param indexName the name of the index to be created
     * @param indexType the type of the index to be created ({@code text} or {@code JSON})
     * @return the requested index name, or the existing index name (see above)
     * @throws QueryException if there was a problem creating the index
     */
    String ensureIndexed(List<FieldSort> fieldNames, String indexName, IndexType indexType)
            throws QueryException;

    /**
     * <p>
     * Create an {@link Index} with a given name and of a given type and optionally a tokenizer.
     * </p>
     * <p>
     * If an index with these fields and options already exists, then no new index is created, and
     * the existing equivalent index name is returned.
     * </p>
     * @param fieldNames the fields to index
     * @param indexName the name of the index to be created
     * @param indexType the type of the index to be created ({@code text} or {@code JSON})
     * @param tokenize the SQLite FTS tokenizer to use for {@code text} indexes
     * @return the requested index name, or the existing index name (see above)
     * @throws QueryException if there was a problem creating the index
     */    String ensureIndexed(List<FieldSort> fieldNames,
                         String indexName,
                         IndexType indexType,
                         String tokenize) throws QueryException;

    /**
     * Delete index metadata and data
     * @param indexName name of the index to delete
     * @throws QueryException if there was a problem deleting the index
     */
    void deleteIndex(String indexName) throws QueryException;

    /**
     * Update all index data by reading all documents in the
     * {@link com.cloudant.sync.documentstore.DocumentStore} which have changed since the last
     * update
     * @throws QueryException if there was a problem updating the index
     */
    void updateAllIndexes() throws QueryException;

    /**
     * Execute a query to find data
     * @param query query in Cloudant Query syntax
     * @return a {@link QueryResult} representing the set of matching documents
     * @throws QueryException if there was a problem executing the query
     */
    QueryResult find(Map<String, Object> query) throws QueryException;

    /**
     * Execute a query to find data
     * @param query query in Cloudant Query syntax
     * @param skip initial number of documents to omit from the result returned (used for
     *             pagination)
     * @param limit upper bound for number of documents to return (used for pagination)
     * @param fields list of field names to choose ("project") from the matching documents
     * @param sortDocument specification of fields to use to order the result
     * @return a {@link QueryResult} representing the set of matching documents
     * @throws QueryException if there was a problem executing the query
     */
    QueryResult find(Map<String, Object> query,
                     long skip,
                     long limit,
                     List<String> fields,
                     List<FieldSort> sortDocument)
            throws QueryException;

}

