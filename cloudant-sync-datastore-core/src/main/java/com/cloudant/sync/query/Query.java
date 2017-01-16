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

import com.cloudant.sync.query.FieldSort.Direction;

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
     * Create a JSON {@link Index} for one or more fields with an optional index name, and ensure the
     * index is up to date.
     * </p>
     * <p>
     * Note that the sort order on fields for index definitions currently only supports the
     * {@link Direction#ASCENDING} direction. If there are one or more members of {@code
     * fields} with a direction of {@link Direction#DESCENDING}, an exception will be thrown.
     * </p>
     * <p>
     * To return data in descending order, create an index with {@link Direction#ASCENDING} fields
     * and execute the subsequent query with {@link Direction#DESCENDING} fields as required.
     * </p>
     * <p>
     * If an index with these fields already exists, then no new index is created, and the
     * existing equivalent {@link Index} is returned. Note that the existing equivalent index
     * may have a different name to the one requested.
     * </p>
     * <p>
     * For new indexes, the index data will be created for the first time. For existing indexes, the
     * index data will be updated by reading all documents in the
     * {@link com.cloudant.sync.documentstore.DocumentStore} which have changed since the last
     * update.
     * </p>
     *
     * @param fields the fields to index
     * @param indexName  the name of the index to be created, or null for an automatically
     *                   generated name
     * @return the requested {@link Index}, or the existing {@link Index} (see above)
     * @throws QueryException if there was a problem creating or updating the index
     */
    Index createJsonIndex(List<FieldSort> fields, String indexName) throws QueryException;

    /**
     * <p>
     * Create a text {@link Index} for one or more fields with an optional index name and optional
     * SQLite FTS tokenizer, and ensure the index is up to date.
     * </p>
     * <p>
     * Note that the sort order on fields for index definitions currently only supports the
     * {@link Direction#ASCENDING} direction. If there are one or more members of {@code
     * fields} with a direction of {@link Direction#DESCENDING}, an exception will be thrown.
     * </p>
     * <p>
     * To return data in descending order, create an index with {@link Direction#ASCENDING} fields
     * and execute the subsequent query with {@link Direction#DESCENDING} fields as required.
     * </p>
     * <p>
     * If an index with these fields and options already exists, then no new index is created, and
     * the existing equivalent {@link Index} is returned. Note that the existing equivalent index
     * may have a different name to the one requested.
     * </p>
     * <p>
     * For new indexes, the index data will be created for the first time. For existing indexes, the
     * index data will be updated by reading all documents in the
     * {@link com.cloudant.sync.documentstore.DocumentStore} which have changed since the last
     * update.
     * </p>
     * @param fields the fields to index
     * @param indexName the name of the index to be created, or null for an automatically generated name
     * @param tokenizer the SQLite FTS tokenizer to use for the text index, or {@code null} or
     *                  {@link Tokenizer#DEFAULT} for the default tokenizer
     * @return the requested {@link Index}, or the existing {@link Index} (see above)
     * @throws QueryException if there was a problem creating or updating the index
     */
    Index createTextIndex(List<FieldSort> fields, String indexName, Tokenizer tokenizer)
            throws QueryException;

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
    void refreshAllIndexes() throws QueryException;

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
     * @param sortSpecification specification of fields to use to order the result
     * @return a {@link QueryResult} representing the set of matching documents
     * @throws QueryException if there was a problem executing the query
     */
    QueryResult find(Map<String, Object> query,
                     long skip,
                     long limit,
                     List<String> fields,
                     List<FieldSort> sortSpecification)
            throws QueryException;

}

