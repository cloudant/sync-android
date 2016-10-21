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

public interface IndexManager {


    List<Index> listIndexes() throws QueryException;

    String ensureIndexed(List<FieldSort> fieldNames) throws QueryException;

    String ensureIndexed(List<FieldSort> fieldNames, String indexName) throws QueryException;

    String ensureIndexed(List<FieldSort> fieldNames, String indexName, IndexType indexType)
            throws QueryException;

    String ensureIndexed(List<FieldSort> fieldNames,
                         String indexName,
                         IndexType indexType,
                         String tokenize) throws QueryException;


    void deleteIndex(String indexName) throws QueryException;

    void updateAllIndexes() throws QueryException; // not sure if this should throw or not.

    QueryResult find(Map<String, Object> query) throws QueryException;

    QueryResult find(Map<String, Object> query,
                     long skip,
                     long limit,
                     List<String> fields,
                     List<FieldSort> sortDocument)
            throws QueryException;

    // TODO we may not want to expose this publicly
    void close();
    boolean isTextSearchEnabled();

}
