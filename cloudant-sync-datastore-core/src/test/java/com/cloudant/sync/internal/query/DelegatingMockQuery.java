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

package com.cloudant.sync.internal.query;

import com.cloudant.sync.query.Index;
import com.cloudant.sync.query.IndexType;
import com.cloudant.sync.query.Query;
import com.cloudant.sync.query.QueryException;
import com.cloudant.sync.query.QueryResult;
import com.cloudant.sync.internal.query.FieldSort;
import com.cloudant.sync.internal.query.QueryImpl;

import java.util.List;
import java.util.Map;

public abstract class DelegatingMockQuery implements Query {

    protected final QueryImpl delegate;

    DelegatingMockQuery(QueryImpl delegate) {
        this.delegate = delegate;
    }

    @Override
    public List<Index> listIndexes() throws QueryException {
        return delegate.listIndexes();
    }

    @Override
    public String ensureIndexed(List<FieldSort> fieldNames) throws QueryException {
        return delegate.ensureIndexed(fieldNames);
    }

    @Override
    public String ensureIndexed(List<FieldSort> fieldNames, String indexName) throws QueryException {
        return delegate.ensureIndexed(fieldNames, indexName);
    }

    @Override
    public String ensureIndexed(List<FieldSort> fieldNames, String indexName, IndexType indexType) throws QueryException {
        return delegate.ensureIndexed(fieldNames, indexName, indexType);
    }

    @Override
    public String ensureIndexed(List<FieldSort> fieldNames, String indexName, IndexType indexType, String tokenize) throws QueryException {
        return delegate.ensureIndexed(fieldNames,indexName, indexType, tokenize);
    }

    @Override
    public void deleteIndex(String indexName) throws QueryException {
        delegate.deleteIndex(indexName);
    }

    @Override
    public void updateAllIndexes() throws QueryException {
        delegate.updateAllIndexes();
    }

    @Override
    public QueryResult find(Map<String, Object> query) throws QueryException {
        return delegate.find(query);
    }

    @Override
    public QueryResult find(Map<String, Object> query, long skip, long limit, List<String> fields, List<FieldSort> sortDocument) throws QueryException {
        return delegate.find(query, skip, limit, fields, sortDocument);
    }

    public void close() {
        delegate.close();
    }
}
