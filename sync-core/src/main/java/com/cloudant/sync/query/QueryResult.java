//  Copyright (c) 2014 Cloudant. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.cloudant.sync.query;

import com.cloudant.sync.datastore.BasicDocumentRevision;

import java.util.List;

/**
 *  Iterable result of a query executed with {@link IndexManager}.
 *
 *  @see IndexManager
 */
public interface QueryResult extends Iterable<BasicDocumentRevision> {

    /**
     *  Returns the number of documents in this query result.
     *
     *  @return the number of the {@code DocumentRevision} in this query result
     */
    public long size();

    /**
     *  Returns a list of the document ids in this query result.
     *
     *  @return list of the document ids
     */
    public List<String> documentIds();

}