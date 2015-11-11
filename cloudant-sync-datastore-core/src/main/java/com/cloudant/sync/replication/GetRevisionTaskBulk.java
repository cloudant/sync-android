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

package com.cloudant.sync.replication;

import com.cloudant.sync.datastore.DocumentRevsList;
import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.List;


class GetRevisionTaskBulk implements Iterable<DocumentRevsList> {

    private final CouchDB sourceDb;
    private final List<BulkGetRequest> requests;
    private final boolean pullAttachmentsInline;
    private final Iterable<DocumentRevsList> resultsIterable;

    public GetRevisionTaskBulk(CouchDB sourceDb,
                           List<BulkGetRequest> requests,
                           boolean pullAttachmentsInline) {
        Preconditions.checkNotNull(sourceDb, "sourceDb cannot be null");
        Preconditions.checkNotNull(requests, "requests cannot be null");
        for(BulkGetRequest request : requests) {
            Preconditions.checkNotNull(request.id, "id cannot be null");
            Preconditions.checkNotNull(request.revs, "revs cannot be null");
        }
        this.sourceDb = sourceDb;
        this.requests = requests;
        this.pullAttachmentsInline = pullAttachmentsInline;
        this.resultsIterable = sourceDb.bulkGetRevisions(requests, pullAttachmentsInline);
    }

    @Override
    public Iterator<DocumentRevsList> iterator() {
        return this.resultsIterable.iterator();
    }

    @Override
    public String toString() {
        return "GetRevisionTask{" +
                "sourceDb=" + sourceDb +
                ", requests=" + requests +
                ", pullAttachmentsInline=" + pullAttachmentsInline +
                '}';
    }
}
