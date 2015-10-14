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

import com.cloudant.common.RetriableTask;
import com.cloudant.sync.datastore.DocumentRevsList;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

/**
 * GetRevisionTask handles calling getting the revision tree for a given docId and open
 * revisions as a Callable.
 *
 * The document id and open revisions are from one row of change feeds. For example, for the
 * following change feed:
 *
 * {
 *   "last_seq": 35,
 *   "results": [
 *     { "changes":
 *       [ { "rev": "1-bd42b942b8b672f0289cf3cd1f67044c" } ],
 *       "id": "2013-09-23T20:50:56.251Z",
 *       "seq": 27
 *     },
 *     { "changes":
 *       [ { "rev": "29-3f4dabfb32290e557ac1d16b2e8f069c" },
 *         { "rev": "29-01fcbf8a3f1457eff21e18f7766d3b45" },
 *         { "rev": "26-30722da17ad35cf1860f126dba391d67" }
 *       ],
 *       "id": "2013-09-10T17:47:17.770Z",
 *       "seq": 35
 *       }
 *    ]
 * }
 *
 * For document with id "2013-09-10T17:47:17.770Z", it has open revisions:
 *
 *       [
 *         { "rev": "29-3f4dabfb32290e557ac1d16b2e8f069c" },
 *         { "rev": "29-01fcbf8a3f1457eff21e18f7766d3b45" },
 *         { "rev": "26-30722da17ad35cf1860f126dba391d67" }
 *       ]
 */
class GetRevisionTask implements Callable<Iterable<DocumentRevsList>> {

    private static final String LOG_TAG = "GetRevisionTask";
    private static final Logger logger = Logger.getLogger(GetRevisionTask.class.getCanonicalName());

    CouchDB sourceDb;
    List<BulkGetRequest> requests;
    private boolean pullAttachmentsInline;

    public static Callable<Iterable<DocumentRevsList>> createGetRevisionTask(CouchDB sourceDb,
                                                                   List<BulkGetRequest> requests,
                                                                   boolean pullAttachmentsInline) {
        GetRevisionTask task = new GetRevisionTask(sourceDb, requests, pullAttachmentsInline);
        return new RetriableTask<Iterable<DocumentRevsList>>(task);
    }

   public GetRevisionTask(CouchDB sourceDb,
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
    }

    @Override
    public Iterable<DocumentRevsList> call() throws Exception {
        Iterable<DocumentRevsList> revs = this.sourceDb.bulkGetRevisions(requests,
                pullAttachmentsInline);
        return revs;
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
