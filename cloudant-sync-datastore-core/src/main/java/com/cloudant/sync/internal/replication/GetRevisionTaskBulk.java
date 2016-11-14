/*
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.internal.replication;

import com.cloudant.sync.internal.documentstore.DocumentRevsList;
import com.cloudant.sync.internal.util.Misc;

import java.util.Iterator;
import java.util.List;

/**
 * Handles calling CouchClient.bulkReadDocsWithOpenRevisions(), returning the results back in a
 * manner which can be iterated over (to avoid deserialising the entire result into memory)
 *
 * For each revision ID, gets the revision tree for a given document ID and lists of open revision IDs
 * and "atts_since" (revision IDs for which we know we have attachments)
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

class GetRevisionTaskBulk implements Iterable<DocumentRevsList> {

    private final CouchDB sourceDb;
    private final List<BulkGetRequest> requests;
    private final boolean pullAttachmentsInline;
    private final Iterable<DocumentRevsList> resultsIterable;

    public GetRevisionTaskBulk(CouchDB sourceDb,
                           List<BulkGetRequest> requests,
                           boolean pullAttachmentsInline) {
        Misc.checkNotNull(sourceDb, "sourceDb");
        Misc.checkNotNull(requests, "requests");
        for(BulkGetRequest request : requests) {
            Misc.checkNotNull(request.id, "id");
            Misc.checkNotNull(request.revs,"revs");
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
