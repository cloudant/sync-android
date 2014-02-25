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
import com.cloudant.common.Log;
import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

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
class GetRevisionTask implements Callable<DocumentRevsList> {

    private static final String LOG_TAG = "GetRevisionTask";

    private String documentId;
    private Collection<String> openRevisions;
    private Collection<String> attsSince;
    private boolean pullAttachmentsInline;
    CouchDB sourceDb;

    public static Callable<DocumentRevsList> createGetRevisionTask(CouchDB sourceDb,
                                                                   String docId,
                                                                   Collection<String> openRevisions,
                                                                   Collection<String> attsSince,
                                                                   boolean pullAttachmentsInline) {
        GetRevisionTask task = new GetRevisionTask(sourceDb, docId, openRevisions, attsSince, pullAttachmentsInline);
        return new RetriableTask<DocumentRevsList>(task);
    }

   public GetRevisionTask(CouchDB sourceDb,
                          String docId, Collection<String> openRevisions,
                          Collection<String> attsSince,
                          boolean pullAttachmentsInline) {
        Preconditions.checkNotNull(docId, "docId cannot be null");
        Preconditions.checkNotNull(openRevisions, "revId cannot be null");
        Preconditions.checkNotNull(sourceDb, "sourceDb cannot be null");

        this.documentId = docId;
        this.openRevisions = openRevisions;
        this.sourceDb = sourceDb;
        this.attsSince = attsSince;
        this.pullAttachmentsInline = pullAttachmentsInline;
    }

    @Override
    public DocumentRevsList call() throws Exception {
        Log.v(this.LOG_TAG, "Fetching document: " + this.documentId);
        return new DocumentRevsList(this.sourceDb.getRevisions(documentId,
                openRevisions,
                attsSince,
                pullAttachmentsInline));
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder("GetRevisionTask: ")
                .append("{ documentId : \"").append(this.documentId).append("\", ")
                .append("openRevisions : ").append(Arrays.asList(this.openRevisions)).append(" }");
        return s.toString();
    }
}
