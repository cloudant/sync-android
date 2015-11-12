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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Handles calling CouchClient.getDocWithOpenRevisions() for a batch of revisions IDs, with
 * multiple simultaneous HTTP threads, returning the results back in a manner which can be iterated
 * over (to avoid deserialising the entire result into memory).
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
class GetRevisionTaskThreaded implements Iterable<DocumentRevsList> {

    // TODO logging
    private static final Logger logger = Logger.getLogger(GetRevisionTaskThreaded.class.getCanonicalName());

    // members used to make requests:

    private final ThreadPoolExecutor executorService;
    private final CouchDB sourceDb;
    private final List<BulkGetRequest> requests;
    private final boolean pullAttachmentsInline;

    int threads = 4; // TODO - config?

    // members used to handle responses:

    LinkedBlockingQueue<DocumentRevsList> responses;
    AtomicInteger requestsOutstanding;

    private RuntimeException exception;

    public GetRevisionTaskThreaded(CouchDB sourceDb,
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
        this.executorService = new ThreadPoolExecutor(threads, threads, 1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>());
        // limit the size of the response queue, so we don't produce thousands of results before
        // they have been consumed
        this.responses = new LinkedBlockingQueue<DocumentRevsList>(threads);
        this.requestsOutstanding = new AtomicInteger(requests.size());

        // we make the request at construction time...
        for (final BulkGetRequest request : requests) {
            if (exception != null) {
                // we already encountered an exception, no point stacking up any more requests
                return;
            }
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        boolean offered = responses.offer(new DocumentRevsList
                                (GetRevisionTaskThreaded.this.sourceDb.getRevisions
                                (request.id, request.revs, request.atts_since,
                                        GetRevisionTaskThreaded.this
                                        .pullAttachmentsInline)), 10, TimeUnit.MINUTES);
                        // we need to tell the iterator things went wrong :(
                        if (!offered) {
                            exception = new RuntimeException("offer() failed; response was not " +
                                    "consumed within time limit");
                        }
                    } catch (Exception e) {
                        exception = new RuntimeException(e);
                    }
                }
            });
        }
    }

    @Override
    public Iterator<DocumentRevsList> iterator() {
        return new GetRevisionTaskIterator();
    }

    @Override
    public String toString() {
        return "GetRevisionTask{" +
                "sourceDb=" + sourceDb +
                ", requests=" + requests +
                ", pullAttachmentsInline=" + pullAttachmentsInline +
                '}';
    }

    private class GetRevisionTaskIterator implements Iterator<DocumentRevsList> {

        @Override
        public boolean hasNext() {
            // can't advance iterator as there was a problem in execute()
            if (exception != null) {
                throw exception;
            }

            boolean next = requestsOutstanding.get() != 0;

            // if we have returned all of our results, we can shut down the executor
            if (!next) {
                executorService.shutdown();
                try {
                    executorService.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    ;
                }
            }

            return next;
        }

        @Override
        public DocumentRevsList next() {
            // can't advance iterator as there was a problem in execute()
            if (exception != null) {
                throw exception;
            }

            try {
                requestsOutstanding.decrementAndGet();
                return responses.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported");
        }

    }
}
