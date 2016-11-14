/*
 * Copyright © 2013, 2016 IBM Corp. All rights reserved.
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
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles calling CouchClient.getDocWithOpenRevisions() for a batch of revisions IDs, with
 * multiple simultaneous HTTP threads, returning the results back in a manner which can be iterated
 * over (to avoid deserialising the entire result into memory).
 *
 * For each revision ID, gets the revision tree for a given document ID and lists of open
 * revision IDs
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

    private static final Logger logger = Logger.getLogger(GetRevisionTaskThreaded.class
            .getCanonicalName());

    // this should be a sensible number of threads but we may make this configurable in future
    private static final int threads = Runtime.getRuntime().availableProcessors() * 2;
    private static final ThreadPoolExecutor executorService;

    static {
        // A static thread pool allows it to be shared between all tasks, reducing the overheads of
        // thread creation and destruction, but at the expense of sharing threads between all
        // replications (within the classloader/android application).
        // On a large number of batches this offers a 4-5% improvement over creating a thread pool
        // for each task instance.
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(threads, threads, 1,
                TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
        // Allowing core threads to timeout means we don't keep threads in memory except when
        // replication tasks are running.
        tpe.allowCoreThreadTimeOut(true);
        executorService = tpe;
    }

    // members used to make requests:
    private final QueuingExecutorCompletionService<BulkGetRequest, DocumentRevsList> completionService;
    private final CouchDB sourceDb;
    private final Queue<BulkGetRequest> requests = new ConcurrentLinkedQueue<BulkGetRequest>();
    private final boolean pullAttachmentsInline;

    // members used to handle responses:
    private boolean iteratorValid = true;

    // timeout for poll()
    int responseTimeout = 5;
    TimeUnit responseTimeoutUnits = TimeUnit.MINUTES;

    public GetRevisionTaskThreaded(CouchDB sourceDb,
                                   List<BulkGetRequest> requests,
                                   boolean pullAttachmentsInline) {
        Misc.checkNotNull(sourceDb, "sourceDb");
        Misc.checkNotNull(requests, "requests");
        for (BulkGetRequest request : requests) {
            Misc.checkNotNull(request.id, "id");
            Misc.checkNotNull(request.revs, "revs");
        }

        this.sourceDb = sourceDb;
        this.requests.addAll(requests);
        this.pullAttachmentsInline = pullAttachmentsInline;
        this.completionService = new QueuingExecutorCompletionService<BulkGetRequest,
                DocumentRevsList>(executorService, this.requests, threads + 1) {
            @Override
            public DocumentRevsList executeRequest(BulkGetRequest request) {
                // since this is part of a thread pool, we'll rename each thread as it takes a task.
                try {
                    Thread.currentThread().setName("GetRevisionThread: " + GetRevisionTaskThreaded.this.sourceDb.getIdentifier());

                } catch (SecurityException e){
                    logger.log(Level.WARNING, "Could not rename pull strategy pool thread", e);
                }
                return new DocumentRevsList(GetRevisionTaskThreaded.this.sourceDb.getRevisions
                        (request.id, request.revs, request.atts_since,
                                GetRevisionTaskThreaded.this.pullAttachmentsInline));
            }
        };
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
            // can't advance iterator as there was a problem in call()
            if (!iteratorValid) {
                return false;
            }

            boolean next = completionService.hasRequestsOutstanding();

            // if we have returned all of our results, we can shut down the executor
            if (!next) {
                iteratorValid = false;
            }

            return next;
        }

        @Override
        public DocumentRevsList next() {
            // can't advance iterator as there was a problem in call()
            if (!iteratorValid) {
                throw new NoSuchElementException("Iterator has been invalidated");
            }

            try {
                Future<DocumentRevsList> pollResult = completionService.poll(responseTimeout,
                        responseTimeoutUnits);
                if (pollResult == null) {
                    throw new NoSuchElementException("Poll timed out");
                }
                return pollResult.get();
            } catch (InterruptedException ie) {
                iteratorValid = false;
                throw new RuntimeException(ie);
            } catch (ExecutionException ee) {
                iteratorValid = false;
                throw new RuntimeException("Problem getting response from queue because the " +
                        "original request threw an exception: ", ee.getCause());
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported");
        }

    }
}
