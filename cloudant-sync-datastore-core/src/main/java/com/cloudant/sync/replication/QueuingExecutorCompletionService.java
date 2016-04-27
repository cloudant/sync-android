/*
 * Copyright (c) 2016 IBM Corp. All rights reserved.
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

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by tomblench on 23/02/16.
 */

/**
 * This class extends the ExecutorCompletionService using a Queue of requests to operate a policy of
 * submitting a new request for execution after a previous request has completed. The number of
 * concurrent requests allowed can be specified and that number of jobs will be submitted on
 * construction after which the one complete, one submitted policy applies. This has the
 * advantage of terminating submission of requests for execution if responses stop being retrieved
 * from the CompletionService.
 *
 * @param <Q> reQuest type
 * @param <R> Response type
 *
 * @api_private
 */
public abstract class QueuingExecutorCompletionService<Q, R> extends ExecutorCompletionService<R> {

    private final Queue<Q> requests;
    // A count of the number of requests that have been submitted for execution, but have either
    // not yet executed or been retrieved from the CompletionService. Use of this object within this
    // class should be synchronized.
    private final AtomicInteger requestsOutstanding = new AtomicInteger();

    /**
     * @param executorService    the executor service to execute the requests
     * @param requests           the Queue of requests to execute
     * @param concurrentRequests the number of concurrent requests
     */
    public QueuingExecutorCompletionService(ExecutorService executorService,
                                            final Queue<Q> requests,
                                            int concurrentRequests) {
        super(executorService);
        this.requests = requests;
        // We submit the first n jobs, corresponding to the number of concurrentRequests
        for (int n = 0; n < concurrentRequests; n++) {
            submitRequestInternal();
        }
        // Subsequent jobs are requested by completion
    }

    /**
     * @return true if requests have been submitted that have not yet been retrieved from the
     * CompletionService
     */
    public boolean hasRequestsOutstanding() {
        synchronized (requestsOutstanding) {
            return requestsOutstanding.get() != 0;
        }
    }

    @Override
    public Future<R> poll() {
        Future<R> result = super.poll();
        return submitNewOnCompletion(result);
    }

    @Override
    public Future<R> poll(long timeout, TimeUnit unit) throws
            InterruptedException {
        Future<R> result = super.poll(timeout, unit);
        return submitNewOnCompletion(result);
    }

    @Override
    public Future<R> take() throws InterruptedException {
        Future<R> result = super.take();
        return submitNewOnCompletion(result);
    }

    private Future<R> submitNewOnCompletion(Future<R> f) {
        if (f != null) {
            // We lock on the requestsOutstanding so that both the decrement and (potential)
            // increment happen before any subsequent check of the value of requestsOutstanding.
            // This avoids any issues where a call in-between the decrement and increment could
            // have returned false for a short duration while the next request was submitted.
            synchronized (requestsOutstanding) {
                requestsOutstanding.decrementAndGet();
                submitRequestInternal();
            }
        }
        return f;
    }

    private void submitRequestInternal() {
        final Q request;
        // We need to synchronize this poll, so that multiple threads performing the next submit
        // do not duplicate requests.
        synchronized (requests) {
            request = requests.poll();
        }
        if (request != null) {
            submit(new Callable<R>() {
                @Override
                public R call() throws Exception {
                    return executeRequest(request);
                }
            });
            synchronized (requestsOutstanding) {
                requestsOutstanding.incrementAndGet();
            }
        }
    }

    /**
     * Subclasses should override this method with the desired execution method, which will
     * ultimately be wrapped in a Callable for submission to the ExecutorCompletionService.
     *
     * @param request
     * @return response
     */
    public abstract R executeRequest(Q request);

}
