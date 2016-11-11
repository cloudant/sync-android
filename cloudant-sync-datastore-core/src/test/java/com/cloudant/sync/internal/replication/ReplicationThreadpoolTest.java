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

package com.cloudant.sync.internal.replication;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ReplicationThreadpoolTest {

    /**
     * This test validates that the type of ThreadPoolExecutor we use for GetRevisionTaskThreaded
     * behaves as we expect in terms of generation and expiry of threads.
     *
     * @throws Exception
     */
    @Test
    public void threadpoolTest() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        int threads = 4;
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(threads, threads, 1,
                TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        // Allowing core threads to timeout means we don't keep threads in memory except when
        // tasks are running or for the configured amount of time afterwards.
        tpe.allowCoreThreadTimeOut(true);

        List<Future<?>> futures = new ArrayList<Future<?>>();

        for (int i = 1; i < threads; i++) {
            futures.add(tpe.submit(new LatchedRunnable(latch)));
            Assert.assertEquals("The threadpool count should increase with tasks up to threads",
                    i, tpe.getPoolSize());
        }
        futures.add(tpe.submit(new LatchedRunnable(latch)));
        Assert.assertEquals("The threadpool count should be capped", threads, tpe.getPoolSize());

        // Now countdown the latch to release the threads
        latch.countDown();
        // Ensure they are all completed
        for (Future<?> f : futures) {
            f.get();
        }

        // Sleep to allow the threads to expire (1 second expiry, but allow 2)
        TimeUnit.SECONDS.sleep(2);
        // Validate no threads remain
        Assert.assertEquals("The threadpool should be empty", 0, tpe.getPoolSize());
    }

    /*
     * Simple test to execute a method which adds numbers over a large number of threads
     */
    @Test
    public void addServiceTest() {
        Queue<AddRequest> requests = new LinkedBlockingQueue<AddRequest>();
        int nRequests = 10000;
        for (int i = 0; i < nRequests; i++) {
            AddRequest request = new AddRequest();
            request.x = i;
            request.y = i * 2;
            requests.add(request);
        }
        int threads = 50;
        final AtomicInteger nResponses = new AtomicInteger();
        QueuingExecutorCompletionService<AddRequest, AddResponse> service =
                new QueuingExecutorCompletionService<AddRequest, AddResponse>(Executors
                        .newFixedThreadPool(threads), requests, threads) {
                    @Override
                    public AddResponse executeRequest(AddRequest request) {
                        AddResponse response = new AddResponse();
                        response.x = request.x;
                        response.y = request.y;
                        response.z = request.x + request.y;
                        return response;
                    }
                };
        try {
            Future<AddResponse> response;
            while (service.hasRequestsOutstanding() && (response = service.take()) != null) {
                Assert.assertEquals(response.get().x + response.get().y, response.get().z);
                nResponses.incrementAndGet();
            }
        } catch (InterruptedException ie) {
            Assert.fail(ie.getMessage());
        } catch (ExecutionException ee) {
            Assert.fail(ee.getMessage());
        }
        Assert.assertEquals(nRequests, nResponses.get());
        Assert.assertFalse(service.hasRequestsOutstanding());
    }

    /*
     * Only consume half of the results
     * - we use a threadpool which times out quickly so we can assert that no threads are running
     *   after only consuming half of the results
     * - assert that the correct number of requests have been de-queued
     */
    @Test
    public void stopConsumingTest() throws Exception {
        Queue<AddRequest> requests = new LinkedBlockingQueue<AddRequest>();
        int nRequests = 10000;
        for (int i = 0; i < nRequests; i++) {
            AddRequest request = new AddRequest();
            request.x = i;
            request.y = i * 2;
            requests.add(request);
        }
        int threads = 50;
        final AtomicInteger nResponses = new AtomicInteger();
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(threads, threads, 1,
                TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        // Allowing core threads to timeout means we don't keep threads in memory except when
        // tasks are running or for the configured amount of time afterwards.
        tpe.allowCoreThreadTimeOut(true);

        QueuingExecutorCompletionService<AddRequest, AddResponse> service =
                new QueuingExecutorCompletionService<AddRequest, AddResponse>(tpe, requests,
                        threads) {
                    @Override
                    public AddResponse executeRequest(AddRequest request) {
                        AddResponse response = new AddResponse();
                        response.x = request.x;
                        response.y = request.y;
                        response.z = request.x + request.y;
                        return response;
                    }
                };
        try {
            for (int i = 0; i < nRequests / 2; i++) {
                Future<AddResponse> response = service.take();
                Assert.assertEquals(response.get().x + response.get().y, response.get().z);
                nResponses.incrementAndGet();
            }
        } catch (InterruptedException ie) {
            Assert.fail(ie.getMessage());
        } catch (ExecutionException ee) {
            Assert.fail(ee.getMessage());
        }
        // Sleep to allow the threads to expire (1 second expiry, but allow 2)
        TimeUnit.SECONDS.sleep(2);
        // Validate no threads remain
        Assert.assertEquals("The threadpool should be empty", 0, tpe.getPoolSize());

        // we should have half of the requests left in the request queue, minus one thread-pool's
        // worth which was 'in flight'
        Assert.assertEquals(nRequests / 2 - threads, requests.size());
    }

    /*
     * Test for run() throwing an exception:
     * - the result of take() throws an ExecutionException
     * - the rest of the results can still be processed
     * - we get the correct number of results (excluding the one which threw)
     */
    @Test
    public void runnableThrowsTest() {
        Queue<AddRequest> requests = new LinkedBlockingQueue<AddRequest>();
        int nRequests = 1000;
        for (int i = 0; i < nRequests; i++) {
            AddRequest request = new AddRequest();
            request.x = i;
            request.y = i * 2;
            requests.add(request);
        }
        int threads = 50;
        final int evilNumber = 666; // throw exception on this one
        final String evilMessage = "It went bang";
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        final QueuingExecutorCompletionService<AddRequest, AddResponse> service =
                new QueuingExecutorCompletionService<AddRequest, AddResponse>(executor, requests,
                        threads) {
                    @Override
                    public AddResponse executeRequest(AddRequest request) {
                        AddResponse response = new AddResponse();
                        response.x = request.x;
                        response.y = request.y;
                        if (response.x == evilNumber) {
                            throw new RuntimeException(evilMessage);
                        }
                        response.z = request.x + request.y;
                        return response;
                    }
                };

        int nResponses = 0;
        try {
            Future<AddResponse> response;
            while (service.hasRequestsOutstanding() && (response = service.take()) != null) {
                try {
                    Assert.assertEquals(response.get().x + response.get().y, response.get().z);
                    nResponses++;
                } catch (ExecutionException ee) {
                    if (ee.getMessage().contains(evilMessage)) {
                        // we got the one with the exception, now drain the queue again and
                        // ensure everything else processes OK
                        System.out.println("Caught ex " + ee);
                    } else {
                        Assert.fail(ee.getMessage());
                    }
                }
            }
        } catch (InterruptedException ie) {
            Assert.fail(ie.getMessage());
        }
        Assert.assertFalse(service.hasRequestsOutstanding());
        Assert.assertEquals(nRequests - 1, nResponses);
    }

    // simple example of a request: x+y = z
    private class AddRequest {
        int x;
        int y;
    }

    // simple example of a response: x+y = z
    private class AddResponse {
        int x;
        int y;
        int z;
    }

    private class LatchedRunnable implements Runnable {

        private final CountDownLatch latch;

        LatchedRunnable(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
