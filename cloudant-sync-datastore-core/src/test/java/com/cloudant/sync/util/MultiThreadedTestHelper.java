/*
 * Copyright (C) 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A simple test abstraction that executes a callable from the specified number of threads as close
 * to simultaneously as possible. The callable is wrapped in a runnable and blocked behind a latch.
 * The latch counts down as each thread is ready and then there is a final countdown to unlatch all
 * the threads at once.
 * <p>
 * To use this class extend it specifying the number of threads and implementing getCallable.
 * </p>
 * <p>
 * The default assertions include checking that all the threads executed and there were no
 * exceptions and that the number of results were correct. Additional assertions are added by
 * implementing the doAssertions method.
 * </p>
 * <p>
 * Call run to start the test.
 * </p>
 *
 * @param <T> the type of result from the Callable
 */
public abstract class MultiThreadedTestHelper<T> {

    private final int numberOfThreads;
    private final List<Thread> threads;
    protected final List<T> results;
    protected final List<Throwable> exceptions;
    final AtomicBoolean failedToExecuteAllThreads = new AtomicBoolean(false);
    final CountDownLatch latch;


    public MultiThreadedTestHelper(int numberOfThreads) {
        this.numberOfThreads = numberOfThreads;
        this.latch = new CountDownLatch(numberOfThreads + 1);
        this.threads = new ArrayList<Thread>(numberOfThreads);
        this.results = Collections.synchronizedList(new ArrayList<T>(numberOfThreads));
        this.exceptions = Collections.synchronizedList(new ArrayList<Throwable>(numberOfThreads));
        for (int i = 0; i < numberOfThreads; i++) {
            final Callable<T> callable = getCallable();
            threads.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        latch.countDown();
                        // Wait for all the threads to be running before proceeding with the calls
                        // we want them to happen as much at same time as possible.
                        if (latch.await(20, TimeUnit.SECONDS)) {
                            results.add(callable.call());
                            return;
                        }
                    } catch (Throwable t) {
                        exceptions.add(t);
                    }
                    // If we reached this we didn't execute the thread correctly
                    failedToExecuteAllThreads.set(true);
                }
            }));
        }
    }

    public void run() throws Exception {
        for (Thread thread : threads) {
            thread.start();
        }
        // Release many threads simultaneously
        latch.countDown();
        // Wait for all threads to finish
        for (Thread thread : threads) {
            thread.join();
        }
        defaultAssertions();
        doAssertions();
    }

    protected void defaultAssertions() throws Exception {
        assertFalse("All threads should have executed", failedToExecuteAllThreads.get());
        assertTrue("There should be no exceptions.", exceptions.isEmpty());
        assertEquals("There should be the correct number of results",
                numberOfThreads, results.size());
    }

    protected abstract void doAssertions() throws Exception;

    protected abstract Callable<T> getCallable();
}
