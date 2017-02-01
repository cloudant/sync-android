/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2015 Cloudant, Inc. All rights reserved.
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

/*
 * Code adapted from:
 *
 * http://stackoverflow.com/questions/4738510/retry-task-framework
 * http://fahdshariff.blogspot.co.uk/2009/08/retrying-operations-in-java.html
 */

package com.cloudant.sync.internal.common;

import com.cloudant.sync.internal.util.Misc;

import java.util.concurrent.Callable;
import java.util.logging.Logger;

public class RetriableTask<T> implements Callable<T> {

    private final static Logger logger = Logger.getLogger(RetriableTask.class.getCanonicalName());

    private final Callable<T> task;
    public static final int DEFAULT_TRIES = 3;
    public static final long DEFAULT_WAIT_TIME = 1000;

    private int totalTries; // total number of tries
    private int triesRemaining; // number left
    private long timeToWait; // wait interval

    public RetriableTask(Callable<T> task) {
        this(DEFAULT_TRIES, DEFAULT_WAIT_TIME, task);
        Misc.checkNotNull(task, "Task");
    }

    public RetriableTask(int totalTries, long timeToWait, Callable<T> task) {
        this.totalTries = totalTries;
        this.triesRemaining = totalTries;
        this.timeToWait = timeToWait;
        this.task = task;
    }

    public int getTotalTries() {
        return this.totalTries;
    }

    public int getTriesRemaining() {
        return this.triesRemaining;
    }

    @Override
    public T call() throws Exception {
        Exception lastException = null; // remember the exception we caught from the last try
        do {
            try {
                logger.fine(String.format("%d tries remaining for task %s", triesRemaining, task));
                T t = task.call();
                return t;
            } catch (RetryException re) {
                // if task.call() throws a RetryException this means we should exit early and not
                // attempt any more retries - so just re-throw the exception
                throw re;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                lastException = e;
                Thread.sleep(timeToWait);
            }
        } while (--triesRemaining > 0);
        // if we got here, we ran out of retries
        throw new RetryException(String.format("Failed after %d attempts for task %s",
                totalTries, task), lastException);
    }
}
