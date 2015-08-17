/**
 * Copyright (c) 2015 Cloudant, Inc. All rights reserved.
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

/**
 * Code adapted from:
 *
 * http://stackoverflow.com/questions/4738510/retry-task-framework
 * http://fahdshariff.blogspot.co.uk/2009/08/retrying-operations-in-java.html
 */

package com.cloudant.common;

import com.google.common.base.Preconditions;

import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RetriableTask<T> implements Callable<T> {

    private final static String LOG_TAG = "RetriableTask";
    private final static Logger logger = Logger.getLogger(RetriableTask.class.getCanonicalName());

    private final Callable<T> task;
    public static final int DEFAULT_NUMBER_OF_RETRIES = 3;
    public static final long DEFAULT_WAIT_TIME = 1000;

    private int totalRetries; // total number of tries
    private int triesRemaining; // number left
    private long timeToWait; // wait interval

    public RetriableTask(Callable<T> task) {
        this(DEFAULT_NUMBER_OF_RETRIES, DEFAULT_WAIT_TIME, task);
        Preconditions.checkNotNull(task, "Task must not be null");
    }

    public RetriableTask(int totalRetries, long timeToWait, Callable<T> task) {
        this.totalRetries = totalRetries;
        this.triesRemaining = totalRetries;
        this.timeToWait = timeToWait;
        this.task = task;
    }

    public int getTotalRetries() {
        return this.totalRetries;
    }

    public int getTriesRemaining() {
        return this.triesRemaining;
    }

    @Override
    public T call() throws Exception {
        while (true) {
            try {
                logger.fine("Retry #: " + triesRemaining + "," + task.toString());
                T t = task.call();
                if(triesRemaining < 3) {
                    logger.fine("Success after retry: " + triesRemaining + ", " + task.toString());
                }
                return t;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                triesRemaining--;
                if (triesRemaining == 0) {
                    throw new RetryException(totalRetries +
                            " attempts to retry failed at " + timeToWait +
                            "ms interval", e);
                }
                logger.log(Level.FINE,"Retry later: " + triesRemaining + ", " + task.toString(), e);
                Thread.sleep(timeToWait);
            }
        }
    }
}
