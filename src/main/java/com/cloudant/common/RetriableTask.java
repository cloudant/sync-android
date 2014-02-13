/**
 * Code adapted from:
 *
 * http://stackoverflow.com/questions/4738510/retry-task-framework
 * http://fahdshariff.blogspot.co.uk/2009/08/retrying-operations-in-java.html
 */

package com.cloudant.common;

import com.google.common.base.Preconditions;

import java.util.concurrent.Callable;

public class RetriableTask<T> implements Callable<T> {

    private final static String LOG_TAG = "RetriableTask";

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
                Log.d(LOG_TAG, "Retry #: " + triesRemaining + "," + task.toString());
                T t = task.call();
                if(triesRemaining < 3) {
                    Log.d(LOG_TAG, "Success after retry: " + triesRemaining + ", " + task.toString());
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
                Log.d(LOG_TAG, "Retry later: " + triesRemaining + ", " + task.toString(), e);
                Thread.sleep(timeToWait);
            }
        }
    }
}
