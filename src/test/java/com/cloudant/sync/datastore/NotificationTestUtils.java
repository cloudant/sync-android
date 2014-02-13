package com.cloudant.sync.datastore;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class NotificationTestUtils {

    static int timeoutSecs = 2;

    static boolean waitForSignal(CountDownLatch b) {
        try {
            return b.await(timeoutSecs, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            return false;
        }
    }

}
