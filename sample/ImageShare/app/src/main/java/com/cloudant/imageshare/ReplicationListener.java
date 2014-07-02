package com.cloudant.imageshare;

import com.cloudant.sync.notifications.ReplicationCompleted;
import com.cloudant.sync.notifications.ReplicationErrored;
import com.cloudant.sync.replication.ErrorInfo;
import com.google.common.eventbus.Subscribe;

import java.util.concurrent.CountDownLatch;

/**
 * Created by pettyurin on 7/2/14.
 */
public class ReplicationListener {

    private final CountDownLatch latch;
    public ErrorInfo error = null;

    ReplicationListener(CountDownLatch latch) {
        this.latch = latch;
    }

    @Subscribe
    public void complete(ReplicationCompleted event) {
        latch.countDown();
    }

    @Subscribe
    public void error(ReplicationErrored event) {
        this.error = event.errorInfo;
        latch.countDown();
    }
}
