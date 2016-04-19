package com.cloudant.sync.replication;

import java.util.Timer;
import java.util.TimerTask;

/**
 * This replication policy invokes replications at regular intervals.
 * This is not suitable for use on Android because the timer task is likely to be killed
 * by the operating system and this does not handle the case where the device is asleep.
 *
 * @api_public
 */
public class IntervalTimerReplicationPolicyManager extends ReplicationPolicyManager {

    private int intervalInSeconds;
    private Timer timer;

    public IntervalTimerReplicationPolicyManager(int intervalInSeconds) {
        this.intervalInSeconds = intervalInSeconds;
        timer = new Timer();
    }

    public void start() {
        // Schedule first replication immediately, then every intervalInSeconds seconds.
        timer.schedule(new IntervalTimerTask(), 0, intervalInSeconds * 1000);
    }

    public void stop() {
        stopReplications();
        timer.cancel();
    }

    private class IntervalTimerTask extends TimerTask {

        public void run() {
            startReplications();
        }
    }
}
