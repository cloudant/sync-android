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
