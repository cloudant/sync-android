/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
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

import com.cloudant.sync.event.EventBus;
import com.cloudant.sync.event.Subscribe;
import com.cloudant.sync.event.notifications.ReplicationCompleted;
import com.cloudant.sync.event.notifications.ReplicationErrored;
import com.cloudant.sync.replication.Replicator;
import com.cloudant.sync.internal.util.Misc;

import java.util.Locale;

/**
 * This class is not intended as API, it is public for EventBus access only.
 * @api_private
 */
public class ReplicatorImpl implements Replicator {

    public static final int NULL_ID = -1;
    protected Thread strategyThread;
    protected ReplicationStrategy strategy;
    protected int id = NULL_ID;

    // Writes need synchronising.
    private State state = null;

    private final EventBus eventBus = new EventBus();

    public ReplicatorImpl(ReplicationStrategy strategy) {
        this(strategy, NULL_ID);
    }

    public ReplicatorImpl(ReplicationStrategy strategy, int id) {
        this.strategy = strategy;
        this.id = id;
        this.state = State.PENDING;
    }

    @Override
    public synchronized void start() {
        switch (this.state) {
            case STARTED:
                break;  // do nothing, we're already started.
            case STOPPING:
                throw new IllegalStateException("The replicator is being shut down, " +
                        "can not be started now.");
            case PENDING:
            case COMPLETE:
            case STOPPED:
            case ERROR:
                // pending -> started: start replication for the first time
                // complete/stopped/error -> started: (re)start replication for nth time
                // we assume register() is idempotent
                this.strategy.getEventBus().register(this);
                String replicatorThreadName = String.format(Locale.ENGLISH,
                        "Replicator: %s - %s",
                        this.strategy.getClass().getSimpleName(),
                        this.strategy.getRemote());
                this.strategyThread = new Thread(this.strategy, replicatorThreadName);
                this.strategyThread.start();
                this.state = State.STARTED;
                break;
        }
    }

    @Override
    public synchronized void stop() {
        switch (this.state) {
            case PENDING:
                this.state = State.STOPPED;
                break;
            case STARTED:
                this.strategy.setCancel();
                this.state = State.STOPPING;
                break;
            default:
                // No operation when state is:
                // State.COMPLETE
                // State.ERROR
                // State.STOPPED
                // State.STOPPING
                // these states means the replication is either stopping or stopped already.
        }
    }

    @Override
    public State getState() {
        return this.state;
    }

    //
    // EventBus callbacks
    //

    @Subscribe
    public synchronized void complete(ReplicationStrategyCompleted rc) {
        this.assertRunningState();

        // Update the replicator state
        switch (this.state) {
            case STARTED:
                this.state = State.COMPLETE;
                break;
            case STOPPING:
                this.state = State.STOPPED;
                break;
            default:
                throw new IllegalStateException("The replicator should be either STARTED or " +
                        "STOPPING state.");
        }

        // Fill in new ReplicationCompleted event with pointer to us
        ReplicationCompleted rcUs = new ReplicationCompleted(this,
                rc.replicationStrategy.getDocumentCounter(),
                rc.replicationStrategy.getBatchCounter());
        eventBus.post(rcUs);        
    }

    @Subscribe
    public synchronized void error(ReplicationStrategyErrored re) {
        this.assertRunningState();

        this.state = State.ERROR;

        // Fill in new ReplicationErrored event with pointer to us
        ReplicationErrored reUs = new ReplicationErrored(this, re.errorInfo);
        eventBus.post(reUs);
    }

    /**
     * Working thread are running when state is either STARTED or STOPPING.
     */
    private void assertRunningState() {
        Misc.checkArgument(this.state == State.STARTED || this.state == State.STOPPING,
                "Replicate state must be STARTED or STOPPING.");
    }

    /**
     * @return the eventBus
     */
    public EventBus getEventBus() {
        return eventBus;
    }

    public int getId() {
        return id;
    }
}
