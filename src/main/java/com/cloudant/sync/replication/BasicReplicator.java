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

package com.cloudant.sync.replication;

import com.cloudant.mazha.CouchClient;
import com.cloudant.mazha.CouchConfig;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.notifications.ReplicationCompleted;
import com.cloudant.sync.notifications.ReplicationErrored;
import com.cloudant.common.Log;
import com.google.common.base.Preconditions;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import java.net.URI;

class BasicReplicator implements Replicator {

    private final static String LOG_TAG = "BasicReplicator";

    protected enum ReplicationType {
        PULL,
        PUSH
    }
    final protected ReplicationType replicationType;

    protected final DatastoreExtended datastore;
    protected final CouchClient couchClient;
    protected Thread strategyThread;
    protected ReplicationStrategy strategy;

    // Writes need synchronising.
    private State state = null;

    private String name;
    
    private final EventBus eventBus = new EventBus();

    public BasicReplicator(URI source, Datastore target) {
        this.replicationType = ReplicationType.PULL;

        this.datastore = (DatastoreExtended) target;
        this.state = State.PENDING;

        CouchConfig couchConfig = ReplicatorURIUtils.extractCouchConfig(source);
        String databaseName = ReplicatorURIUtils.extractDatabaseName(source);
        this.couchClient = new CouchClient(couchConfig, databaseName);

        name = String.format("%s <-- %s", target.getDatastoreName(), databaseName);
    }

    public BasicReplicator(Datastore source, URI target) {
        this.replicationType = ReplicationType.PUSH;

        this.datastore = (DatastoreExtended) source;
        this.state = State.PENDING;

        CouchConfig couchConfig = ReplicatorURIUtils.extractCouchConfig(target);
        String databaseName = ReplicatorURIUtils.extractDatabaseName(target);
        this.couchClient = new CouchClient(couchConfig, databaseName);

        name = String.format("%s --> %s", source.getDatastoreName(), databaseName);
    }

    protected ReplicationStrategy getReplicationStrategy() {
        switch (this.replicationType) {
            case PULL:
                return new BasicPullStrategy(
                        new CouchClientWrapper(couchClient),
                        this.datastore,
                        name
                );
            case PUSH:
                return new BasicPushStrategy(
                        new CouchClientWrapper(couchClient),
                        this.datastore,
                        name
                );
            default:
                return null;
        }
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
                Preconditions.checkArgument(
                        this.strategy == null || this.strategy.isReplicationTerminated(),
                        "strategy must be null or not running"
                );

                this.strategy = getReplicationStrategy();
                this.strategy.getEventBus().register(this);
                this.strategyThread = new Thread(this.strategy);
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
        ReplicationCompleted rcUs = new ReplicationCompleted(this);
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
        Preconditions.checkArgument(this.state == State.STARTED || this.state == State.STOPPING,
                "Replicate state must be STARTED or STOPPING.");
    }

    /**
     * @return the eventBus
     */
    public EventBus getEventBus() {
        return eventBus;
    }
}
