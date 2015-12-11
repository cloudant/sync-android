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

import com.google.common.eventbus.EventBus;

/**
 * <p>Manages replication between a local
 * {@link com.cloudant.sync.datastore.Datastore} and a remote Cloudant or
 * CouchDB database.</p>
 *
 * <p>Create instances using the {@link ReplicatorBuilder} class.</p>
 *
 * <p>The replicator is thread safe.</p>
 */
public interface Replicator {

    /**
     * <p>Starts a replication.</p>
     *
     * <p>The replication will continue until the
     * replication is caught up with the source database; that is, until
     * there are no current changes to replicate.</p>
     *
     * <p>{@code start} can be called from any thread. It spawns background
     * threads for its work. The methods on the ReplicationListener
     * may be called from the background threads; any work that needs
     * to be on the main thread will need to be explicitly executed
     * on that thread.</p>
     *
     * <p>{@code start} will spawn a manager thread for the replication and
     * immediately return.</p>
     *
     * <p>A given replicator instance can be reused:</p>
     *
     * <ul>
     *  <li>If you call start when in {@link Replicator.State#PENDING},
     *   replication will start.</li>
     *  <li>In {@link Replicator.State#STARTED}, nothing changes.</li>
     *  <li>In {@link Replicator.State#STOPPING}, nothing changes.</li>
     *  <li>In {@link Replicator.State#ERROR}, the replication will restart.
     *   It's likely its going to error again, however, depending on whether
     *   the error is transient or not.</li>
     *  <li>In {@link Replicator.State#STOPPED} or
     *   {@link Replicator.State#COMPLETE}, the replication will start a
     *   second or further time.</li>
     * </ul>
     */
    void start();

    /**
     * <p>Stops an in-progress replication.</p>
     *
     * <p>Already replicated changes will remain
     * in the datastore database.</p>
     *
     * <p>{@code stop} can be called from any thread. It will initiate a
     * shutdown process and return immediately.</p>
     *
     * <p>The shutdown process may take time as we need to wait for in-flight
     * network requests to complete before background threads can be safely
     * stopped. However, no modifications to the database will be made
     * after {@code stop} is called, including checkpoint related
     * operations.</p>
     *
     * <p>Consumers should check
     * {@link com.cloudant.sync.replication.Replicator#getState()} if they need
     * to know when the replicator has fully stopped. After {@code stop} is
     * called, the replicator will be in the {@link Replicator.State#STOPPING}
     * state while operations complete and will move to the
     * {@link Replicator.State#STOPPED} state when the replicator has fully
     * shutdown.</p>
     *
     * <p>It is also possible the replicator moves to the
     * {@link Replicator.State#ERROR} state if an error happened during the
     * shutdown process.</p>
     *
     * <p>If the replicator is in the {@link Replicator.State#PENDING} state,
     * it will immediately move to the {@link Replicator.State#STOPPED} state.
     * </p>
     */
    void stop();

    /**
     * <p>Returns the {@link Replicator.State} this replicator is in.</p>
     *
     * <p>{@code getState} may be called from any thread.</p>
     *
     * <p>In all states other than {@link Replicator.State#STARTED} and
     * {@link Replicator.State#STOPPING}, the replicator object
     * is idle with no background threads.</p>
     *
     * @return the {@link Replicator.State} this replicator is in
     */
    State getState();

    /**
     * <p>Describes the state of a {@link Replicator} at a given moment.</p>
     */
    enum State {
        /**
         * The replicator is initialised and is ready to start.
         */
        PENDING,
        /**
         * A replication is in progress.
         */
        STARTED,
        /**
         * The last replication was stopped using
         * {@link com.cloudant.sync.replication.Replicator#stop()}.
         */
        STOPPED,
        /**
         * {@link com.cloudant.sync.replication.Replicator#stop()} has
         * been called and the replicator is stopping its worker threads.
         */
        STOPPING,
        /**
         * The last replication successfully completed.
         */
        COMPLETE,
        /**
         * The last replication completed in error.
         */
        ERROR
    }

    /**
     * <p>Returns an EventBus that clients can use to listen for state changes
     * for this replicator.</p>
     *
     * <p>The replicator raises the following events:</p>
     *
     * <ul>
     *     <li>{@link com.cloudant.sync.notifications.ReplicationErrored} if
     *     there is an error during replication.</li>
     *     <li>{@link com.cloudant.sync.notifications.ReplicationCompleted}
     *     when the replication is completed, unless there is an error.</li>
     * </ul>
     *
     * @return EventBus object.
     */
    EventBus getEventBus();

    int getId();
}

