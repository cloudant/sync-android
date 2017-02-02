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

import com.cloudant.sync.internal.replication.ReplicationListener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Manages the coordination of starting/stopping all replicators used in a replication policy, and
 * provides monitoring of when replications have completed.
 */
public class ReplicationPolicyManager {

    private final List<Replicator> replicators = new ArrayList<Replicator>();
    private final ReplicationListener replicationListener = new ReplicationListener();

    /**
     * Interface with methods to receive callbacks for different termination states
     * of the replications configured by a replication policy.
     */
    public interface ReplicationsCompletedListener {
        /**
         * Gets called back when all replicators completed.
         */
        void allReplicationsCompleted();
        /**
         * Gets called back when a replication completed.
         * @param id the replication ID
         */
        void replicationCompleted(int id);
        /**
         * Gets called back when a replication has an error.
         * @param id the replication ID
         */
        void replicationErrored(int id);
    }

    protected void startReplications() {
        synchronized (replicators) {
            for (Replicator replicator : replicators) {
                if (replicationListener.add(replicator)) {
                    replicator.start();
                }
            }
        }
    }

    protected void stopReplications() {
        synchronized (replicators) {
            for (Replicator replicator : replicators) {
                if (replicationListener.remove(replicator)) {
                    replicator.stop();
                }
            }
        }
    }

    public void addReplicators(Replicator... replicators) {
        synchronized (this.replicators) {
            this.replicators.addAll(Arrays.asList(replicators));
        }
    }

    /**
     * Interested parties wishing to receive replication lifecycle events should call
     * this method with a class implementing the {@link ReplicationsCompletedListener} interface.
     *
     * @param listener A class implementing appropriate callback methods for replication lifecycle
     *                 events
     */
    public void setReplicationsCompletedListener(ReplicationsCompletedListener listener) {
        replicationListener.setReplicationsCompletedListener(listener);
    }

    /**
     * A simple {@link ReplicationsCompletedListener}
     * to save clients having to override every method if they are only interested in a subset of
     * the events.
     */
    public static class SimpleReplicationsCompletedListener implements
            ReplicationsCompletedListener {
        @Override
        public void allReplicationsCompleted() {
        }

        @Override
        public void replicationCompleted(int id) {
        }

        @Override
        public void replicationErrored(int id) {
        }
    }
}
