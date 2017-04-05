/*
 * Copyright © 2017 IBM Corp. All rights reserved.
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

/**
 * Interface with methods to receive callbacks for different termination states
 * of the replications configured by a replication policy.
 */
public interface PolicyReplicationsCompletedListener {
    /**
     * Gets called back when all replicators completed, whether due to normal completion or
     * completion due to error.
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

    /**
     * A simple {@link PolicyReplicationsCompletedListener}
     * to save clients having to override every method if they are only interested in a subset of
     * the events.
     */
    class SimpleListener implements PolicyReplicationsCompletedListener {
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
