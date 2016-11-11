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

import com.cloudant.sync.event.Subscribe;
import com.cloudant.sync.event.notifications.ReplicationCompleted;
import com.cloudant.sync.event.notifications.ReplicationErrored;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @api_public
 */
public class ReplicationPolicyManager {

    private final List<Replicator> replicators = new ArrayList<Replicator>();
    private ReplicationListener replicationListener;
    private ReplicationsCompletedListener mReplicationsCompletedListener;

    public interface ReplicationsCompletedListener {
        void allReplicationsCompleted();

        void replicationCompleted(int id);

        void replicationErrored(int id);
    }

    /**
     * This class is not intended as API, it is public for EventBus access only.
     * API consumers should not call these methods. See #setReplicationsCompletedListener for more
     * information on replication lifecycle events.
     * @api_private
     */
    public class ReplicationListener {

        Set<Replicator> replicatorsInProgress;

        ReplicationListener() {
            replicatorsInProgress = new HashSet<Replicator>();
        }

        void add(Replicator replicator) {
            synchronized (replicatorsInProgress) {
                replicatorsInProgress.add(replicator);
            }
        }

        void remove(Replicator replicator) {
            synchronized (replicatorsInProgress) {
                if (replicatorsInProgress.remove(replicator)) {
                    replicator.getEventBus().unregister(this);
                }
            }
        }

        boolean inProgress(Replicator replicator) {
            synchronized (replicatorsInProgress) {
                return replicatorsInProgress.contains(replicator);
            }
        }

        @Subscribe
        public void complete(ReplicationCompleted event) {
            finishedReplication(event.replicator);
            if (mReplicationsCompletedListener != null) {
                mReplicationsCompletedListener.replicationCompleted(event.replicator.getId());
            }
        }

        @Subscribe
        public void error(ReplicationErrored event) {
            finishedReplication(event.replicator);
            if (mReplicationsCompletedListener != null) {
                mReplicationsCompletedListener.replicationErrored(event.replicator.getId());
            }
        }

        public void finishedReplication(Replicator replicator) {
            synchronized (replicatorsInProgress) {
                remove(replicator);
                if (replicatorsInProgress.size() == 0 && mReplicationsCompletedListener != null) {
                    mReplicationsCompletedListener.allReplicationsCompleted();
                }
            }
        }
    }

    public ReplicationPolicyManager() {
        replicationListener = new ReplicationListener();
    }

    protected void startReplications() {
        synchronized (replicators) {
            for (Replicator replicator : replicators) {
                if (!replicationListener.inProgress(replicator)) {
                    replicationListener.add(replicator);
                    replicator.getEventBus().register(replicationListener);

                    replicator.start();
                }
            }
        }
    }

    protected void stopReplications() {
        synchronized (replicators) {
            for (Replicator replicator : replicators) {
                replicationListener.remove(replicator);
                replicator.stop();
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
        mReplicationsCompletedListener = listener;
    }
}
