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
