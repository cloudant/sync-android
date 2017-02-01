package com.cloudant.sync.internal.replication;

import com.cloudant.sync.event.Subscribe;
import com.cloudant.sync.event.notifications.ReplicationCompleted;
import com.cloudant.sync.event.notifications.ReplicationErrored;
import com.cloudant.sync.replication.ReplicationPolicyManager;
import com.cloudant.sync.replication.Replicator;

import java.util.HashSet;
import java.util.Set;

/**
 * This class is not intended as API, it is public for EventBus access only.
 * API consumers should not call these methods. Forwards events from the EventBus to a
 * {@link com.cloudant.sync.replication.ReplicationPolicyManager.ReplicationsCompletedListener}.
 */
public class ReplicationListener {

    private final Set<Replicator> replicatorsInProgress = new HashSet<Replicator>();
    private ReplicationPolicyManager.ReplicationsCompletedListener replicationsCompletedListener
            = null;

    public void setReplicationsCompletedListener(ReplicationPolicyManager
                                                         .ReplicationsCompletedListener
                                                         replicationsCompletedListener) {
        this.replicationsCompletedListener = replicationsCompletedListener;
    }

    public boolean add(Replicator replicator) {
        synchronized (replicatorsInProgress) {
            boolean added = replicatorsInProgress.add(replicator);
            if (added) {
                replicator.getEventBus().register(this);
            }
            return added;
        }
    }

    public boolean remove(Replicator replicator) {
        synchronized (replicatorsInProgress) {
            boolean removed = replicatorsInProgress.remove(replicator);
            if (removed) {
                replicator.getEventBus().unregister(this);
            }
            return removed;
        }
    }

    @Subscribe
    public void complete(ReplicationCompleted event) {
        finishedReplication(event.replicator);
        if (replicationsCompletedListener != null) {
            replicationsCompletedListener.replicationCompleted(event.replicator.getId());
        }
    }

    @Subscribe
    public void error(ReplicationErrored event) {
        finishedReplication(event.replicator);
        if (replicationsCompletedListener != null) {
            replicationsCompletedListener.replicationErrored(event.replicator.getId());
        }
    }

    public void finishedReplication(Replicator replicator) {
        synchronized (replicatorsInProgress) {
            remove(replicator);
            if (replicatorsInProgress.size() == 0 && replicationsCompletedListener != null) {
                replicationsCompletedListener.allReplicationsCompleted();
            }
        }
    }
}
