package com.cloudant.sync.replication;

import com.cloudant.sync.notifications.ReplicationCompleted;
import com.cloudant.sync.notifications.ReplicationErrored;
import com.google.common.eventbus.Subscribe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class ReplicationPolicyManager {

    private List<Replicator> replicators;
    private ReplicationListener replicationListener;
    private ReplicationsCompletedListener mReplicationsCompletedListener;

    public interface ReplicationsCompletedListener {
        void allReplicationsCompleted();

        void replicationCompleted(int id);

        void replicationErrored(int id);
    }

    private class ReplicationListener {

        Set<Replicator> replicatorsInProgress;

        ReplicationListener() {
            replicatorsInProgress = Collections.synchronizedSet(new HashSet<Replicator>());
        }

        void add(Replicator replicator) {
            replicatorsInProgress.add(replicator);
        }

        void remove(Replicator replicator) {
            if (replicatorsInProgress.remove(replicator)) {
                replicator.getEventBus().unregister(this);
            }
        }

        boolean inProgress(Replicator replicator) {
            return replicatorsInProgress.contains(replicator);
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
        replicators = new ArrayList<Replicator>();
        replicationListener = new ReplicationListener();
    }

    public abstract void start();

    public abstract void stop();

    protected void startReplications() {
        for (Replicator replicator : replicators) {
            if (!replicationListener.inProgress(replicator)) {
                replicationListener.add(replicator);
                replicator.getEventBus().register(replicationListener);

                replicator.start();
            }
        }
    }

    protected void stopReplications() {
        for (Replicator replicator : replicators) {
            replicationListener.remove(replicator);
            replicator.stop();
        }
    }

    public void addReplicators(Replicator... replicators) {
        if (this.replicators == null) {
            this.replicators = new ArrayList<Replicator>();
        }
        this.replicators.addAll(Arrays.asList(replicators));
    }

    public void setReplicationsCompletedListener(ReplicationsCompletedListener listener) {
        mReplicationsCompletedListener = listener;
    }
}
