package com.cloudant.sync.replication;

/**
 * Interface with methods to receive callbacks for different termination states
 * of the replications configured by a replication policy.
 */
public interface PolicyReplicationsCompletedListener {
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
