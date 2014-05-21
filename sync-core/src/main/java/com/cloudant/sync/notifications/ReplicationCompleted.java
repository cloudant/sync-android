package com.cloudant.sync.notifications;

import com.cloudant.sync.replication.Replicator;

/**
 * <p>Event posted when a state transition to COMPLETE or STOPPED is
 * completed.</p>
 *
 * <p>{@code complete} may be called from one of the replicator's
 * worker threads.</p>
 *
 * <p>Continuous replications (when implemented) will never complete.</p>
 *
 */
public class ReplicationCompleted {

    public ReplicationCompleted(Replicator replicator) {
        this.replicator = replicator;
    }
    
    /** 
     * The {@code Replicator} issuing the event
     */
    public final Replicator replicator;
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof ReplicationCompleted) {
            ReplicationCompleted rc = (ReplicationCompleted)other;
            return this.replicator == rc.replicator;
        }
        return false;
    }

}
