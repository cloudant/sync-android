package com.cloudant.sync.notifications;

import com.cloudant.sync.replication.ErrorInfo;
import com.cloudant.sync.replication.Replicator;

/**
 * <p>Event posted when a state transition to ERROR is completed.</p>
 *
 * <p>Errors may include things such as:</p>
 *
 * <ul>
 *      <li>incorrect credentials</li>
 *      <li>network connection unavailable</li>
 * </ul>
 *
 */
public class ReplicationErrored {

    public ReplicationErrored(Replicator replicator, ErrorInfo errorInfo) {
        this.replicator = replicator;
        this.errorInfo = errorInfo;
    }

    /** 
     * The {@code Replicator} issuing the event
     */
    public final Replicator replicator;

    /** 
     * Error information about the error that occurred
     */
    public final ErrorInfo errorInfo;
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof ReplicationErrored) {
            ReplicationErrored re = (ReplicationErrored)other;
            return this.replicator == re.replicator && this.errorInfo == re.errorInfo;
        }
        return false;
    }

}
