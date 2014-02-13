package com.cloudant.sync.replication;

public class ReplicationStrategyErrored {

    public ReplicationStrategyErrored(ReplicationStrategy replicationStrategy, ErrorInfo errorInfo) {
        this.replicationStrategy = replicationStrategy;
        this.errorInfo = errorInfo;
    }
    
    public final ReplicationStrategy replicationStrategy;
    public final ErrorInfo errorInfo;
    
}
