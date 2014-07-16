package com.cloudant.sync.replication;

class ReplicationStrategyErrored {

    protected ReplicationStrategyErrored(ReplicationStrategy replicationStrategy, ErrorInfo errorInfo) {
        this.replicationStrategy = replicationStrategy;
        this.errorInfo = errorInfo;
    }

    protected final ReplicationStrategy replicationStrategy;
    protected final ErrorInfo errorInfo;
    
}
