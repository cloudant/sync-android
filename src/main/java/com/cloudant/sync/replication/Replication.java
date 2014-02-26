package com.cloudant.sync.replication;

abstract class Replication {

    public abstract String getName();

    public abstract ReplicationStrategy createReplicationStrategy();
}
