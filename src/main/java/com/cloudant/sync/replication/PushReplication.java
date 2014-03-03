package com.cloudant.sync.replication;

import com.cloudant.mazha.CouchConfig;
import com.cloudant.sync.datastore.Datastore;

import java.net.URI;

public class PushReplication extends Replication {

    public URI target;
    public Datastore source;

    @Override
    public String getName() {
        return String.format("%s <-- %s ", target, source.getDatastoreName());
    }

    public String getDbName() {
        return this.extractDatabaseName(this.target);
    }

    public CouchConfig getCouchConfig() {
        return this.createCouchConfig(this.target, this.username, this.password);
    }

    @Override
    public ReplicationStrategy createReplicationStrategy() {
        return new BasicPushStrategy(this);
    }

}
