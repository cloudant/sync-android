package com.cloudant.sync.replication;

import com.cloudant.mazha.CouchConfig;
import com.cloudant.sync.datastore.Datastore;

import java.net.URI;

public class PullReplication extends Replication {

    public URI source;
    public Datastore target;
    public Filter filter;

    @Override
    public String getName() {
        if(filter == null) {
            return String.format("%s <-- %s ", target.getDatastoreName(), source);
        } else {
            return String.format("%s <-- %s (%s)", target.getDatastoreName(), source, filter.name);
        }
    }

    public String getDbName() {
        return this.extractDatabaseName(this.source);
    }

    public CouchConfig getCouchConfig() {
        return this.createCouchConfig(this.source, this.username, this.password);
    }

    @Override
    public ReplicationStrategy createReplicationStrategy() {
        return new BasicPullStrategy(this, null, null);
    }
}
