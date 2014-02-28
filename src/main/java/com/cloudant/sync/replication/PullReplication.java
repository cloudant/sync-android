package com.cloudant.sync.replication;

import com.cloudant.mazha.CouchConfig;
import com.cloudant.sync.datastore.Datastore;
import com.google.common.base.Preconditions;

import java.net.URI;

/**
 * Manages information about a pull replication: replication from
 * remote CouchDB/Cloudant DB to local {@link Datastore}.
 */
public class PullReplication extends Replication {

    public URI source;
    public Datastore target;
    public Filter filter;

    @Override
    public void validate() {
        Preconditions.checkNotNull(this.target);
        Preconditions.checkNotNull(this.source);
        checkURI(this.source);
    }

    @Override
    public String getReplicatorName() {
        if(filter == null) {
            return String.format("%s <-- %s ", target.getDatastoreName(), source);
        } else {
            return String.format("%s <-- %s (%s)", target.getDatastoreName(), source, filter.name);
        }
    }

    public String getSourceDbName() {
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
