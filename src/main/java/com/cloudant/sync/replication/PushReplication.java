package com.cloudant.sync.replication;

import com.cloudant.mazha.CouchConfig;
import com.cloudant.sync.datastore.Datastore;
import com.google.common.base.Preconditions;

import java.net.URI;

/**
 * Manages information about a push replication: replication from
 * local {@link Datastore} to remote CouchDB/Cloudant DB.
 */
public class PushReplication extends Replication {

    public URI target;
    public Datastore source;

    @Override
    public void validate() {
        Preconditions.checkNotNull(this.target);
        Preconditions.checkNotNull(this.source);
        checkURI(this.target);
    }

    @Override
    public String getReplicatorName() {
        return String.format("%s <-- %s ", target, source.getDatastoreName());
    }

    public String getTargetDbName() {
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
