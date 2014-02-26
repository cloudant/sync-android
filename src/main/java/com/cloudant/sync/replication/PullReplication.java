package com.cloudant.sync.replication;

import com.cloudant.mazha.CouchClient;
import com.cloudant.mazha.CouchConfig;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreExtended;

import java.net.URI;

public class PullReplication extends Replication {
    public URI source;
    public Datastore target;

    @Override
    public String getName() {
        return String.format("%s <-- %s ", target.getDatastoreName(), source);
    }

    @Override
    public ReplicationStrategy createReplicationStrategy() {
        CouchConfig couchConfig = ReplicatorURIUtils.extractCouchConfig(source);
        String dbName = ReplicatorURIUtils.extractDatabaseName(source);
        CouchClient couchClient = new CouchClient(couchConfig, dbName);

        return new BasicPullStrategy(
                new CouchClientWrapper(couchClient),
                (DatastoreExtended)this.target,
                this.getName()
        );
    }
}
