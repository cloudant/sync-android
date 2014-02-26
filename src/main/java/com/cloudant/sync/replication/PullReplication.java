package com.cloudant.sync.replication;

import com.cloudant.mazha.CouchClient;
import com.cloudant.mazha.CouchConfig;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreExtended;

import java.net.URI;

/**
 * Manages information about a pull replication: replication from
 * remote CouchDB/Cloudant DB to local {@link Datastore}.
 */
public class PullReplication extends Replication {

    public URI source;
    public Datastore target;

    @Override
    public String getName() {
        return String.format("%s <-- %s ", target.getDatastoreName(), source);
    }

    @Override
    public ReplicationStrategy createReplicationStrategy() {

        String dbName = extractDatabaseName(this.source);
        CouchConfig couchConfig = createCouchConfig(this.source, this.username, this.password);
        CouchClient couchClient = new CouchClient(couchConfig, dbName);

        return new BasicPullStrategy(
                new CouchClientWrapper(couchClient),
                (DatastoreExtended)this.target,
                this.getName()
        );
    }
}
