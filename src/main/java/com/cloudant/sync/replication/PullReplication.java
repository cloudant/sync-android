package com.cloudant.sync.replication;

import com.cloudant.mazha.CouchClient;
import com.cloudant.mazha.CouchConfig;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreExtended;

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

    @Override
    public ReplicationStrategy createReplicationStrategy() {

        String dbName = extractDatabaseName(this.source);
        CouchConfig couchConfig = createCouchConfig(this.source, this.username, this.password);
        CouchClient couchClient = new CouchClient(couchConfig, dbName);

        return new BasicPullStrategy(
                new CouchClientWrapper(couchClient),
                this.filter,
                (DatastoreExtended)this.target,
                this.getName()
        );
    }
}
