package com.cloudant.sync.replication;

import com.cloudant.mazha.CouchClient;
import com.cloudant.mazha.CouchConfig;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreExtended;

import java.net.URI;

public class PushReplication extends Replication {

    public URI target;
    public Datastore source;

    @Override
    public String getName() {
        return String.format("%s <-- %s ", target, source.getDatastoreName());
    }

    @Override
    public ReplicationStrategy createReplicationStrategy() {
        String dbName = extractDatabaseName(target);
        CouchConfig couchConfig = createCouchConfig(target, this.username, this.password);
        CouchClient couchClient = new CouchClient(couchConfig, dbName);

        return new BasicPushStrategy(
                new CouchClientWrapper(couchClient),
                (DatastoreExtended)this.source,
                this.getName()
        );
    }

}
