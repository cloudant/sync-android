package com.cloudant.sync.replication;

import com.cloudant.mazha.CouchConfig;
import com.google.common.base.Preconditions;

import java.net.URI;

/**
 * Manages information about a replication. It could be "pull" or "push",
 * and concrete classes "PullReplication" and "PushReplication" are for
 * them respectively.
 *
 * The concrete object is used to create a {@link com.cloudant.sync.replication.Replicator}
 * that can be used to managed a pull or push replication:
 * {@code ReplicatorFactory.oneway(PullReplication)} or
 * {@code ReplicatorFactory.oneway(PushReplication)}
 *
 * @see com.cloudant.sync.replication.PullReplication
 * @see com.cloudant.sync.replication.PushReplication
 * @see com.cloudant.sync.replication.ReplicatorFactory
 */
abstract class Replication {

    public String username;
    public String password;

    public abstract String getName();

    public abstract ReplicationStrategy createReplicationStrategy();

    static CouchConfig createCouchConfig(URI uri, String username, String password) {
        int port = uri.getPort() < 0 ? getDefaultPort(uri.getScheme()) : uri.getPort();
        return new CouchConfig(uri.getScheme(), uri.getHost(),  port, username, password);
    }

    static int getDefaultPort(String protocol) {
        if(protocol.equalsIgnoreCase("http")) {
            return 80;
        } else if(protocol.equalsIgnoreCase("https")) {
            return 443;
        } else {
            throw new IllegalArgumentException("Unsupported protocol: " + protocol);
        }
    }

    static String extractDatabaseName(URI uri) {
        String db =  uri.getPath().substring(1);
        if(db.contains("/"))
            throw new IllegalArgumentException("DB name can not contain slash: '/'");
        return db;
    }
}
