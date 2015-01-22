package com.cloudant.sync.replication;

import com.cloudant.mazha.CouchConfig;
import com.cloudant.sync.datastore.Datastore;
import com.google.common.base.Preconditions;

import java.net.URI;

/**
 * <p>Provides configuration for a pull replication.</p>
 *
 * <p>A pull replication is <em>from</em> a remote Cloudant or CouchDB database
 * to the device's local datastore.</p>
 */
public class PullReplication extends Replication {

    /**
     * URI for this replication's remote database.
     */
    public URI source;
    /**
     * The local datastore for this replication.
     */
    public Datastore target;
    /**
     * Configuration for any remote filter function to use during replication.
     *
     * @see <a href="http://docs.couchdb.org/en/latest/couchapp/ddocs.html#filter-functions">CouchDB docs on filter functions</a>
     */
    public Filter filter;

    /**
     * Constructs a PullReplication object, configured by assigning to the
     * instance's attributes after construction.
     */
    public PullReplication() {
        /* Does nothing but we can now document it */
    }

    @Override
    void validate() {
        Preconditions.checkNotNull(this.target);
        Preconditions.checkNotNull(this.source);
        checkURI(this.source);
    }

    @Override
    String getReplicatorName() {
        if(filter == null) {
            return String.format("%s <-- %s ", target.getDatastoreName(), source);
        } else {
            return String.format("%s <-- %s (%s)", target.getDatastoreName(), source, filter.name);
        }
    }

    String getSourceDbName() {
        return this.extractDatabaseName(this.source);
    }

    public CouchConfig getCouchConfig() {
        return this.createCouchConfig(this.source, this.username, this.password);
    }

    @Override
    ReplicationStrategy createReplicationStrategy() {
        return new BasicPullStrategy(this, null, null);
    }
}
