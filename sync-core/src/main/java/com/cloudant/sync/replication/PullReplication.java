/**
 * Copyright (c) 2015 Cloudant, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

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
public class PullReplication extends Replication<PullReplication> {

    /**
     * URI for this replication's remote database.
     *
     * Include username and password in the URL, or supply an Authorization header using
     * setCustomHeaders() in CouchConfig.
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



    /**
     * Sets the database from which to pull data
     * @param source The uri to the database to replicate with
     * @return The current instance of PullReplication
     */
    public PullReplication source(URI source){
        this.source = source;
        return this;
    }

    /**
     * Sets the target data store for this replication
     * @param target The target data store for this repliction
     * @return The current instance of PullReplication
     */
    public PullReplication target(Datastore target){
        this.target = target;
        return this;
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

    public CouchConfig getCouchConfig() {
        return this.createCouchConfig(this.source);
    }

    @Override
    ReplicationStrategy createReplicationStrategy() {
        return new BasicPullStrategy(this);
    }
}
