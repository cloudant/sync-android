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

import com.cloudant.http.HttpConnectionRequestFilter;
import com.cloudant.http.HttpConnectionResponseFilter;
import com.cloudant.mazha.CouchConfig;
import com.cloudant.sync.datastore.Datastore;
import com.google.common.base.Preconditions;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

/**
 * <p>Provides configuration for a pull replication.</p>
 *
 * <p>A pull replication is <em>from</em> a remote Cloudant or CouchDB database
 * to the device's local datastore.</p>
 */
public class PullReplication extends Replication {

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
     * Adds request filters to run for every request made by this replication
     * @param requestFilters The filters to run
     * @return The current instance of PullReplication
     */
    public PullReplication addRequestFilters(List<HttpConnectionRequestFilter> requestFilters){
        this.requestFilters.addAll(requestFilters);
        return this;
    }

    /**
     *  Adds response filters to run for every response received from the server for this
     *  replication
     * @param responseFilters The filters to run
     * @return The current instance of PullReplication
     */
    public PullReplication addResponseFilters(List<HttpConnectionResponseFilter> responseFilters){
        this.responseFilters.addAll(responseFilters);
        return this;
    }

    /**
     *  Variable argument version of {@link PullReplication#addRequestFilters(List)}
     * @param requestFilters The filters to run
     * @return The current instance of PullReplication
     */
    public PullReplication addRequestFilters(HttpConnectionRequestFilter ... requestFilters){
        return this.addRequestFilters(Arrays.asList(requestFilters));
    }

    /**
     *  Variable argument version of {@link PullReplication#addResponseFilters(List)}
     * @param responseFilters The filters to run
     * @return The current instance of PullReplication
     */
    public PullReplication addResponseFilters(HttpConnectionResponseFilter ... responseFilters){
        this.addResponseFilters(Arrays.asList(responseFilters));
        return this;
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
