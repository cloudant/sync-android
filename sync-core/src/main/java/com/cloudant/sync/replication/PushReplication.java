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
 * <p>Provides configuration for a push replication.</p>
 *
 * <p>A push replication is <em>to</em> a remote Cloudant or CouchDB database
 * from the device's local datastore.</p>
 */
public class PushReplication extends Replication {

    /**
     * URI for this replication's remote database.
     *
     * Include username and password in the URL, or supply an Authorization header using
     * setCustomHeaders() in CouchConfig.
     */
    public URI target;
    /**
     * The local datastore for this replication.
     */
    public Datastore source;

    /**
     * Constructs a PushReplication object, configured by assigning to the
     * instance's attributes after construction.
     */
    public PushReplication() {
        /* Does nothing but we can now document it */
    }

    /**
     * Sets the data store from which data will be replicated
     *
     * @param source The source data store
     * @return The current instance of PushReplication
     */
    public PushReplication source(Datastore source) {
        this.source = source;
        return this;
    }

    /**
     * Sets the uri of the target remote database for this replication
     *
     * @param target The uri of the remot database
     * @return The current instance of PushReplication
     */
    public PushReplication target(URI target) {
        this.target = target;
        return this;
    }

    @Override
    void validate() {
        Preconditions.checkNotNull(this.target);
        Preconditions.checkNotNull(this.source);
        checkURI(this.target);
    }

    @Override
    String getReplicatorName() {
        return String.format("%s <-- %s ", target, source.getDatastoreName());
    }

    public CouchConfig getCouchConfig() {
        return this.createCouchConfig(this.target);
    }

    @Override
    ReplicationStrategy createReplicationStrategy() {
        return new BasicPushStrategy(this);
    }

}
