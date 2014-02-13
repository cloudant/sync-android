/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
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

import com.cloudant.sync.datastore.Datastore;

import java.net.URI;

/**
 * <p>Factory for {@link Replicator} objects.</p>
 *
 * <p>The {@code source} or {@code target} {@link URI} parameters used in the
 * methods below must include:</p>
 *
 * <pre>
 *   protocol://[username:password@]host[:port]/database_name
 * </pre>
 *
 * <p><em>protocol</em>, <em>host</em> and <em>database_name</em> are required.
 * If no <em>port</em> is provided, the default for <em>protocol</em> is used.
 * Using a <em>database_name</em> containing a {@code /} is not supported.</p>
 */
public class ReplicatorFactory {

    /**
     * <p>Creates a Replicator object set up to replicate changes from the
     * local datastore to a remote database.</p>
     *
     * @param source local {@link Datastore} to replicate changes from.
     * @param target remote database to replicate changes to.
     *
     * @return a {@link Replicator} instance which can be used to start and
     *  stop the replication itself.
     *
     */
    public static Replicator oneway(Datastore source, URI target) {
        return new BasicReplicator(source, target);
    }

    /**
     * <p>Creates a Replicator object set up to replicate changes from a
     * remote database to the local datastore.</p>
     *
     * @param source remote database to replicate changes from.
     * @param target local {@link Datastore} to replicate changes to.
     *
     * @return a {@link Replicator} instance which can be used to start and
     *  stop the replication itself.
     */
    public static Replicator oneway(URI source, Datastore target) {
        return new BasicReplicator(source, target);
    }
}