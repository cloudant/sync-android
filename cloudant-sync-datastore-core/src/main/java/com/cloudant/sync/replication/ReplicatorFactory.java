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

/**
 * <p>Factory for {@link Replicator} objects.</p>
 * @deprecated Use {@link ReplicatorBuilder} instead
 */
@Deprecated
public final class ReplicatorFactory {

    private ReplicatorFactory() {
        /* prevent instances of this class being constructed */
    }


    /**
     * <p>Creates a Replicator object set up to replicate changes in one
     * direction between a local datastore and remote database.</p>
     *
     * @param replication replication configuration information
     *
     * @return a {@link Replicator} instance which can be used to start and
     *  stop the replication itself.
     *
     * @see com.cloudant.sync.replication.PushReplication
     * @see com.cloudant.sync.replication.PullReplication
     */
    public static Replicator oneway(Replication replication) {
        return new BasicReplicator(replication);
    }

    public static Replicator oneway(Replication replication, int id) {
        return new BasicReplicator(replication, id);
    }
}
