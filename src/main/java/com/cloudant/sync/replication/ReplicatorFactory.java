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
import java.net.URISyntaxException;

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
    @Deprecated
    public static Replicator oneway(Datastore source, URI target) {
        try {
            String user = getUsername(target.getUserInfo());
            String password = getPassword(target.getUserInfo());
            PushReplication pushReplication = new PushReplication();
            pushReplication.username = user;
            pushReplication.password = password;
            pushReplication.source = source;
            pushReplication.target = removeUsernamePassword(target);

            return new BasicReplicator(pushReplication);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * <p>Creates a Replicator object set up to replicate changes from the
     * local datastore to a remote database.</p>
     *
     * @param replication {@code PushReplication} instance to specify replication
     *                    from local datastore to remote CouchDb/Cloudant
     *
     * @return a {@link Replicator} instance which can be used to start and
     *  stop the replication itself.
     *
     * @see com.cloudant.sync.replication.PushReplication
     */
    public static Replicator oneway(PushReplication replication) {
        return new BasicReplicator(replication);
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
    @Deprecated
    public static Replicator oneway(URI source, Datastore target) {
        try {
            String user = getUsername(source.getUserInfo());
            String password = getPassword(source.getUserInfo());
            PullReplication pullReplication = new PullReplication();
            pullReplication.username = user;
            pullReplication.password = password;
            pullReplication.source = removeUsernamePassword(source);
            pullReplication.target = target;

            return new BasicReplicator(pullReplication);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * <p>Creates a Replicator object set up to replicate changes from a
     * remote database to the local datastore.</p>
     *
     * @param replication {@code PullReplication} instance to specify replication
     *                    from remote CouchDB/Cloudant to local datastore
     *
     * @return a {@link Replicator} instance which can be used to start and
     *  stop the replication itself.
     *
     * @see com.cloudant.sync.replication.PullReplication
     */
    public static Replicator oneway(PullReplication replication) {
        return new BasicReplicator(replication);
    }

    static URI removeUsernamePassword(URI u) throws URISyntaxException {
        return new URI(u.getScheme(),
                null, u.getHost(), u.getPort(),
                u.getPath(), u.getQuery(), u.getFragment());
    }

    static String getUsername(String userInfo) {
        if(userInfo == null || userInfo.indexOf(':') < 0) {
            return "";
        } else {
            return userInfo.substring(0, userInfo.indexOf(':'));
        }
    }

    static String getPassword(String userInfo) {
        if(userInfo == null || userInfo.indexOf(':') < 0) {
            return "";
        } else {
            return userInfo.substring(userInfo.indexOf(':') + 1);
        }
    }
}