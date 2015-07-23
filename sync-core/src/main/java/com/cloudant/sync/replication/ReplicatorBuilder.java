/*
 * Copyright (c) 2015 IBM Corp. All rights reserved.
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
 * A Builder to create a {@link Replicator Object}
 */
// S = Source Type, T = target Type, E = Extending class Type
public abstract class ReplicatorBuilder<S, T, E> {

    private T target;
    private S source;

    /**
     * A Push Replication Builder
     */
    public static class Push extends ReplicatorBuilder<Datastore, URI, Push> {

        @Override
        public Replicator build() {

            if (super.source == null || super.target == null) {
                throw new IllegalStateException("Source and target cannot be null");
            }

            PushReplication pushReplication = new PushReplication();
            pushReplication.source = super.source;
            pushReplication.target = super.target;
            return new BasicReplicator(pushReplication);

        }
    }

    /**
     * A Pull Replication Builder
     */
    public static class Pull extends ReplicatorBuilder<URI, Datastore, Pull> {

        private PullFilter pullPullFilter = null;

        @Override
        public Replicator build() {

            if (super.source == null || super.target == null) {
                throw new IllegalStateException("Source and target cannot be null");
            }

            PullReplication pullReplication = new PullReplication();
            pullReplication.source = super.source;
            pullReplication.target = super.target;
            //convert the new filter to the old one.
            //this is to avoid invasive changes for now.
            Replication.Filter filter = new Replication.Filter(this.pullPullFilter.getName(),
                    this.pullPullFilter.getParameters());
            pullReplication.filter = filter;

            return new BasicReplicator(pullReplication);
        }

        /**
         * Sets the filter to use for a pull replication
         *
         * @param pullPullFilter The Filter to use during a pull replication
         * @return This instance of {@link ReplicatorBuilder}
         */
        public Pull filter(PullFilter pullPullFilter) {
            this.pullPullFilter = pullPullFilter;
            return this;
        }
    }


    /**
     * Sets the target database for the replication
     *
     * @param target The target for the replication
     * @return This instance of {@link ReplicatorBuilder}
     */
    public E to(T target) {
        this.target = target;
        return (E) this;
    }

    /**
     * Sets the source database for the replication
     *
     * @param source The source database for the replication
     * @return This instance of {@link ReplicatorBuilder}
     */
    public E from(S source) {
        this.source = source;
        return (E) this;
    }

    /**
     * Builds a replicator by calling {@link #build()} and then {@link Replicator#start()}
     * on the replicator returned.
     *
     * @return The replicator running the replication for this builder.
     */
    public Replicator start() {
        Replicator replicator = this.build();
        replicator.start();
        return replicator;
    }

    /**
     * Builds a replicator based on the configuration set.
     *
     * @return {@link Replicator} that will carry out the replication
     */
    public abstract Replicator build();

    /**
     * Creates a pull replication builder.
     *
     * @return A newly created {@link Pull} replication builder
     */
    public static Pull pull() {
        return new Pull();
    }

    /**
     * Creates a {@link Push} replication builder.
     *
     * @return A newly created @{Link Push} replication builder
     */
    public static Push push() {
        return new Push();
    }

}
