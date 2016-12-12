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

import com.cloudant.http.HttpConnectionRequestInterceptor;
import com.cloudant.http.HttpConnectionResponseInterceptor;
import com.cloudant.http.interceptors.CookieInterceptor;
import com.cloudant.http.internal.interceptors.UserAgentInterceptor;
import com.cloudant.sync.datastore.Datastore;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A Builder to create a {@link Replicator Object}
 *
 * @api_public
 */
// S = Source Type, T = target Type, E = Extending class Type
public abstract class ReplicatorBuilder<S, T, E> {

    private static final UserAgentInterceptor USER_AGENT_INTERCEPTOR =
            new UserAgentInterceptor(ReplicatorBuilder.class.getClassLoader(),
                    "META-INF/com.cloudant.sync.client.properties");

    private T target;
    private S source;
    private int id = ReplicatorImpl.NULL_ID;
    private List<HttpConnectionRequestInterceptor> requestInterceptors = new ArrayList
            <HttpConnectionRequestInterceptor>();
    private List<HttpConnectionResponseInterceptor> responseInterceptors = new ArrayList
            <HttpConnectionResponseInterceptor>();

    private ReplicatorBuilder(){
        requestInterceptors.add(USER_AGENT_INTERCEPTOR);
    }

    private URI addCookieInterceptorIfRequired(URI uri) {
        String uriProtocol = uri.getScheme();
        String uriHost = uri.getHost();
        String uriPath = uri.getRawPath();

        //Check if port exists
        int uriPort = uri.getPort();
        if (uriPort < 0) {
            if ("http".equals(uriProtocol)) {
                uriPort = 80;
            } else if ("https".equals(uriProtocol)) {
                uriPort = 443;
            } else {
                throw new RuntimeException("Unknown protocol: "+uriProtocol);
            }
        }

        if (uri.getUserInfo() != null) {
            String[] parts = uri.getRawUserInfo().split(":");
            if (parts.length == 2) {

                String path = uri.getRawPath();
                int index = path.lastIndexOf("/");
                if (index == path.length() - 1) {
                    // we need to go back one
                    path = path.substring(0, index);
                    index = path.lastIndexOf("/");
                }

                path = path.substring(0, index);

                URI baseURI;

                try {
                    baseURI = new URI(uriProtocol, null, uriHost, uriPort, path, null, null);
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }


                try {
                    // Decode the creds that came from the URI since they will already have been
                    // encoded
                    CookieInterceptor ci = new CookieInterceptor(URLDecoder.decode(parts[0],
                            "UTF-8"), URLDecoder.decode(parts[1], "UTF-8"), baseURI.toString());

                    requestInterceptors.add(ci);
                    responseInterceptors.add(ci);
                } catch (UnsupportedEncodingException e) {
                    // Should never happen, UTF-8 is required in JVM
                    throw new RuntimeException(e);
                }
            }
        }

        try {
            //Remove user credentials from url
            URI redacted = new URI(uriProtocol
                    + "://"
                    + uriHost
                    + ":"
                    + uriPort
                    + (uriPath != null ? uriPath : ""));
            return redacted;
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * A Push Replication Builder
     */
    public static class Push extends ReplicatorBuilder<Datastore, URI, Push> {

        private int changeLimitPerBatch = 500;

        private int batchLimitPerRun = 100;

        private int bulkInsertSize = 10;

        private PushAttachmentsInline pushAttachmentsInline = PushAttachmentsInline.Small;

        private PushFilter pushFilter = null;

        @Override
        public Replicator build() {

            if (super.source == null || super.target == null) {
                throw new IllegalStateException("Source and target cannot be null");
            }


            // add cookie interceptor and remove creds from URI if required
            super.target = super.addCookieInterceptorIfRequired(super.target);

            PushStrategy pushStrategy = new PushStrategy(super.source,
                    super.target,
                    super.requestInterceptors,
                    super.responseInterceptors);

            pushStrategy.changeLimitPerBatch = changeLimitPerBatch;
            pushStrategy.batchLimitPerRun = batchLimitPerRun;
            pushStrategy.bulkInsertSize = bulkInsertSize;
            pushStrategy.pushAttachmentsInline = pushAttachmentsInline;
            pushStrategy.filter = pushFilter;

            return new ReplicatorImpl(pushStrategy, super.id);
        }

        /**
         *  Sets the filter to use for this replication
         * @param filter the filter to use for this replication
         * @return This instance of {@link ReplicatorBuilder}
         */
        public Push filter(PushFilter filter){
            this.pushFilter = filter;
            return this;
        }

        /**
         * Sets the number of changes to fetch from the _changes feed per batch
         *
         * @param changeLimitPerBatch The number of changes to fetch from the _changes feed per batch
         * @return This instance of {@link ReplicatorBuilder}
         */
        public Push changeLimitPerBatch(int changeLimitPerBatch) {
            this.changeLimitPerBatch = changeLimitPerBatch;
            return this;
        }

        /**
         * Sets the number of batches to push in one replication run
         *
         * @param batchLimitPerRun The number of batches to push in one replication run
         * @return This instance of {@link ReplicatorBuilder}
         */
        public Push batchLimitPerRun(int batchLimitPerRun) {
            this.batchLimitPerRun = batchLimitPerRun;
            return this;
        }

        /**
         * Sets the number of documents to bulk insert into the CouchDB instance at a time
         *
         * @param bulkInsertSize The number of documents to bulk insert into the CouchDB instance at a time
         * @return This instance of {@link ReplicatorBuilder}
         */
        public Push bulkInsertSize(int bulkInsertSize) {
            this.bulkInsertSize = bulkInsertSize;
            return this;
        }

        /**
         * Sets the strategy to decide whether to push attachments inline or separately
         *
         * @param pushAttachmentsInline The strategy to decide whether to push attachments inline or separately
         * @return This instance of {@link ReplicatorBuilder}
         */
        public Push pushAttachmentsInline(PushAttachmentsInline pushAttachmentsInline) {
            this.pushAttachmentsInline = pushAttachmentsInline;
            return this;
        }
    }

    /**
     * A Pull Replication Builder
     */
    public static class Pull extends ReplicatorBuilder<URI, Datastore, Pull> {

        private PullFilter pullPullFilter = null;

        private int changeLimitPerBatch = 1000;

        private int batchLimitPerRun = 100;

        private int insertBatchSize = 100;

        private boolean pullAttachmentsInline = false;

        @Override
        public Replicator build() {

            if (super.source == null || super.target == null) {
                throw new IllegalStateException("Source and target cannot be null");
            }

            // add cookie interceptor and remove creds from URI if required
            super.source = super.addCookieInterceptorIfRequired(super.source);

            PullStrategy pullStrategy = new PullStrategy(super.source,
                    super.target,
                    pullPullFilter,
                    super.requestInterceptors,
                    super.responseInterceptors);

            pullStrategy.changeLimitPerBatch = changeLimitPerBatch;
            pullStrategy.batchLimitPerRun = batchLimitPerRun;
            pullStrategy.insertBatchSize = insertBatchSize;
            pullStrategy.pullAttachmentsInline = pullAttachmentsInline;

            return new ReplicatorImpl(pullStrategy, super.id);
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

        /**
         * Sets the number of changes to fetch from the _changes feed per batch
         *
         * @param changeLimitPerBatch The number of changes to fetch from the _changes feed per batch
         * @return This instance of {@link ReplicatorBuilder}
         */
        public Pull changeLimitPerBatch(int changeLimitPerBatch) {
            this.changeLimitPerBatch = changeLimitPerBatch;
            return this;
        }

        /**
         * Sets the number of batches to pull in one replication run
         *
         * @param batchLimitPerRun The number of batches to pull in one replication run
         * @return This instance of {@link ReplicatorBuilder}
         */
        public Pull batchLimitPerRun(int batchLimitPerRun) {
            this.batchLimitPerRun = batchLimitPerRun;
            return this;
        }

        /**
         * Sets the number of documents to insert into the SQLite database in one transaction
         *
         * @param insertBatchSize The number of documents to insert into the SQLite database in one transaction
         * @return This instance of {@link ReplicatorBuilder}
         */
        public Pull insertBatchSize(int insertBatchSize) {
            this.insertBatchSize = insertBatchSize;
            return this;
        }

        /**
         * Sets whether to pull attachments inline or separately
         *
         * @param pullAttachmentsInline Whether to pull attachments inline or separately
         * @return This instance of {@link ReplicatorBuilder}
         */
        public Pull pullAttachmentsInline(boolean pullAttachmentsInline) {
            this.pullAttachmentsInline = pullAttachmentsInline;
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
        //noinspection unchecked
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
        //noinspection unchecked
        return (E) this;
    }

    public E withId(int id) {
        this.id = id;
        //noinspection unchecked
        return (E) this;
    }

    /**
     * Variable argument version of {@link #addRequestInterceptors(List)}
     * @param interceptors The request interceptors to add.
     * @return The current instance of {@link ReplicatorBuilder}
     */
    public E addRequestInterceptors(HttpConnectionRequestInterceptor... interceptors){
        return addRequestInterceptors(Arrays.asList(interceptors));
    }

    /**
     * Adds interceptors to the list of request interceptors to use for each request made by this replication.
     * @param interceptors The request interceptors to add.
     * @return The current instance of {@link ReplicatorBuilder}
     */
    public E addRequestInterceptors(List<HttpConnectionRequestInterceptor> interceptors){
        this.requestInterceptors.addAll(interceptors);
        //noinspection unchecked
        return (E)this;
    }

    /**
     * Variable argument version of {@link #addResponseInterceptors(List)}
     * @param interceptors The response interceptors to add.
     * @return The current instance of {@link ReplicatorBuilder}
     */
    public E addResponseInterceptors(HttpConnectionResponseInterceptor... interceptors){
        return addResponseInterceptors(Arrays.asList(interceptors));
    }

    /**
     * Adds interceptors to the list of response interceptors to use for each response received by this replication.
     * @param interceptors The response interceptors to add.
     * @return The current instance of {@link ReplicatorBuilder}
     */
    public E addResponseInterceptors(List<HttpConnectionResponseInterceptor> interceptors){
        this.responseInterceptors.addAll(interceptors);
        //noinspection unchecked
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
