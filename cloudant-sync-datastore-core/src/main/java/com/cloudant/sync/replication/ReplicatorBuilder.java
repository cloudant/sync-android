/*
 * Copyright © 2015, 2017 IBM Corp. All rights reserved.
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

import com.cloudant.http.HttpConnectionInterceptor;
import com.cloudant.http.HttpConnectionRequestInterceptor;
import com.cloudant.http.HttpConnectionResponseInterceptor;
import com.cloudant.http.internal.interceptors.CookieInterceptor;
import com.cloudant.http.internal.interceptors.IamCookieInterceptor;
import com.cloudant.http.internal.interceptors.UserAgentInterceptor;
import com.cloudant.sync.documentstore.DocumentStore;
import com.cloudant.sync.internal.replication.PullStrategy;
import com.cloudant.sync.internal.replication.PushStrategy;
import com.cloudant.sync.internal.replication.ReplicatorImpl;
import com.cloudant.sync.internal.util.Misc;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * A Builder to create a {@link Replicator Object}
 */
// S = Source Type, T = target Type, E = Extending class Type
public abstract class ReplicatorBuilder<S, T, E> {

    private static final UserAgentInterceptor USER_AGENT_INTERCEPTOR =
            new UserAgentInterceptor(ReplicatorBuilder.class.getClassLoader(),
                    "META-INF/com.cloudant.sync.client.properties");

    private T target;

    private S source;

    private String username;

    private String password;

    private int id = ReplicatorImpl.NULL_ID;

    private List<HttpConnectionRequestInterceptor> requestInterceptors = new ArrayList
            <HttpConnectionRequestInterceptor>();

    private List<HttpConnectionResponseInterceptor> responseInterceptors = new ArrayList
            <HttpConnectionResponseInterceptor>();

    private String iamApiKey = null;

    private ReplicatorBuilder(){
        requestInterceptors.add(USER_AGENT_INTERCEPTOR);
    }

    private int getDefaultPort(URI uri) {

        String uriProtocol = uri.getScheme();


        // assign default port if it hasn't been set
        // and check that we support the protocol
        int uriPort = uri.getPort();
        if ("http".equals(uriProtocol)) {
            uriPort = uriPort < 0 ? 80 : uriPort;
        } else if ("https".equals(uriProtocol)) {
            uriPort = uriPort < 0 ? 443 : uriPort;
        } else {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Protocol %s not supported", uriProtocol));
        }

        return uriPort;
    }

    private void setUserInfo(URI uri) {
        if (uri.getUserInfo() != null && this.username == null && this.password == null) {
            String[] parts = uri.getRawUserInfo().split(":");
            if (parts.length == 2) {
                try {
                    this.username = URLDecoder.decode(parts[0], "UTF-8");
                    this.password = URLDecoder.decode(parts[1], "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    // Should never happen, UTF-8 is required in JVM
                    throw new RuntimeException(e);
                }
            }

        }
    }

    private URI scrubUri(URI uri, String uriHost, String uriPath, String uriProtocol, int uriPort) {
        if(this.username == null && this.password == null){
            return uri;
        }

        try {
            //Remove user credentials from url
            return new URI(uriProtocol
                    + "://"
                    + uriHost
                    + ":"
                    + uriPort
                    + (uriPath != null ? uriPath : ""));
        } catch (URISyntaxException use) {
            throw new RuntimeException("Failed to construct URI", use);
        }

    }

    private URI getBaseUri(String uriHost, String uriPath, String uriProtocol, int uriPort) {
        try {
            String path = uriPath == null ? "" : uriPath;

            if (path.length() > 0) {
                int index = path.lastIndexOf("/");
                if (index == path.length() - 1) {
                    // we need to go back one
                    path = path.substring(0, index);
                    index = path.lastIndexOf("/");
                }
                path = path.substring(0, index);
            }

            return new URI(uriProtocol, null, uriHost, uriPort, path, null, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private void setAuthInterceptor(String uriHost, String uriPath, String uriProtocol, int uriPort) {
        if (iamApiKey != null) {
            URI baseURI = getBaseUri(uriHost, uriPath, uriProtocol, uriPort);
            IamCookieInterceptor ici = new IamCookieInterceptor(iamApiKey, baseURI.toString());
            requestInterceptors.add(ici);
            responseInterceptors.add(ici);

        } else if (this.username != null && this.password != null) {
            URI baseURI = getBaseUri(uriHost, uriPath, uriProtocol, uriPort);
            CookieInterceptor ci = new CookieInterceptor(this.username, this.password, baseURI.toString());
            requestInterceptors.add(ci);
            responseInterceptors.add(ci);
        }
    }

    // - set default port if needed and validate protocol (http(s))
    // - scrub out username and password if given, and pass it into cookie interceptor or discard it if we're doing IAM
    // - add IAM interceptor if needed
    // - else add cookie interceptor if needed
    private URI addAuthInterceptorIfRequired(URI uri) {

        String uriProtocol = uri.getScheme();
        String uriHost = uri.getHost();
        String uriPath = uri.getRawPath();

        int uriPort = getDefaultPort(uri);
        setUserInfo(uri);
        setAuthInterceptor(uriHost, uriPath, uriProtocol, uriPort);
        return scrubUri(uri, uriHost, uriPath, uriProtocol, uriPort);
    }

    /**
     * A Push Replication Builder
     */
    public static class Push extends ReplicatorBuilder<DocumentStore, URI, Push> {

        private int changeLimitPerBatch = 500;

        private int bulkInsertSize = 10;

        private PushAttachmentsInline pushAttachmentsInline = PushAttachmentsInline.Small;

        private PushFilter pushFilter = null;

        @Override
        public Replicator build() {

            Misc.checkState(super.source != null && super.target != null,
                    "Source and target cannot be null");

            // add cookie interceptor and remove creds from URI if required
            super.target = super.addAuthInterceptorIfRequired(super.target);

            PushStrategy pushStrategy = new PushStrategy(super.source.database(),
                    super.target,
                    super.requestInterceptors,
                    super.responseInterceptors);

            pushStrategy.changeLimitPerBatch = changeLimitPerBatch;
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
    public static class Pull extends ReplicatorBuilder<URI, DocumentStore, Pull> {

        private PullFilter pullPullFilter = null;

        private PullSelector pullPullSelector = null;

        private int changeLimitPerBatch = 1000;

        private int insertBatchSize = 100;

        private boolean pullAttachmentsInline = false;

        @Override
        public Replicator build() {

            Misc.checkState(super.source != null && super.target != null,
                    "Source and target cannot be null");

            // add cookie interceptor and remove creds from URI if required
            super.source = super.addAuthInterceptorIfRequired(super.source);

            PullStrategy pullStrategy = new PullStrategy(super.source,
                    super.target.database(),
                    pullPullFilter,
                    pullPullSelector,
                    super.requestInterceptors,
                    super.responseInterceptors);

            pullStrategy.changeLimitPerBatch = changeLimitPerBatch;
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
         * Sets the selector to use for a pull replication
         *
         * @param pullPullSelector The Selector to use during a pull replication
         * @return This instance of {@link ReplicatorBuilder}
         */
        public Pull selector(PullSelector pullPullSelector) {
            this.pullPullSelector = pullPullSelector;
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
     * <p>
     *     Sets the IAM API key to use for authenticating requests.
     * </p>
     * <p>
     *     Note: the replicator will only use IAM to authenticate requests. This means that the
     *     userinfo part of the URL, if set, will be ignored.
     * </p>
     * <p>
     *     See the
     *     <a href="https://console.bluemix.net/docs/services/Cloudant/guides/iam.html#ibm-cloud-identity-and-access-management" target="_blank">
     *         Bluemix Identity and Access Management
     *     </a> documentation for more details.
     * </p>
     *
     * @param iamApiKey The API key
     * @return This instance of {@link ReplicatorBuilder}
     */
    public E iamApiKey(String iamApiKey) {
        this.iamApiKey = iamApiKey;
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
     * Sets the username to use when authenticating with the server.
     *
     * Setting the username and password (using the {@link ReplicatorBuilder#password(String)}) method
     * takes precedence over credentials passed via the URI.
     * @param username The username to use when authenticating.
     * @return The current instance of {@link ReplicatorBuilder}
     * @throws IllegalArgumentException if {@code username} is {@code null}.
     */
    public E username(String username) {
        Misc.checkNotNull(username, "username");
        this.username = username;
        //noinspection unchecked
        return (E) this;
    }

    /**
     * Sets the password to use when authenticating with the server.
     *
     * Setting the username (using the {@link ReplicatorBuilder#username(String)}) and password method
     * takes precedence over credentials passed via the URI.
     *
     * @param password The password to use when authenticating.
     * @return The current instance of {@link ReplicatorBuilder}
     * @throws IllegalArgumentException if {@code password} is {@code null}.
     */
    public E password(String password) {
        Misc.checkNotNull(password, "password");
        this.password = password;
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
