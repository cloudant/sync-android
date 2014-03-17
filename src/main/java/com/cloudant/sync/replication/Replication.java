package com.cloudant.sync.replication;

import com.cloudant.mazha.CouchConfig;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * <p>Abstract class which provides configuration for a replication.</p>
 *
 * <p>This class is abstract. Concrete classes
 * {@link com.cloudant.sync.replication.PullConfiguration}
 * and {@link com.cloudant.sync.replication.PushReplication} are used to
 * configure pull and push replications respectively.</p>
 *
 * @see com.cloudant.sync.replication.PullReplication
 * @see ReplicatorFactory#oneway(PullReplication)
 * @see com.cloudant.sync.replication.PushReplication
 * @see ReplicatorFactory#oneway(PushReplication)
 */
public abstract class Replication {

    /**
     * Username to use for authentication with the remote Cloudant/CouchDB
     * database.
     */
    public String username;

    /**
     * Password to use for authentication with the remote Cloudant/CouchDB
     * database.
     */
    public String password;

    /**
     * <p>Provides the name and parameters for a filter function to be used
     * when a pull replication calls the source database's {@code _changes}
     * feed.</p>
     *
     * <p>For a filter function that takes no parameters, use this
     * constructor:</p>
     *
     * <pre>
     * Filter filter = new Filter("filterDoc/filterFunctionName")
     * </pre>
     *
     * <p>For a filter function that requires parameters, use this
     * constructor:</p>
     *
     * <pre>
     * Map<String, String> params = new HashMap();
     * map.put("max_age", "10");
     * map.put("name", "john");
     * Filter filter = new Filter("doc/filterName", map);
     * </pre>
     *
     * @see com.cloudant.sync.replication.PullReplication
     * @see <a href="http://docs.couchdb.org/en/1.4.x/replication.html#controlling-which-documents-to-replicate">Controlling documents replicated</a>
     * @see <a href="http://docs.couchdb.org/en/1.4.x/ddocs.html#filterfun">Filter functions CouchDB docs</a>
     */
    public static class Filter {

        /**
         * The name of the filter function to use.
         *
         * <p>The {@code name} attribute is the name of the filter, as passed to
         * the {@code filter} parameter of CouchDB's {@code _changes} feed. This
         * is the name of the design document, minus the leading {@code _design/},
         * and the name of the filter function, separated by a slash. For example,
         * {@code filterDoc/filterFunctionName}.</p>
         */
        public final String name;

        /**
         * Any parameters required for the function. Can be {@code null}.
         *
         * <p>The contents of {@code properties} are expanded
         * to {@code key=value} pairs when constructing the
         * {@code _changes} feed call for the remote database.
         * Integer values should be added as String objects.</p>
         *
         * @see <a href="http://docs.couchdb.org/en/1.4.x/ddocs.html#filterfun">Filter functions CouchDB docs</a>
         */
        public final Map<String, String> parameters;

        /**
         * Constructs a filter object for a function that requires no
         * parameters.
         *
         * @param filterName filter function name
         */
        public Filter(String filterName) {
            this.name = filterName;
            this.parameters = null;
        }

        /**
         * <p>Constructs a filter object for a function requiring
         * parameters.</p>
         *
         * <p>The query parameters should not be URL-escaped.</p>
         *
         * @param filterName filter function name
         * @param parameters filter function parameters
         */
        public Filter(String filterName, Map<String, String> parameters) {
            this.name = filterName;
            ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>();
            builder.putAll(parameters);
            this.parameters = builder.build();
        }

        /**
         * <p>Generate a string representation of this {@code Filter} object
         * that is consistent for a given name and parameter set.</p>
         *
         * <p>The string is not intended for use in URLs as it's not
         * escaped.</p>
         *
         * <p>Filter parameters are sorted by key so that the  generated
         * String are the same for different calls. This is important
         * because the String can therefore be part of the replication ID.</p>
         *
         * @return Query string like representation of the filter
         *
         * @see BasicPullStrategy#getReplicationId()
         */
        public String toQueryString() {
            if(this.parameters == null) {
                return String.format("filter=%s", this.name);
            } else {
                List<String> queries = new ArrayList<String>();

                for(Map.Entry<String, String> parameter : this.parameters.entrySet()) {
                    queries.add(String.format("%s=%s", parameter.getKey(), parameter.getValue()));
                }
                Collections.sort(queries);

                queries.add(0, String.format("filter=%s", this.name));

                return Joiner.on('&').skipNulls().join(queries);
            }
        }
    }

    protected Replication() {
        /* prevent instances of this class being constructed */
    }

    /**
     * Validate the replication parameters, and throw
     * {@code IllegalArgumentException} if the replication is not valid.
     */
    abstract void validate();

    /**
     * Name of the replication.
     *
     * @return name of the replication.
     */
    abstract String getReplicatorName();

    /**
     * Create the {@code ReplicationStrategy} that can use to conduct the
     * replication.
     *
     * @return the correct ReplicationStrategy that can use to conduct
     *         the replication.
     */
    abstract ReplicationStrategy createReplicationStrategy();

    CouchConfig createCouchConfig(URI uri, String username, String password) {
        int port = uri.getPort() < 0 ? getDefaultPort(uri.getScheme()) : uri.getPort();
        return new CouchConfig(uri.getScheme(), uri.getHost(),  port, username, password);
    }

    int getDefaultPort(String protocol) {
        if(protocol.equalsIgnoreCase("http")) {
            return 80;
        } else if(protocol.equalsIgnoreCase("https")) {
            return 443;
        } else {
            throw new IllegalArgumentException("Unsupported protocol: " + protocol);
        }
    }

    String extractDatabaseName(URI uri) {
        String db =  uri.getPath().substring(1);
        if(db.contains("/"))
            throw new IllegalArgumentException("DB name can not contain slash: '/'");
        return db;
    }

    void checkURI(URI uri) {
        Preconditions.checkArgument(uri.getUserInfo() == null,
                "There must be no user info (credentials) in replication URI " +
                        "(use Replication instance attributes)");
        Preconditions.checkArgument(uri.getScheme() != null, "Protocol must be provided in replication URI");
        Preconditions.checkArgument(uri.getHost() != null, "Host must be provided in replication URI");
        Preconditions.checkArgument(uri.getScheme().equalsIgnoreCase("http")
                || uri.getScheme().equalsIgnoreCase("https"), "Only http/https are supported in replication URI");
    }
}
