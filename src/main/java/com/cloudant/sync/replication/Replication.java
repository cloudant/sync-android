package com.cloudant.sync.replication;

import com.cloudant.mazha.CouchConfig;
import com.google.common.base.Joiner;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

abstract class Replication {

    public String username;
    public String password;

    public abstract String getName();

    public abstract ReplicationStrategy createReplicationStrategy();

    public static class Filter {
        public String name;
        public Map<String, String> parameters;

        public Filter(String name) {
            this.name = name;
        }

        public Filter(String name, Map<String, String> parameters) {
            this.name = name;
            this.parameters = parameters;
        }

        @Override
        public String toString() {
            if(this.parameters == null) {
                return String.format("filter=%s", this.name);
            } else {
                List<String> queries = new ArrayList<String>();
                for(Map.Entry<String, String> parameter : this.parameters.entrySet()) {
                    queries.add(String.format("%s=%s", parameter.getKey(), parameter.getValue()));
                }
                Collections.sort(queries);
                return String.format("filter=%s&%s", this.name,
                        Joiner.on('&').skipNulls().join(queries));
            }
        }
    }

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
}
