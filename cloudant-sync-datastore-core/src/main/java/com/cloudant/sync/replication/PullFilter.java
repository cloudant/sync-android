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

import com.cloudant.sync.util.Misc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Provides the name and parameters for a filter function to be used
 * when a pull replication calls the source database's {@code _changes}
 * feed.</p>
 *
 * <p>For a filter function that takes no parameters, use this
 * constructor:</p>
 *
 * <pre>
 * PullFilter filter = new PullFilter("filterDoc/filterFunctionName")
 * </pre>
 *
 * <p>For a filter function that requires parameters, use this
 * constructor:</p>
 *
 * <pre>
 * Map&lt;String, String&gt; params = new HashMap();
 * map.put("max_age", "10");
 * map.put("name", "john");
 * PullFilter pullFilter = new PullFilter("doc/filterName", map);
 * </pre>
 *
 * @see <a href="http://couchdb.readthedocs.org/en/1.6.x/replication/intro.html#controlling-which-documents-to-replicate">Controlling documents replicated</a>
 * @see <a href="http://docs.couchdb.org/en/1.6.x/couchapp/ddocs.html#filter-functions">Filter functions CouchDB docs</a>
 * @api_public
 */
public class PullFilter {

    private final String name;

    private final Map<String, String> parameters;

    /**
     * Constructs a filter object for a function that requires no
     * parameters.
     *
     *
     *
     *
     * @param filterName filter function name. The {@code filterName} argument is the name of the
     *                   filter, as passed to
     *                   the {@code filter} parameter of CouchDB's {@code _changes} feed. This
     *                   and the name of the filter function, separated by a slash. For example,
     *                   {@code filterDoc/filterFunctionName}
     */
    public PullFilter(String filterName) {
        this.name = filterName;
        this.parameters = null;
    }

    /**
     * Constructs a filter object for a function that requires no
     * parameters.
     *
     *
     *
     *
     * @param filterName filter function name. The {@code filterName} argument is the name of the
     *                   filter, as passed to
     *                   the {@code filter} parameter of CouchDB's {@code _changes} feed. This
     *                   and the name of the filter function, separated by a slash. For example,
     *                   {@code filterDoc/filterFunctionName}
     *
     * @param parameters Any parameters required for the function. Can be {@code null}. The contents
     *                   of {@code properties} are expanded to {@code key=value} pairs when
     *                   constructing the {@code _changes} feed call for the remote database.
     *                   Integer values should be added as String objects.
     *
     * @see <a href="http://docs.couchdb.org/en/1.6.x/couchapp/ddocs.html#filter-functions">Filter
     * functions CouchDB docs</a>
     */
    public PullFilter(String filterName, Map<String, String> parameters) {
        this.name = filterName;
        Map<String, String> internalParams = new HashMap<String, String>();
        internalParams.putAll(parameters);
        this.parameters = Collections.unmodifiableMap(internalParams);
    }

     public String getName() {
        return this.name;
    }

     public Map<String, String> getParameters() {
        return this.parameters;
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
     * @return query string like representation of the filter
     *
     * @see PullStrategy#getReplicationId()
     */
    public String toQueryString() {
        if (this.parameters == null) {
            return String.format("filter=%s", this.name);
        } else {
            List<String> queries = new ArrayList<String>();

            for(Map.Entry<String, String> parameter : this.parameters.entrySet()) {
                queries.add(String.format("%s=%s", parameter.getKey(), parameter.getValue()));
            }
            Collections.sort(queries);

            queries.add(0, String.format("filter=%s", this.name));

            return Misc.join("&", queries);
        }
    }
}
