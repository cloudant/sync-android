/*
 * Copyright © 2015 IBM Corp. All rights reserved.
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

import java.util.Map;

/**
 * <p>Provides the mango query selector for filter criteria to be used
 * when a pull replication calls the source database's {@code _changes}
 * feed.</p>
 *
 * <p>Use this constructor:</p>
 *
 * <pre>
 * PullSelector selector = new PullSelector("{...a selector...}")
 * </pre>
 *
 * @see
 * <a target="_blank" href="http://docs.couchdb.org/en/latest/replication/intro.html#controlling-which-documents-to-replicate">Controlling documents replicated</a>
 * @see
 * <a target="_blank" href="http://docs.couchdb.org/en/latest/replication/replicator.html#selectorobj">Selector Objects</a>
 */

public class PullSelector {
    private final String selector;

    /**
     * Constructs a selector object.
     *
     * @param selector expression. The {@code selector} argument is mangi query expression
     *                 as passed to
     *                 the {@code filter} parameter of CouchDB's {@code _changes} feed.
     */
    public PullSelector(String selector) {
        this.selector = selector;
    }

    /**
     * Returns the selector definition.
     * @return selector expression.
     */
    public String getSelector() {
        return selector;
    }
}
