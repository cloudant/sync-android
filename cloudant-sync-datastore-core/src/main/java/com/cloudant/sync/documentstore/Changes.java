/**
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.documentstore;


import com.cloudant.sync.internal.util.Misc;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>{@code Changes} objects describe a list of changes to the datastore.</p>
 *
 * <p>The object contains a list of the changes between some sequence number
 * (passed to the {@link Database#changes(long, int)} method) and
 * the {@link Changes#lastSequence} field of the object.</p>
 *
 * @api_public
 */
public class Changes {

    private final long lastSequence;

    private final List<DocumentRevision> results;

    /**
     * <p>
     * Construct a list of changes
     * </p>
     * <p>
     * Note that this constructor is for internal use. To get a set of changes from a given sequence
     * number, use {@link Database#changes}
     * </p>
     * @param lastSequence the last sequence number of this change set
     * @param results the list of {@code DocumentRevision}s in this change set
     *
     * @see Database#changes
     *
     * @api_private
     */
    public Changes(long lastSequence, List<DocumentRevision> results) {
        Misc.checkNotNull(results, "Changes results");
        this.lastSequence = lastSequence;
        this.results = results;
    }

    /**
     * <p>Returns the last sequence number of this change set.</p>
     *
     * <p>This number isn't necessarily the same as the sequence number of the
     * last {@code DocumentRevision} in the list of changes.</p>
     *
     * @return last sequence number of the changes set
     */
    public long getLastSequence() {
        return this.lastSequence;
    }

    /**
     * <p>Returns the list of {@code DocumentRevision}s in this change set.</p>
     *
     * @return the list of {@code DocumentRevision}s in this change set.
     */
    public List<DocumentRevision> getResults() {
        return this.results;
    }

    /**
     * <p>Returns the number of {@code DocumentRevision}s in this change set.</p>
     * @return the number of {@code DocumentRevision}s in this change set.
     */
    public int size() {
        return this.results.size();
    }

    /**
     * <p>Returns the list of document IDs for the {@code DocumentRevision}s in this
     * change set.</p>
     * @return the list of document IDs for the {@code DocumentRevision}s in this
     * change set.
     */
    public List<String> getIds() {
        List<String> ids = new ArrayList<String>();
        for(DocumentRevision obj : results) {
            ids.add(obj.getId());
        }
        return ids;
    }
}
