/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
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

package com.cloudant.sync.internal.documentstore;


import com.cloudant.sync.documentstore.Changes;
import com.cloudant.sync.documentstore.Database;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.internal.util.Misc;

import java.util.List;

/**
 * <p>The {@code Changes} object describes a list of changes to the {@link Database}.</p>
 *
 * <p>The object contains a list of the changes between a given sequence number
 * (passed to the {@link Database#changes(long, int)} method) and
 * the {@link Changes#lastSequence} field of the object.</p>
 *
 */
public class ChangesImpl implements Changes {

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
     */
    public ChangesImpl(long lastSequence, List<DocumentRevision> results) {
        Misc.checkNotNull(results, "Changes results");
        this.lastSequence = lastSequence;
        this.results = results;
    }

    @Override public long getLastSequence() {
        return this.lastSequence;
    }

    @Override public List<DocumentRevision> getResults() {
        return this.results;
    }
}
