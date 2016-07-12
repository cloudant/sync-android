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

import com.cloudant.mazha.ChangesResult;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @api_private
 */
class ChangesResultWrapper {

    private final ChangesResult changes;

    public ChangesResultWrapper(ChangesResult changes) {
        Preconditions.checkNotNull(changes, "Changes feed can not be null");
        this.changes = changes;
    }


    public int size() {
        return this.changes.size();
    }

    public Object getLastSeq() {
        return this.changes.getLastSeq();
    }

    public List<ChangesResult.Row> getResults() {
        return this.changes.getResults();
    }

    public Multimap<String, String> openRevisions(int start, int end) {
        Preconditions.checkArgument(start >= 0, "Start position must be greater or equal to zero.");
        Preconditions.checkArgument(end > start, "End position must be greater than start.");
        Preconditions.checkArgument(end <= this.size(), "End position must be smaller than changes feed size.");

        Multimap<String, String> openRevisions = HashMultimap.create();
        for(int i = start; i < end ; i ++) {
            ChangesResult.Row row = this.getResults().get(i);
            List<ChangesResult.Row.Rev> revisions = row.getChanges();
            Set<String> openRevs = new HashSet<String>(revisions.size());
            for(ChangesResult.Row.Rev rev : revisions) {
                openRevs.add(rev.getRev());
            }
            openRevisions.putAll(row.getId(), openRevs);
        }
        return openRevisions;
    }
}
