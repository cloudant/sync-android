/*
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
 * Copyright © 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.internal.replication;

import com.cloudant.sync.internal.common.ValueListMap;
import com.cloudant.sync.internal.mazha.ChangesResult;
import com.cloudant.sync.internal.util.Misc;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ChangesResultWrapper {

    private final ChangesResult changes;

    public ChangesResultWrapper(ChangesResult changes) {
        Misc.checkNotNull(changes, "Changes");
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

    public Map<String, List<String>> openRevisions(int start, int end) {
        Misc.checkArgument(start >= 0, "Start position must be greater or equal to zero.");
        Misc.checkArgument(end > start, "End position must be greater than start.");
        Misc.checkArgument(end <= this.size(), "End position must be less than or equal to the " +
                "changes feed size.");

        ValueListMap<String, String> openRevisions = new ValueListMap<String, String>();
        for(int i = start; i < end ; i ++) {
            ChangesResult.Row row = this.getResults().get(i);
            List<ChangesResult.Row.Rev> revisions = row.getChanges();
            Set<String> openRevs = new HashSet<String>(revisions.size());
            for(ChangesResult.Row.Rev rev : revisions) {
                openRevs.add(rev.getRev());
            }
            openRevisions.addValuesToKey(row.getId(), openRevs);
        }
        return openRevisions;
    }
}
