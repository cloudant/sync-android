/**
 * Copyright (c) 2015 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.datastore;

import java.util.List;

public class TimestampBasedConflictsResolver implements ConflictResolver {

    @Override
    public BasicDocumentRevision resolve(String docId, List<BasicDocumentRevision> conflicts) {
        Long timestamp = null;
        BasicDocumentRevision winner = null;
        for(BasicDocumentRevision revision : conflicts) {
            if(revision.isDeleted()) { continue; }
            Long newTimestamp = (Long)revision.asMap().get("timestamp");
            if(newTimestamp != null) {
                if(timestamp == null || newTimestamp > timestamp) {
                    timestamp = newTimestamp;
                    winner = revision;
                }
            }
        }
        return winner;
    }
}
