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
