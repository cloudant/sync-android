package com.cloudant.sync.datastore;

import java.util.List;

public class TimestampBasedConflictsResolver implements ConflictResolver {

    @Override
    public DocumentRevision resolve(String docId, List<DocumentRevision> conflicts) {
        Long timestamp = null;
        DocumentRevision winner = null;
        for(DocumentRevision revision : conflicts) {
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
