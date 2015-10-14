package com.cloudant.sync.replication;

import java.util.List;

/**
 * Created by tomblench on 14/10/15.
 */

/**
 * Represents a bulk GET request for documents
 *
 * This is in the format which the replicator understands, where a list of rev ids is given for each
 * doc id
 */
public class BulkGetRequest {
    String id;
    List<String> revs;
    List<String> atts_since;

    public BulkGetRequest(String id, List<String> revs, List<String> atts_since) {
        this.atts_since = atts_since;
        this.id = id;
        this.revs = revs;
    }
}
