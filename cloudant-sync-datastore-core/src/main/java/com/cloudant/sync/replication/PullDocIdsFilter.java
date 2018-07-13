package com.cloudant.sync.replication;

import com.cloudant.sync.internal.util.Misc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Sets the list of doc IDs to use as filtering criteria when a pull replication calls the
 * source database's {@code _changes} feed.
 * </p>
 * Note: Doc IDs filtering are supported only when replicating against a CouchDB 2.x compliant
 * database
 *
 * @see
 * <a target="_blank" href="https://console.bluemix.net/docs/services/Cloudant/api/database.html#get-changes">See doc_ids filtering</a>
 */
public class PullDocIdsFilter extends PullFilter {

    private static final String DOC_IDS_FILTER_NAME = "_doc_ids";

    public PullDocIdsFilter(List<String> docIds) {
        super(DOC_IDS_FILTER_NAME);

        Misc.checkState(docIds != null && !docIds.isEmpty(),
                "Doc Ids should not be empty in a Doc Ids Filter");

        List<String> internalDocIds = new ArrayList<String>(docIds);
        Collections.sort(internalDocIds);

        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("doc_ids", internalDocIds);
        this.parameters = parameters;
    }
}
