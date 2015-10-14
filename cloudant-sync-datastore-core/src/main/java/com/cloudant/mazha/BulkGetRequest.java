package com.cloudant.mazha;

import java.util.List;

/**
 * Created by tomblench on 14/10/15.
 */

/**
 * Represents a bulk GET request for documents
 *
 * This is in the format which the _bulk_get endpoint understands, where &lt;doc id, rev id&gt;
 * pairs are given. If multiple rev ids are required, then the doc id needs to be repeated across
 * multiple requests.
 */
public class BulkGetRequest {
    public String id;
    public String rev;
    public List<String> atts_since;
}