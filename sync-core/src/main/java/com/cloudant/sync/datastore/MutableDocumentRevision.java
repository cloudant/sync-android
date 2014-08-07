package com.cloudant.sync.datastore;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tomblench on 06/08/2014.
 */
public class MutableDocumentRevision
{

    public String docId;
    // NB the key is purely for the user's convenience and doesn't have to be the same as the attachment name
    public Map<String, Attachment> attachments;
    public DocumentBody body;
    public String sourceRevisionId;

    public MutableDocumentRevision() {
        this.attachments = new HashMap<String, Attachment>();
    }

}
