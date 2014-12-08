package com.cloudant.sync.datastore;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tomblench on 06/08/2014.
 */
public class MutableDocumentRevision implements DocumentRevision
{

    public String docId;

    // ctor with no source revision id: this revision hasn't been saved yet
    public MutableDocumentRevision() {
        this(null);
    }

    // ctor with revision id: this revision has been saved (eg mutable copy)
    protected MutableDocumentRevision(String sourceRevisionId) {
        this.attachments = new HashMap<String, Attachment>();
        this.sourceRevisionId = sourceRevisionId;
    }

    @Override
    public String getId() {
        return docId;
    }

    protected final String sourceRevisionId;

    @Override
    public String getRevision() {
        // hasn't been saved yet so doesn't have a rev id
        return null;
    }

    // NB the key is purely for the user's convenience and doesn't have to be the same as the attachment name
    public Map<String, Attachment> attachments;

    @Override
    public Map<String, Attachment> getAttachments() {
        return attachments;
    }

    public DocumentBody body;

    @Override
    public DocumentBody getBody() {
        return body;
    }

    /**
     * Get the previous revision id of this document
     *
     * @return The previous revision id of this document
     */
    public String getSourceRevisionId() {
        return this.sourceRevisionId;
    }


}
