package com.cloudant.sync.notifications;

import com.cloudant.sync.datastore.DocumentRevision;

public class DocumentModified {

    /**
     * Generic event for document create/update/delete
     * 
     * @param prevDocument
     *            Previous document revision (null for document create)
     * @param newDocument
     *            New document revision
     */

    public DocumentModified(DocumentRevision prevDocument,
            DocumentRevision newDocument) {
        this.prevDocument = prevDocument;
        this.newDocument = newDocument;
    }

    public final DocumentRevision prevDocument;
    public final DocumentRevision newDocument;

}
