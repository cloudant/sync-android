package com.cloudant.sync.notifications;

import com.cloudant.sync.datastore.BasicDocumentRevision;

public class DocumentModified {

    /**
     * Generic event for document create/update/delete
     * 
     * @param prevDocument
     *            Previous document revision (null for document create)
     * @param newDocument
     *            New document revision
     */

    public DocumentModified(BasicDocumentRevision prevDocument,
            BasicDocumentRevision newDocument) {
        this.prevDocument = prevDocument;
        this.newDocument = newDocument;
    }

    public final BasicDocumentRevision prevDocument;
    public final BasicDocumentRevision newDocument;

}
