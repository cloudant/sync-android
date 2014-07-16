package com.cloudant.sync.notifications;

import com.cloudant.sync.datastore.DocumentRevision;

public class DocumentDeleted extends DocumentModified {

    /**
     * Event for document delete
     *
     * <p>This event is posted by
     * {@link com.cloudant.sync.datastore.Datastore#deleteDocument deleteDocument}.</p>
     * 
     * @param prevDocument
     *            Previous document revision
     * @param newDocument
     *            New (empty) document revision
     * 
     */
    public DocumentDeleted(DocumentRevision prevDocument,
            DocumentRevision newDocument) {
        super(prevDocument, newDocument);
    }

}
