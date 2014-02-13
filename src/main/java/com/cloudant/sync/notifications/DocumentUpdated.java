package com.cloudant.sync.notifications;

import com.cloudant.sync.datastore.DocumentRevision;

public class DocumentUpdated extends DocumentModified {

    /**
     * Event for document update
     * 
     * <p>This event is posted by
     * {@link com.cloudant.sync.datastore.Datastore#updateDocument updateDocument}.</p>
     *
     * @param prevDocument
     *            Previous document revision
     * @param newDocument
     *            New document revision
     */
    public DocumentUpdated(DocumentRevision prevDocument,
            DocumentRevision newDocument) {
        super(prevDocument, newDocument);
    }

}
