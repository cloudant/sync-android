package com.cloudant.sync.notifications;

import com.cloudant.sync.datastore.BasicDocumentRevision;

public class DocumentCreated extends DocumentModified {

    /**
     * Event for document create
     * 
     * <p>This event is posted by
     * {@link com.cloudant.sync.datastore.Datastore#createDocument(String, DocumentBody) createDocument(String, DocumentBody)} and
     * {@link com.cloudant.sync.datastore.Datastore#createDocument(DocumentBody) createDocument(DocumentBody)}.</p>
     *
     * @param newDocument
     *            New document revision
     */
    public DocumentCreated(BasicDocumentRevision newDocument) {
        super(null, newDocument);
    }

}
