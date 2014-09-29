package com.cloudant.sync.notifications;

import com.cloudant.sync.datastore.BasicDocumentRevision;

public class DocumentCreated extends DocumentModified {

    /**
     * Event for document create
     * 
     * <p>This event is posted by
     * {@link com.cloudant.sync.datastore.Datastore#createDocumentFromRevision(com.cloudant.sync.datastore.MutableDocumentRevision)}
     * </p>
     *
     * @param newDocument
     *            New document revision
     */
    public DocumentCreated(BasicDocumentRevision newDocument) {
        super(null, newDocument);
    }

}
