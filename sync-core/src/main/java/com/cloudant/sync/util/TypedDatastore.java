/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.util;

import com.cloudant.sync.datastore.ConflictException;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.DocumentNotFoundException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * <p>A datastore which wraps a {@link Datastore} in a serialisation layer
 * for a given class.</p>
 */
public class TypedDatastore<T extends Document> {

    private final Datastore datastore;
    private final Class<T> klass;

    public TypedDatastore(Class<T> klass, Datastore datastore) {
        this.klass = klass;
        this.datastore = datastore;
    }

    /**
     * <p>Returns passed {@link com.cloudant.sync.datastore.BasicDocumentRevision} deserialised to a concrete type.</p>
     * @param documentRevision document to deserialise
     * @return the deserialised version
     */
    public T deserializeDocumentRevision(BasicDocumentRevision documentRevision) {
        T document = JSONUtils.deserialize(documentRevision.asBytes(), klass);
        document.setId(documentRevision.getId());
        document.setRevision(documentRevision.getRevision());
        return document;
    }

    /**
     * <p>Returns a mapped document.</p>
     * @param documentId documentId to retrieve
     * @return the document mapped to the type of the datastore.
     *
     * @see Datastore#getDocument(String)
     */
    public T getDocument(String documentId) {
        BasicDocumentRevision documentRevision = this.datastore.getDocument(documentId);

        if(documentRevision == null) {
            throw new DocumentNotFoundException("DocumentRevisionTree does not exist with id: " + documentId);
        }

        if(documentRevision.isDeleted()) {
            throw new DocumentDeletedException("DocumentRevisionTree does not exist with id: " + documentId);
        }

        return deserializeDocumentRevision(documentRevision);
    }

    /**
     * <p>Returns a mapped document for a revision ID.</p>
     * @param documentId document ID of document
     * @param revisionId revision of document
     * @return the deserialized object for id and revision
     *
     * @see Datastore#getDocument(String, String)
     */
    public T getDocument(String documentId, String revisionId) {
        BasicDocumentRevision documentRevision = this.datastore.getDocument(documentId, revisionId);

        if(documentRevision == null) {
            throw new DocumentNotFoundException("DocumentRevisionTree does not exist with id: " + documentId);
        }

        if(documentRevision.isDeleted()) {
            throw new DocumentDeletedException("DocumentRevisionTree with id: " + documentId + " is marked as deleted.");
        }

        return deserializeDocumentRevision(documentRevision);
    }

    /**
     * Returns whether this datastore contains a document.
     * @param documentId document ID of document
     * @param revisionId revision of document
     * @return true if document exists, false otherwise.
     *
     * @see Datastore#containsDocument(String, String)
     */
    public boolean containsDocument(String documentId, String revisionId) {
        BasicDocumentRevision documentRevision = this.datastore.getDocument(documentId, revisionId);
        return documentRevision != null;
    }


    /**
     * Returns whether this datastore contains a document.
     * @param documentId document ID of document
     * @return true if document exists, false otherwise.
     *
     * @see Datastore#containsDocument(String)
     */
    public boolean containsDocument(String documentId) {
        BasicDocumentRevision documentRevision = this.datastore.getDocument(documentId);
        return documentRevision != null;
    }

    /**
     * Save a document, creating it if it doesn't exist
     * @param document document to create
     * @return the new revision of the document
     * @throws ConflictException if the revision passed does not contain the
     *          revision ID of the current version in the datastore, if any
     *          is present.
     */
    public T saveDocument(T document) throws ConflictException {
        String id = document.getId();
        if(id == null) {
            return createDocument(document);
        } else {
            return updateDocument(document);
        }
    }

    /**
     * Create a document with an auto-generated ID.
     * @param document document to create
     * @return new revision of document
     *
     * @see Datastore#createDocument(com.cloudant.sync.datastore.DocumentBody)
     */
    public T createDocument(T document) {
        Preconditions.checkArgument(document.getId() == null, "DocumentRevisionTree id should be null.");

        byte[] json = JSONUtils.serializeAsBytes(document);
        BasicDocumentRevision documentRevision = this.datastore.createDocument(DocumentBodyFactory.create(json));
        T savedDocument = JSONUtils.deserialize(documentRevision.asBytes(), (Class<T>)document.getClass());
        savedDocument.setId(documentRevision.getId());
        savedDocument.setRevision(documentRevision.getRevision());
        return savedDocument;
    }

    /**
     * Update a document.
     * @param document document to update
     * @return new revision of document
     * @throws ConflictException if the revision passed does not contain the
     *          revision ID of the current version in the datastore.
     *
     * @see Datastore#updateDocument(String, String, com.cloudant.sync.datastore.DocumentBody)
     */
    public T updateDocument(T document)
            throws ConflictException {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(document.getId()), "DocumentRevisionTree id");
            Preconditions.checkArgument(!Strings.isNullOrEmpty(document.getRevision()),
                    "DocumentRevisionTree revision");

        String id = document.getId();
        String revision = document.getRevision();
        byte[] json = JSONUtils.serializeAsBytes(document);
        BasicDocumentRevision documentRevision = this.datastore.updateDocument(id, revision, DocumentBodyFactory.create(json));

        T updatedDocument = JSONUtils.deserialize(documentRevision.asBytes(), (Class<T>)document.getClass());
        updatedDocument.setId(documentRevision.getId());
        updatedDocument.setRevision(documentRevision.getRevision());

        return updatedDocument;
    }

    /**
     * Delete a document
     * @param document current revision of document to delete
     * @throws ConflictException if the revision passed is not the current one
     *          in the datastore.
     */
    public void deleteDocument(T document) throws ConflictException {
        deleteDocument(document.getId(), document.getRevision());
    }

    public void deleteDocument(String documentId, String revisionId)
            throws ConflictException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(documentId), "DocumentRevisionTree id");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(revisionId), "DocumentRevisionTree revision");

        this.datastore.deleteDocument(documentId, revisionId);
    }
}
