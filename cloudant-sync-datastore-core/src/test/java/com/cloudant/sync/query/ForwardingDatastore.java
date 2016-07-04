package com.cloudant.sync.query;

import com.cloudant.sync.datastore.Changes;
import com.cloudant.sync.datastore.ConflictException;
import com.cloudant.sync.datastore.ConflictResolver;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DocumentException;
import com.cloudant.sync.datastore.DocumentNotFoundException;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.event.EventBus;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by rhys on 04/07/2016.
 */
public class ForwardingDatastore implements Datastore {


    protected final Datastore datastore;

    public ForwardingDatastore(Datastore datastore){
        this.datastore = datastore;
    }

    @Override
    public String getDatastoreName() {
        return datastore.getDatastoreName();
    }

    @Override
    public DocumentRevision getDocument(String documentId) throws DocumentNotFoundException {
        return datastore.getDocument(documentId);
    }

    @Override
    public DocumentRevision getDocument(String documentId, String revisionId) throws
            DocumentNotFoundException {
        return datastore.getDocument(documentId, revisionId);
    }

    @Override
    public boolean containsDocument(String documentId, String revisionId) {
        return datastore.containsDocument(documentId, revisionId);
    }

    @Override
    public boolean containsDocument(String documentId) {
        return datastore.containsDocument(documentId);
    }

    @Override
    public List<DocumentRevision> getAllDocuments(int offset, int limit, boolean descending) {
        return datastore.getAllDocuments(offset, limit, descending);
    }

    @Override
    public List<String> getAllDocumentIds() {
        return datastore.getAllDocumentIds();
    }

    @Override
    public List<DocumentRevision> getDocumentsWithIds(List<String> documentIds) throws
            DocumentException {
        return datastore.getDocumentsWithIds(documentIds);
    }

    @Override
    public long getLastSequence() {
        return datastore.getLastSequence();
    }

    @Override
    public int getDocumentCount() {
        return datastore.getDocumentCount();
    }

    @Override
    public Changes changes(long since, int limit) {
        return datastore.changes(since, limit);
    }

    @Override
    public EventBus getEventBus() {
        return datastore.getEventBus();
    }

    @Override
    public Iterator<String> getConflictedDocumentIds() {
        return datastore.getConflictedDocumentIds();
    }

    @Override
    public void resolveConflictsForDocument(String docId, ConflictResolver resolver) throws
            ConflictException {
        datastore.resolveConflictsForDocument(docId, resolver);
    }

    @Override
    public void close() {
        datastore.close();
    }

    @Override
    public DocumentRevision createDocumentFromRevision(DocumentRevision rev) throws DocumentException {
        return datastore.createDocumentFromRevision(rev);
    }

    @Override
    public DocumentRevision updateDocumentFromRevision(DocumentRevision rev) throws DocumentException {
        return datastore.updateDocumentFromRevision(rev);
    }

    @Override
    public DocumentRevision deleteDocumentFromRevision(DocumentRevision rev) throws ConflictException {
        return datastore.deleteDocumentFromRevision(rev);
    }

    @Override
    public List<DocumentRevision> deleteDocument(String id) throws DocumentException {
        return datastore.deleteDocument(id);
    }

    @Override
    public void compact() {
        datastore.compact();
    }

    @Override
    public Map<String, Object> listIndexes() {
        return datastore.listIndexes();
    }

    @Override
    public String ensureIndexed(List<Object> fieldNames) throws CheckedQueryException {
        return datastore.ensureIndexed(fieldNames);
    }

    @Override
    public String ensureIndexed(List<Object> fieldNames, String indexName) throws
            CheckedQueryException {
        return datastore.ensureIndexed(fieldNames, indexName);
    }

    @Override
    public String ensureIndexed(List<Object> fieldNames, String indexName, IndexType indexType) throws CheckedQueryException {
        return datastore.ensureIndexed(fieldNames, indexName, indexType);
    }

    @Override
    public String ensureIndexed(List<Object> fieldNames, String indexName, IndexType indexType, Map<String, String> indexSettings) throws CheckedQueryException {
        return datastore.ensureIndexed(fieldNames, indexName, indexType, indexSettings);
    }

    @Override
    public boolean deleteIndexNamed(String indexName) throws CheckedQueryException {
        return datastore.deleteIndexNamed(indexName);
    }

    @Override
    public boolean updateAllIndexes() {
        return datastore.updateAllIndexes();
    }

    @Override
    public boolean isTextSearchEnabled() throws CheckedQueryException {
        return datastore.isTextSearchEnabled();
    }

    @Override
    public QueryResult find(Map<String, Object> query) throws CheckedQueryException {
        return datastore.find(query);
    }

    @Override
    public QueryResult find(Map<String, Object> query, long skip, long limit, List<String> fields, List<Map<String, String>> sortDocument) throws CheckedQueryException {
        return datastore.find(query, skip, limit, fields, sortDocument);
    }
}
