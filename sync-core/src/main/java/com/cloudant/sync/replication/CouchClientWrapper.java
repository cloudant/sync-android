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

package com.cloudant.sync.replication;

import com.cloudant.mazha.ChangesResult;
import com.cloudant.mazha.CouchClient;
import com.cloudant.mazha.CouchConfig;
import com.cloudant.mazha.CouchException;
import com.cloudant.mazha.DocumentRevs;
import com.cloudant.mazha.OkOpenRevision;
import com.cloudant.mazha.OpenRevision;
import com.cloudant.mazha.Response;
import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.MultipartAttachmentWriter;
import com.cloudant.sync.datastore.UnsavedStreamAttachment;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

public class CouchClientWrapper implements CouchDB {

    private final static String LOG_TAG = "CouchClientWrapper";
    private static final Logger logger = Logger.getLogger(CouchClientWrapper.class.getCanonicalName());
    public static final int SOCKET_TIMEOUT_DEFAULT = 30000;
    public static final int CONNECTION_TIMEOUT_DEFAULT = 30000;

    final CouchClient couchClient;

    public CouchClientWrapper(CouchClient client) {
        Preconditions.checkNotNull(client, "Couch client must not be null");
        this.couchClient = client;
    }

    public CouchClientWrapper(CouchConfig config) {
        this(new CouchClient(config));
    }

    public CouchClient getCouchClient() {
        return couchClient;
    }

    @Override
    public String getIdentifier() {
        return couchClient.getRootUri().toString();
    }

    @Override
    public boolean exists() {
        try {
            this.couchClient.getDbInfo();
            return true;
        } catch (CouchException e) {
            if (e.getStatusCode() == 404) {
                return false;
            } else {
                throw e;
            }
        }
    }

    @Override
    public String getCheckpoint(String checkpointId) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(checkpointId),
                "Checkpoint id must not be empty");
        try {
            RemoteCheckpointDoc response = couchClient.getDocument(
                    getCheckpointLocalDocId(checkpointId), RemoteCheckpointDoc.class);
            return response.getLastSequence();
        } catch (CouchException e) {
            return null;
        }
    }

    private String getCheckpointLocalDocId(String replicatorIdentifier) {
        return "_local/" + replicatorIdentifier;
    }

    @Override
    public void putCheckpoint(String checkpointId, String sequence) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(checkpointId),
                "Checkpoint id must not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(sequence), "Sequence must not be empty");
        String replicatorLocalDocId = getCheckpointLocalDocId(checkpointId);
        if (couchClient.contains(replicatorLocalDocId)) {
            updateCheckpoint(replicatorLocalDocId, sequence);
        } else {
            createCheckpoint(replicatorLocalDocId, sequence);
        }
    }

    @Override
    public ChangesResult changes(Object lastSequence, int limit) {
        return couchClient.changes(lastSequence, limit);
    }

    @Override
    public ChangesResult changes(Replication.Filter filter, Object lastSequence, int limit) {
        if(filter == null) {
            return couchClient.changes(lastSequence, limit);
        } else {
            return couchClient.changes(filter.name, filter.parameters, lastSequence, limit);
        }
    }

    /**
     * For each open revision, there should be a response of <code>DocumentRevs</code> returned.
     *
     * @see DocumentRevs
     */
    @Override
    public List<DocumentRevs> getRevisions(String documentId,
                                           Collection<String> revisionIds,
                                           Collection<String> attsSince,
                                           boolean pullAttachmentsInline) {
        List<OpenRevision> openRevisions =
                couchClient.getDocWithOpenRevisions(documentId, revisionIds, attsSince, pullAttachmentsInline);

        // expect all the open revisions return ok, return error is there is any missing
        List<DocumentRevs> documentRevs = new ArrayList<DocumentRevs>();
        for(OpenRevision openRev : openRevisions) {
            if(openRev instanceof OkOpenRevision) {
                documentRevs.add(((OkOpenRevision) openRev).getDocumentRevs());
            } else {
                throw new RuntimeException("Missing open revision for document:" + documentId
                           + ", revisions: " + Arrays.asList(revisionIds));
            }
        }

        return documentRevs;
    }

    @Override
    public Response create(Object object) {
        return couchClient.create(object);
    }

    @Override
    public Response update(String id, Object object) {
        return couchClient.update(id, object);
    }

    @Override
    public <T> T get(Class<T> classType, String id) {
        return couchClient.getDocument(id, classType);
    }

    @Override
    public Response delete(String id, String rev) {
        return couchClient.delete(id, rev);
    }


    private void createCheckpoint(String checkpointDocId, String sequence) {
        RemoteCheckpointDoc checkpointDoc = new RemoteCheckpointDoc(sequence);
        checkpointDoc.setId(checkpointDocId);
        Response response = couchClient.create(checkpointDoc);
        logger.fine(String.format("Response: %s",response));
    }

    private void updateCheckpoint(String checkpointDocId, String sequence) {
        RemoteCheckpointDoc checkpointDoc = couchClient.getDocument(
                checkpointDocId, RemoteCheckpointDoc.class);
        checkpointDoc.setLastSequence(sequence);
        Response response = couchClient.update(checkpointDocId, checkpointDoc);
        logger.fine(String.format("Response: %s",response));
    }

    public void createDatabase() {
        couchClient.createDb();
    }

    // Very dangerous call, be careful
    void deleteDatabase() {
        couchClient.deleteDb();
    }

    @Override
    public void bulk(List<BasicDocumentRevision> revisions) {
        logger.entering("com.cloudant.sync.replication.CouchClientWrapper","bulk",revisions);

        List<Map> allObjs = new ArrayList<Map>();
        for (BasicDocumentRevision obj : revisions) {
            logger.finer(String.format("Document body: %s",obj.getBody()));
            allObjs.add(obj.asMap());
        }

        List<Response> responses = couchClient.bulk(allObjs);
        for (Response r : responses) {
            logger.info(String.format("Response: %s",r));
        }
    }

    @Override
    public void bulkSerializedDocs(List<String> serializedDocs) {
        logger.entering("com.cloudant.sync.replication.CouchClientWrapper","bulkSerializedDocs",serializedDocs);
        if(serializedDocs.size() <= 0) {
            return;
        }

        List<Response> responses = couchClient.bulkSerializedDocs(serializedDocs);
        if(responses != null && responses.size() > 0) {
            logger.severe(String.format("Unknown bulk API error: %s for input: %s",responses,serializedDocs));
            throw new RuntimeException("Unknown bulk api error");
        }
    }

    @Override
    public List<Response> putMultiparts(List<MultipartAttachmentWriter> multiparts) {
        logger.entering("com.cloudant.sync.replication.CouchClientWrapper","putMultiparts",multiparts);
        ArrayList<Response> responses = new ArrayList<Response>();
        for (MultipartAttachmentWriter mpw : multiparts) {
            Response r = couchClient.putMultipart(mpw);
            responses.add(r);
        }
        return responses;
    }

    @Override
    public Map<String, Set<String>> revsDiff(Map<String, Set<String>> revisions) {
        return this.couchClient.revsDiff(revisions);
    }

    @Override
    public UnsavedStreamAttachment getAttachmentStream(String id, String rev, String attachmentName, String contentType, String encodingStr) {
        InputStream is = this.couchClient.getAttachmentStream(id, rev, attachmentName);
        Attachment.Encoding encoding = Attachment.getEncodingFromString(encodingStr);
        UnsavedStreamAttachment usa = new UnsavedStreamAttachment(is, attachmentName, contentType, encoding);
        return usa;
    }

}
