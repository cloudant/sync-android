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

import com.cloudant.mazha.DocumentRevs;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.datastore.DocumentRevisionTree;
import com.cloudant.sync.datastore.DocumentRevsList;
import com.cloudant.sync.datastore.DocumentRevsUtils;
import com.cloudant.sync.util.JSONUtils;
import com.cloudant.common.Log;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class DatastoreWrapper {

    private final static String LOG_TAG = "DatastoreWrapper";

    private DatastoreExtended dbCore;

    public DatastoreWrapper(DatastoreExtended dbCore) {
        this.dbCore = dbCore;
    }

    public DatastoreExtended getDbCore() {
        return dbCore;
    }

    public String getIdentifier() {
        return dbCore.getPublicIdentifier();
    }

    public String getCheckpoint(String replicatorIdentifier) {
        Log.i(LOG_TAG, "getCheckpoint(): " + replicatorIdentifier);
        DocumentRevision doc = dbCore.getLocalDocument(getCheckpointDocumentId(replicatorIdentifier));
        if(doc == null) {
            return null;
        }

        Map<String, Object> checkpointDoc = JSONUtils.deserialize(doc.asBytes(), Map.class);
        if(checkpointDoc == null) {
            return null;
        } else {
            return (String)checkpointDoc.get("lastSequence");
        }
    }

    public void putCheckpoint(String replicatorIdentifier, String sequence) {
        Log.i(LOG_TAG, "putCheckpoint(): " + replicatorIdentifier);
        String checkpointDocumentId = getCheckpointDocumentId(replicatorIdentifier);
        DocumentRevision doc = dbCore.getLocalDocument(checkpointDocumentId);
        Map<String, String> checkpointDoc = new HashMap<String, String>();
        checkpointDoc.put("lastSequence", sequence);
        byte[] json = JSONUtils.serializeAsBytes(checkpointDoc);
        if(doc == null) {
            dbCore.createLocalDocument(checkpointDocumentId, DocumentBodyFactory.create(json));
        } else {
            dbCore.updateLocalDocument(doc.getId(), doc.getRevision(), DocumentBodyFactory.create(json));
        }
    }

    private String getCheckpointDocumentId(String replicatorIdentifier) {
        return "_local/" + replicatorIdentifier;
    }

    public void bulkInsert(DocumentRevsList documentRevsList) {
        for(DocumentRevs documentRevs: documentRevsList) {
            Log.v(LOG_TAG, "Bulk Inserting DocumentRevs: [ " + documentRevs.getId() + ", "
                    + documentRevs.getRev() + "," + documentRevs.getRevisions().getIds() + " ]");
            DocumentRevision doc = DocumentRevsUtils.createDocument(documentRevs);

            List<String> revisions = DocumentRevsUtils.createRevisionIdHistory(documentRevs);
            Map<String, Object> attachments = documentRevs.getAttachments();
            dbCore.forceInsert(doc, revisions, attachments);
        }
    }

    Map<String, DocumentRevisionTree> getDocumentTrees(List<DocumentRevision> documents) {
        Map<String, DocumentRevisionTree> allDocumentTrees =
                new HashMap<String, DocumentRevisionTree>();
        for(DocumentRevision doc: documents) {
            DocumentRevisionTree tree =
                    this.dbCore.getAllRevisionsOfDocument(doc.getId());
            allDocumentTrees.put(doc.getId(), tree);
        }
        return allDocumentTrees;
    }

}
