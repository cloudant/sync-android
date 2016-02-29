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
import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.AttachmentException;
import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.datastore.DocumentException;
import com.cloudant.sync.datastore.DocumentNotFoundException;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.datastore.DocumentRevisionTree;
import com.cloudant.sync.datastore.DocumentRevsList;
import com.cloudant.sync.datastore.DocumentRevsUtils;
import com.cloudant.sync.datastore.ForceInsertItem;
import com.cloudant.sync.datastore.LocalDocument;
import com.cloudant.sync.datastore.PreparedAttachment;
import com.cloudant.sync.util.JSONUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

class DatastoreWrapper {

    private final static Logger logger = Logger.getLogger(DatastoreWrapper.class.getCanonicalName());

    private DatastoreExtended dbCore;

    public DatastoreWrapper(DatastoreExtended dbCore) {
        this.dbCore = dbCore;
    }

    public DatastoreExtended getDbCore() {
        return dbCore;
    }

    public String getIdentifier() throws DatastoreException {
        return dbCore.getPublicIdentifier();
    }

    public Object getCheckpoint(String replicatorIdentifier) {
        logger.entering("DatastoreWrapper","getCheckpoint" + replicatorIdentifier);
        LocalDocument doc;
        try {
            doc = dbCore.getLocalDocument(getCheckpointDocumentId(replicatorIdentifier));
        } catch (DocumentNotFoundException e){
            //this exception is expected it means we don't have a checkpoint doc yet so we'll just
            //return null
            return null;
        }

        Map<String, Object> checkpointDoc = JSONUtils.deserialize(doc.body.asBytes(), Map.class);
        if(checkpointDoc == null) {
            return null;
        } else {
            return checkpointDoc.get("lastSequence");
        }
    }

    public void putCheckpoint(String replicatorIdentifier, Object sequence) throws DocumentException {

        logger.entering("DatastoreWrapper","putCheckpoint",new Object[]{replicatorIdentifier,sequence});
        String checkpointDocumentId = getCheckpointDocumentId(replicatorIdentifier);
        Map<String, Object> checkpointDoc = new HashMap<String, Object>();
        checkpointDoc.put("lastSequence", sequence);
        byte[] json = JSONUtils.serializeAsBytes(checkpointDoc);
        dbCore.insertLocalDocument(checkpointDocumentId, DocumentBodyFactory.create(json));

    }

    private String getCheckpointDocumentId(String replicatorIdentifier) {
        return "_local/" + replicatorIdentifier;
    }

    public void bulkInsert(List<BasicPullStrategy.BatchItem> batches, boolean pullAttachmentsInline) throws DocumentException  {
        List<ForceInsertItem> itemsToInsert = new ArrayList<ForceInsertItem>();
        for (BasicPullStrategy.BatchItem batch : batches) {
            for (DocumentRevs documentRevs : batch.revsList) {
                logger.log(Level.FINEST, "Bulk inserting document revs: %s", documentRevs);

                DocumentRevision doc = DocumentRevsUtils.createDocument(documentRevs);

                List<String> revisions = DocumentRevsUtils.createRevisionIdHistory(documentRevs);
                Map<String, Object> attachments = documentRevs.getAttachments();
                itemsToInsert.add(new ForceInsertItem(doc, revisions, attachments, batch.attachments, pullAttachmentsInline));

            }
        }
        dbCore.forceInsert(itemsToInsert);

    }


    public void bulkInsert(DocumentRevsList documentRevsList, Map<String[],List<PreparedAttachment>> preparedAttachments, boolean pullAttachmentsInline) throws DocumentException  {
        for(DocumentRevs documentRevs: documentRevsList) {
            logger.log(Level.FINEST,"Bulk inserting document revs: %s",documentRevs);

            DocumentRevision doc = DocumentRevsUtils.createDocument(documentRevs);

            List<String> revisions = DocumentRevsUtils.createRevisionIdHistory(documentRevs);
            Map<String, Object> attachments = documentRevs.getAttachments();
            dbCore.forceInsert(doc, revisions, attachments,preparedAttachments, pullAttachmentsInline);
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

    protected PreparedAttachment prepareAttachment(Attachment att, long length, long encodedLength) throws AttachmentException {
        return this.dbCore.prepareAttachment(att, length, encodedLength);
    }

}
