/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.internal.replication;

import com.cloudant.sync.documentstore.Attachment;
import com.cloudant.sync.documentstore.AttachmentException;
import com.cloudant.sync.documentstore.DocumentBodyFactory;
import com.cloudant.sync.documentstore.DocumentException;
import com.cloudant.sync.documentstore.DocumentNotFoundException;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.documentstore.LocalDocument;
import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.internal.documentstore.DocumentRevisionTree;
import com.cloudant.sync.internal.documentstore.DocumentRevsList;
import com.cloudant.sync.internal.documentstore.DocumentRevsUtils;
import com.cloudant.sync.internal.documentstore.ForceInsertItem;
import com.cloudant.sync.internal.documentstore.InternalDocumentRevision;
import com.cloudant.sync.internal.documentstore.PreparedAttachment;
import com.cloudant.sync.internal.mazha.DocumentRevs;
import com.cloudant.sync.internal.util.JSONUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

class DatastoreWrapper {

    private final static Logger logger = Logger.getLogger(DatastoreWrapper.class.getCanonicalName());

    private DatabaseImpl dbCore;

    public DatastoreWrapper(DatabaseImpl dbCore) {
        this.dbCore = dbCore;
    }

    public DatabaseImpl getDbCore() {
        return dbCore;
    }

    public String getIdentifier() throws DocumentStoreException {
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

    public void bulkInsert(List<PullStrategy.BatchItem> batches, boolean pullAttachmentsInline) throws DocumentException  {
        List<ForceInsertItem> itemsToInsert = new ArrayList<ForceInsertItem>();
        for (PullStrategy.BatchItem batch : batches) {
            for (DocumentRevs documentRevs : batch.revsList) {
                logger.log(Level.FINEST, "Bulk inserting document revs: %s", documentRevs);

                InternalDocumentRevision doc = DocumentRevsUtils.createDocument(documentRevs);

                List<String> revisions = DocumentRevsUtils.createRevisionIdHistory(documentRevs);
                Map<String, Object> attachments = documentRevs.getAttachments();
                itemsToInsert.add(new ForceInsertItem(doc, revisions, attachments, batch.attachments, pullAttachmentsInline));

            }
        }
        dbCore.forceInsert(itemsToInsert);

    }


    public void bulkInsert(DocumentRevsList documentRevsList, Map<String[],Map<String, PreparedAttachment>> preparedAttachments, boolean pullAttachmentsInline) throws DocumentException  {
        for(DocumentRevs documentRevs: documentRevsList) {
            logger.log(Level.FINEST,"Bulk inserting document revs: %s",documentRevs);

            InternalDocumentRevision doc = DocumentRevsUtils.createDocument(documentRevs);

            List<String> revisions = DocumentRevsUtils.createRevisionIdHistory(documentRevs);
            Map<String, Object> attachments = documentRevs.getAttachments();
            dbCore.forceInsert(Collections.singletonList(new ForceInsertItem(doc, revisions,
                    attachments, preparedAttachments, pullAttachmentsInline)));
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
