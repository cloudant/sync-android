/*
 * Copyright (C) 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.datastore.callables;

import com.cloudant.android.Base64InputStreamFactory;
import com.cloudant.sync.datastore.AttachmentManager;
import com.cloudant.sync.datastore.AttachmentStreamFactory;
import com.cloudant.sync.datastore.DatabaseImpl;
import com.cloudant.sync.datastore.DocumentNotFoundException;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.datastore.ForceInsertItem;
import com.cloudant.sync.datastore.PreparedAttachment;
import com.cloudant.sync.datastore.UnsavedStreamAttachment;
import com.cloudant.sync.notifications.DocumentCreated;
import com.cloudant.sync.notifications.DocumentModified;
import com.cloudant.sync.notifications.DocumentUpdated;
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.sqlite.SQLDatabase;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Force insert a list of items (Revisions) obtained by pull Replication into the local database
 *
 * @api_private
 */
public class ForceInsertCallable implements SQLCallable<List<DocumentModified>> {

    private static final Logger logger = Logger.getLogger(DatabaseImpl.class.getCanonicalName());

    private List<ForceInsertItem> items;

    private String attachmentsDir;
    private AttachmentStreamFactory attachmentStreamFactory;

    public ForceInsertCallable(List<ForceInsertItem> items, String attachmentsDir,
                               AttachmentStreamFactory attachmentStreamFactory) {
        this.items = items;
        this.attachmentsDir = attachmentsDir;
        this.attachmentStreamFactory = attachmentStreamFactory;
    }

    @Override
    public List<DocumentModified> call(SQLDatabase db) throws Exception {

        List<DocumentModified> events = new ArrayList<DocumentModified>();

        for (ForceInsertItem item : items) {

            logger.finer("forceInsert(): " + item.rev.toString());

            DocumentCreated documentCreated = null;
            DocumentUpdated documentUpdated = null;

            boolean ok = true;

            long docNumericId = new GetNumericIdCallable(item.rev.getId()).call(db);
            long seq = 0;

            if (docNumericId != -1) {
                seq = new DoForceInsertExistingDocumentWithHistoryCallable(item.rev,
                        docNumericId, item.revisionHistory,
                        item.attachments, attachmentsDir, attachmentStreamFactory).call(db);
                item.rev.initialiseSequence(seq);
                // TODO fetch the parent doc?
                documentUpdated = new DocumentUpdated(null, item.rev);
            } else {
                seq = new DoForceInsertNewDocumentWithHistoryCallable(item.rev, item
                        .revisionHistory).call(db);
                item.rev.initialiseSequence(seq);
                documentCreated = new DocumentCreated(item.rev);
            }

            // now deal with any attachments
            if (item.pullAttachmentsInline) {
                if (item.attachments != null) {
                    for (String att : item.attachments.keySet()) {
                        Map attachmentMetadata = (Map) item.attachments.get(att);
                        Boolean stub = (Boolean) attachmentMetadata.get("stub");

                        if (stub != null && stub) {
                            // stubs get copied forward at the end of
                            // insertDocumentHistoryIntoExistingTree - nothing to do
                            // here
                            continue;
                        }
                        String data = (String) attachmentMetadata.get("data");
                        String type = (String) attachmentMetadata.get("content_type");
                        InputStream is = Base64InputStreamFactory.get(new
                                ByteArrayInputStream(data.getBytes("UTF-8")));
                        // inline attachments are automatically decompressed,
                        // so we don't have to worry about that
                        UnsavedStreamAttachment usa = new UnsavedStreamAttachment(is,
                                att, type);
                        try {
                            PreparedAttachment pa = AttachmentManager.prepareAttachment(
                                    attachmentsDir, attachmentStreamFactory, usa);
                            AttachmentManager.addAttachment(db, attachmentsDir, item
                                    .rev, pa);
                        } catch (Exception e) {
                            logger.log(Level.SEVERE, "There was a problem adding the " +
                                            "attachment "
                                            + usa + "to the datastore for document "
                                            + item.rev,
                                    e);
                            throw e;
                        }
                    }
                }
            } else {

                try {
                    if (item.preparedAttachments != null) {
                        for (String[] key : item.preparedAttachments.keySet()) {
                            String id = key[0];
                            String rev = key[1];
                            try {
                                DocumentRevision doc = new GetDocumentCallable(id, rev, attachmentsDir, attachmentStreamFactory).call(db);
                                if (doc != null) {
                                    AttachmentManager.addAttachmentsToRevision(db,
                                            attachmentsDir, doc, item
                                                    .preparedAttachments.get(key));
                                }
                            } catch (DocumentNotFoundException e) {
                                //safe to continue, previously getDocumentInQueue
                                // could return
                                // null and this was deemed safe and expected behaviour
                                // DocumentNotFoundException is thrown instead of
                                // returning
                                // null now.
                                continue;
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "There was a problem adding an " +
                            "attachment to the datastore", e);
                    throw e;
                }


            }
            if (ok) {
                logger.log(Level.FINER, "Inserted revision: %s", item.rev);
                if (documentCreated != null) {
                    events.add(documentCreated);
                } else if (documentUpdated != null) {
                    events.add(documentUpdated);
                }
            }
        }
        return events;
    }
}
