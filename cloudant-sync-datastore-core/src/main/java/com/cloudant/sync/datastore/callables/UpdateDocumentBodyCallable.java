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

import com.cloudant.sync.datastore.AttachmentStreamFactory;
import com.cloudant.sync.datastore.ConflictException;
import com.cloudant.sync.datastore.DatabaseImpl;
import com.cloudant.sync.datastore.DocumentBody;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.CouchUtils;
import com.cloudant.sync.util.Misc;

/**
 * Update body of Document Revision by inserting a new child Revision with the JSON contents
 * {@code body}
 *
 * @api_private
 */
public class UpdateDocumentBodyCallable implements SQLCallable<DocumentRevision> {

    private String docId;
    private String prevRevId;
    private DocumentBody body;

    private String attachmentsDir;
    private AttachmentStreamFactory attachmentStreamFactory;

    public UpdateDocumentBodyCallable(String docId, String prevRevId, DocumentBody body, String
            attachmentsDir, AttachmentStreamFactory attachmentStreamFactory) {
        this.docId = docId;
        this.prevRevId = prevRevId;
        this.body = body;
        this.attachmentsDir = attachmentsDir;
        this.attachmentStreamFactory = attachmentStreamFactory;
    }

    @Override
    public DocumentRevision call(SQLDatabase db) throws Exception {
        Misc.checkNotNullOrEmpty(docId, "Input document id");
        Misc.checkNotNullOrEmpty(prevRevId, "Input previous revision id cannot be empty");
        Misc.checkNotNull(body, "Input document body");

        DatabaseImpl.validateDBBody(body);
        CouchUtils.validateRevisionId(prevRevId);

        // TODO quicker way of finding if current than fetching whole revision
        DocumentRevision preRevision = new GetDocumentCallable(docId, prevRevId, this.attachmentsDir, this.attachmentStreamFactory).call(db);

        if (!preRevision.isCurrent()) {
            throw new ConflictException("Revision to be updated is not current revision.");
        }

        new SetCurrentCallable(preRevision.getSequence(), false).call(db);
        InsertRevisionCallable insertRevisionCallable = DatabaseImpl.insertNewWinnerRevisionAdaptor(body, preRevision);
        String newRevisionId = insertRevisionCallable.revId;
        insertRevisionCallable.call(db);
        // TODO build the new DocumentRevision instead of retrieving the whole document again
        return new GetDocumentCallable(docId, newRevisionId, this.attachmentsDir, this.attachmentStreamFactory).call(db);
    }
}
