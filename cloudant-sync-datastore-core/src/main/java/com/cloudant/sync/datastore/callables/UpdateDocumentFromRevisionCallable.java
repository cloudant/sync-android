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

import com.cloudant.sync.datastore.AttachmentManager;
import com.cloudant.sync.datastore.AttachmentStreamFactory;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.datastore.PreparedAttachment;
import com.cloudant.sync.datastore.SavedAttachment;
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.Misc;

import java.util.List;

/**
 * Update body and attachments of Document Revision by inserting a new child Revision with the JSON contents
 * {@code rev.body} and the new and existing attachments {@code preparedNewAttachments} and {@code existingAttachments}
 *
 * @api_private
 */
public class UpdateDocumentFromRevisionCallable implements SQLCallable<DocumentRevision> {



    private DocumentRevision rev;
    private List<PreparedAttachment> preparedNewAttachments;
    private List<SavedAttachment> existingAttachments;

    private String attachmentsDir;
    private AttachmentStreamFactory attachmentStreamFactory;

    public UpdateDocumentFromRevisionCallable(DocumentRevision rev, List<PreparedAttachment>
            preparedNewAttachments, List<SavedAttachment> existingAttachments, String
            attachmentsDir, AttachmentStreamFactory attachmentStreamFactory) {
        this.rev = rev;
        this.preparedNewAttachments = preparedNewAttachments;
        this.existingAttachments = existingAttachments;
        this.attachmentsDir = attachmentsDir;
        this.attachmentStreamFactory = attachmentStreamFactory;
    }

    @Override
    public DocumentRevision call(SQLDatabase db) throws Exception {
        Misc.checkNotNull(rev, "DocumentRevision");

        DocumentRevision updated = new UpdateDocumentBodyCallable(rev.getId(), rev.getRevision(), rev
                .getBody(), attachmentsDir, attachmentStreamFactory).call(db);
        AttachmentManager.addAttachmentsToRevision(db, attachmentsDir, updated,
                preparedNewAttachments);

        AttachmentManager.copyAttachmentsToRevision(db, existingAttachments, updated);

        // now re-fetch the revision with updated attachments
        DocumentRevision updatedWithAttachments = new GetDocumentCallable(updated.getId(),
                updated.getRevision(), this.attachmentsDir, this.attachmentStreamFactory).call(db);
        return updatedWithAttachments;
    }

}
