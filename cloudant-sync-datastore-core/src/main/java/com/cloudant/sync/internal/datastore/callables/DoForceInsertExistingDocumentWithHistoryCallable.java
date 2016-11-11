/*
 * Copyright © 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.internal.datastore.callables;

import com.cloudant.sync.documentstore.AttachmentException;
import com.cloudant.sync.internal.datastore.AttachmentStreamFactory;
import com.cloudant.sync.internal.datastore.DatabaseImpl;
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.documentstore.DocumentNotFoundException;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.util.Misc;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Force insert a Revision where the Document already exists in the local database (ie at least one
 * Revision already exists)
 *
 * @api_private
 */
public class DoForceInsertExistingDocumentWithHistoryCallable implements SQLCallable<Long> {

    private static final Logger logger = Logger.getLogger(DatabaseImpl.class.getCanonicalName());

    private DocumentRevision newRevision;
    private long docNumericId;
    private List<String> revisions;
    private Map<String, Object> attachments;

    private String attachmentsDir;
    private AttachmentStreamFactory attachmentStreamFactory;

    /**
     *
     * @param newRevision DocumentRevision to insert
     * @param revisions   revision history to insert, it includes all revisions (include the
     *                    revision of the DocumentRevision
     *                    as well) sorted in ascending order.
     */
    public DoForceInsertExistingDocumentWithHistoryCallable(DocumentRevision newRevision, long
            docNumericId, List<String> revisions, Map<String, Object> attachments, String
            attachmentsDir, AttachmentStreamFactory attachmentStreamFactory) {
        this.newRevision = newRevision;
        this.docNumericId = docNumericId;
        this.revisions = revisions;
        this.attachments = attachments;
        this.attachmentsDir = attachmentsDir;
        this.attachmentStreamFactory = attachmentStreamFactory;
    }

    @Override
    public Long call(SQLDatabase db) throws AttachmentException, DocumentNotFoundException, DocumentStoreException {

        logger.entering("BasicDatastore",
                "doForceInsertExistingDocumentWithHistory",
                new Object[]{newRevision, revisions, attachments});
        Misc.checkNotNull(newRevision, "New document revision");
        Misc.checkArgument(new GetDocumentCallable(newRevision.getId(), null, attachmentsDir, attachmentStreamFactory).call(db) !=
                null, "DocumentRevisionTree must exist.");
        Misc.checkNotNull(revisions, "Revision history");
        Misc.checkArgument(revisions.size() > 0, "Revision history should have at least " +
                "one revision.");

        // do we have a common ancestor?
        long ancestorSequence = new GetSequenceCallable(newRevision.getId(), revisions.get(0)).call(db);

        long sequence;

        if (ancestorSequence == -1) {
            sequence = new InsertDocumentHistoryToNewTreeCallable(newRevision, revisions, docNumericId).call(db);
        } else {
            sequence = new InsertDocumentHistoryIntoExistingTreeCallable(newRevision, revisions,
                    docNumericId, attachments).call(db);
        }
        return sequence;
    }
}
