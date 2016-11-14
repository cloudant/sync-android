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

package com.cloudant.sync.internal.documentstore.callables;

import com.cloudant.sync.internal.documentstore.AttachmentManager;
import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.util.Misc;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Insert a Revision @{code newRevision} into an existing Revision tree according to the history
 * described by @{code revisions}. Stub Revisions described by @{code revisions} are inserted where they do not
 * already exist
 *
 * @api_private
 */
public class InsertDocumentHistoryIntoExistingTreeCallable implements SQLCallable<Long> {

    private static final Logger logger = Logger.getLogger(DatabaseImpl.class.getCanonicalName());

    private DocumentRevision newRevision;
    private List<String> revisions;
    private Long docNumericID;
    private Map<String, Object> attachments;

    public InsertDocumentHistoryIntoExistingTreeCallable(DocumentRevision newRevision,
                                                         List<String> revisions, Long
                                                                 docNumericID, Map<String,
            Object> attachments) {
        this.newRevision = newRevision;
        this.revisions = revisions;
        this.docNumericID = docNumericID;
        this.attachments = attachments;
    }

    @Override
    public Long call(SQLDatabase db) throws DocumentStoreException {

        // get info about previous "winning" rev
        long previousLeafSeq = new GetSequenceCallable(newRevision.getId(), null).call(db);
        Misc.checkArgument(previousLeafSeq > 0, "Parent revision must exist");

        // Insert the new stub revisions, going down the tree
        // at the end of the loop, parentSeq will be the parent of our doc to insert
        long parentSeq = 0L;
        for (int i = 0; i < revisions.size() - 1; i++) {
            String revId = revisions.get(i);
            long seq = new GetSequenceCallable(newRevision.getId(), revId).call(db);
            if (seq == -1) {
                seq = DatabaseImpl.insertStubRevisionAdaptor(docNumericID, revId, parentSeq).call(db);
                new SetCurrentCallable(parentSeq, false).call(db);
            }
            parentSeq = seq;
        }

        // Insert the new leaf revision
        String newLeafRev = revisions.get(revisions.size() - 1);
        logger.finer("Inserting new revision, id: " + docNumericID + ", rev: " + newLeafRev);
        new SetCurrentCallable(parentSeq, false).call(db);
        // don't copy over attachments
        InsertRevisionCallable callable = new InsertRevisionCallable();
        callable.docNumericId = docNumericID;
        callable.revId = newLeafRev;
        callable.parentSequence = parentSeq;
        callable.deleted = newRevision.isDeleted();
        callable.current = false; // we'll call pickWinnerOfConflicts to set this if it needs it
        callable.data = newRevision.asBytes();
        callable.available = true;
        long newLeafSeq = callable.call(db);

        new PickWinningRevisionCallable(docNumericID).call(db);

        // copy stubbed attachments forward from last real revision to this revision
        if (attachments != null) {
            for (Map.Entry<String, Object> att : attachments.entrySet()) {
                Boolean stub = ((Map<String, Boolean>) att.getValue()).get("stub");
                if (stub != null && stub.booleanValue()) {
                    try {
                        AttachmentManager.copyAttachment(db, previousLeafSeq, newLeafSeq, att
                                .getKey());
                    } catch (SQLException sqe) {
                        logger.log(Level.SEVERE, "Error copying stubbed attachments", sqe);
                        throw new DocumentStoreException("Error copying stubbed attachments", sqe);
                    }
                }
            }
        }

        return newLeafSeq;
    }
}
