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

import com.cloudant.sync.documentstore.ConflictException;
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.internal.common.CouchUtils;
import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.documentstore.DocumentBodyFactory;
import com.cloudant.sync.documentstore.DocumentNotFoundException;
import com.cloudant.sync.internal.documentstore.InternalDocumentRevision;
import com.cloudant.sync.internal.documentstore.DocumentRevisionBuilder;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.util.DatabaseUtils;
import com.cloudant.sync.internal.util.JSONUtils;
import com.cloudant.sync.internal.util.Misc;

import java.sql.SQLException;

/**
 * Delete a Document for given Document ID and Revision ID.
 *
 * In the same manner as CouchDB, a new leaf Revision which is marked as deleted is created and made
 * the child Revision of the Revision to be deleted.
 *
 * @api_private
 */
public class DeleteDocumentCallable implements SQLCallable<InternalDocumentRevision> {

    String docId;
    String prevRevId;

    /**
     * @param docId     The Document ID of the Revision to be deleted
     * @param prevRevId The Revision ID of the Revision to be deleted
     */
    public DeleteDocumentCallable(String docId, String prevRevId) {
        this.docId = docId;
        this.prevRevId = prevRevId;
    }

    public InternalDocumentRevision call(SQLDatabase db) throws ConflictException,
            DocumentNotFoundException, DocumentStoreException {

        Misc.checkNotNullOrEmpty(docId, "Input document id");
        Misc.checkNotNullOrEmpty(prevRevId, "Input previous revision id");

        CouchUtils.validateRevisionId(prevRevId);

        // get the sequence, numeric document ID, current flag for the given revision - if it's a
        // non-deleted leaf
        Cursor c = null;
        long sequence;
        long docNumericId;
        boolean current;
        try {
            // first check if it exists
            c = db.rawQuery(CallableSQLConstants.GET_METADATA_GIVEN_REVISION, new String[]{docId,
                    prevRevId});
            boolean exists = c.moveToFirst();
            if (!exists) {
                throw new DocumentNotFoundException(docId);
            }
            // now check it's a leaf revision
            String leafQuery = "SELECT " + CallableSQLConstants.METADATA_COLS + " FROM revs, docs WHERE " +
                    "docs.docid=? AND revs.doc_id=docs.doc_id AND revid=? AND revs.sequence NOT " +
                    "IN (SELECT DISTINCT parent FROM revs WHERE parent NOT NULL) ";
            c = db.rawQuery(leafQuery, new String[]{docId, prevRevId});
            boolean isLeaf = c.moveToFirst();
            if (!isLeaf) {
                throw new ConflictException("Document has newer revisions than the revision " +
                        "passed to delete; get the newest revision of the document and try again.");
            }
            boolean isDeleted = c.getInt(c.getColumnIndex("deleted")) != 0;
            if (isDeleted) {
                throw new DocumentNotFoundException("Previous Revision is already deleted");
            }
            sequence = c.getLong(c.getColumnIndex("sequence"));
            docNumericId = c.getLong(c.getColumnIndex("doc_id"));
            current = c.getInt(c.getColumnIndex("current")) != 0;
        } catch (SQLException sqe) {
            throw new DocumentStoreException(sqe);
        } finally {
            DatabaseUtils.closeCursorQuietly(c);
        }

        new SetCurrentCallable(sequence, false).call(db);
        String newRevisionId = CouchUtils.generateNextRevisionId(prevRevId);
        // Previous revision to be deleted could be winner revision ("current" == true),
        // or a non-winner leaf revision ("current" == false), the new inserted
        // revision must have the same flag as it previous revision.
        // Deletion of non-winner leaf revision is mainly used when resolving
        // conflicts.
        InsertRevisionCallable callable = new InsertRevisionCallable();

        callable.docNumericId = docNumericId;
        callable.revId = newRevisionId;
        callable.parentSequence = sequence;
        callable.deleted = true;
        callable.current = current;
        callable.data = JSONUtils.emptyJSONObjectAsBytes();
        callable.available = false;
        long newSequence = callable.call(db);

        // build up the document to return to the caller - it's quicker than re-querying the
        // database and we know all the values we need
        return new DocumentRevisionBuilder()
                .setInternalId(docNumericId)
                .setDocId(docId)
                .setRevId(newRevisionId)
                .setParent(sequence)
                .setDeleted(true)
                .setCurrent(current)
                .setBody(DocumentBodyFactory.create(JSONUtils.emptyJSONObjectAsBytes()))
                .setSequence(newSequence)
                .build();
    }

}
