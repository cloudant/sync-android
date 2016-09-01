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

import com.cloudant.sync.datastore.ConflictException;
import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.datastore.DatastoreImpl;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentNotFoundException;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.datastore.DocumentRevisionBuilder;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.CouchUtils;
import com.cloudant.sync.util.DatabaseUtils;
import com.cloudant.sync.util.JSONUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.sql.SQLException;

/**
 * Delete a Document for given Document ID and Revision ID.
 *
 * In the same manner as CouchDB, a new leaf Revision which is marked as deleted is created and made
 * the child Revision of the Revision to be deleted.
 *
 * @api_private
 */
public class DeleteDocumentCallable implements SQLCallable<DocumentRevision> {

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

    public DocumentRevision call(SQLDatabase db) throws ConflictException, DocumentNotFoundException, DatastoreException {

        Preconditions.checkArgument(!Strings.isNullOrEmpty(docId),
                "Input document id cannot be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(prevRevId),
                "Input previous revision id cannot be empty");

        CouchUtils.validateRevisionId(prevRevId);

        // get the sequence, numeric document id, current flag for the given revision - if it's a non-deleted leaf
        Cursor c = null;
        long sequence;
        long docNumericId;
        boolean current;
        try {
            // first check if it exists
            c = db.rawQuery(DatastoreImpl.GET_METADATA_GIVEN_REVISION, new String[]{docId, prevRevId});
            boolean exists = c.moveToFirst();
            if (!exists) {
                throw new DocumentNotFoundException();
            }
            // now check it's a leaf revision
            String leafQuery = "SELECT " + DatastoreImpl.METADATA_COLS + " FROM revs, docs WHERE " +
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
            throw new DatastoreException(sqe);
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

        // build up the document to return to the caller - it's quicker than re-querying the database
        // and we know all the values we need
        return new DocumentRevisionBuilder().setInternalId(docNumericId).setDocId(docId).setRevId(newRevisionId).setParent(sequence).
                setDeleted(true).setCurrent(current).setBody(DocumentBodyFactory.create(JSONUtils.emptyJSONObjectAsBytes())).
                setSequence(newSequence).build();
    }

}
