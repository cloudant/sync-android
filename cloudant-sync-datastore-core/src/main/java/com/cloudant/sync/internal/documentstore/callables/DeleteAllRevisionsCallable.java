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
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.documentstore.DocumentNotFoundException;
import com.cloudant.sync.internal.documentstore.InternalDocumentRevision;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Delete all Revisions for a given Document ID
 *
 * @api_private
 */
public class DeleteAllRevisionsCallable implements SQLCallable<List<DocumentRevision>> {

    private String id;

    public DeleteAllRevisionsCallable(String id) {
        this.id = id;
    }

    @Override
    public List<DocumentRevision> call(SQLDatabase db) throws DocumentStoreException, ConflictException,
            DocumentNotFoundException {

        ArrayList<DocumentRevision> deleted = new ArrayList<DocumentRevision>();
        Cursor cursor = null;
        // delete all in one tx
        try {
            // get revid for each leaf
            final String sql = "SELECT revs.revid FROM docs,revs " +
                    "WHERE revs.doc_id = docs.doc_id " +
                    "AND docs.docid = ? " +
                    "AND deleted = 0 AND revs.sequence NOT IN " +
                    "(SELECT DISTINCT parent FROM revs WHERE parent NOT NULL) ";

            cursor = db.rawQuery(sql, new String[]{id});
            while (cursor.moveToNext()) {
                String revId = cursor.getString(0);
                deleted.add(new DeleteDocumentCallable(id, revId).call(db));
            }
            return deleted;
        } catch (SQLException sqe) {
            throw new DocumentStoreException("SQLException in deleteDocument, not " +
                    "deleting revisions", sqe);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }
}
