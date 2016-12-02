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

import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Get all document IDs of Documents having conflicted Revisions: more than one non-deleted leaf
 * Revision
 *
 * @api_private
 */
public class GetConflictedDocumentIdsCallable implements SQLCallable<List<String>> {

    private static final Logger logger = Logger.getLogger(DatabaseImpl.class.getCanonicalName());

    @Override
    public List<String> call(SQLDatabase db) throws Exception {
        // the "SELECT DISTINCT ..." subquery selects all the parent
        // sequence, and so the outer "SELECT ..." practically selects
        // all the leaf nodes. The "GROUP BY" and "HAVING COUNT(*) > 1"
        // make sure only those document with more than one leafs are
        // returned.
        final String sql = "SELECT docs.docid, COUNT(*) FROM docs,revs " +
                "WHERE revs.doc_id = docs.doc_id " +
                "AND deleted = 0 AND revs.sequence NOT IN " +
                "(SELECT DISTINCT parent FROM revs WHERE parent IS NOT NULL) " +
                "GROUP BY docs.docid HAVING COUNT(*) > 1";

        List<String> conflicts = new ArrayList<String>();
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            while (cursor.moveToNext()) {
                String docId = cursor.getString(0);
                conflicts.add(docId);
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error getting conflicted document: ", e);
            throw new DocumentStoreException(e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
        return conflicts;

    }
}
