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

import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Checks the supplied collection of revisions for the given document ID and returns a
 * collection containing only those entries that are missing from the database.
 *
 * @api_private
 */

public class RevsDiffBatchCallable implements SQLCallable<Collection<String>> {

    private String docId;
    private Set<String> missingRevs;

    /**
     * @param docId the doc ID to check
     * @param revs  the rev IDs to check
     */
    public RevsDiffBatchCallable(String docId, Collection<String> revs) {
        this.docId = docId;
        // Consider all missing to start
        this.missingRevs = new HashSet<String>(revs);
    }

    /**
     * @return collection of rev IDs not present in the database
     */
    @Override
    public Collection<String> call(SQLDatabase db) throws Exception {

        final String sql = String.format(
                "SELECT revs.revid FROM docs, revs " +
                        "WHERE docs.doc_id = revs.doc_id AND docs.docid = ? AND revs.revid IN " +
                        "(%s) ", DatabaseUtils.makePlaceholders(missingRevs.size()));

        String[] args = new String[1 + missingRevs.size()];
        args[0] = docId;
        // Copy the revisions into the end of the args array
        System.arraycopy(missingRevs.toArray(new String[missingRevs.size()]), 0, args, 1,
                missingRevs.size());

        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, args);
            while (cursor.moveToNext()) {
                String revId = cursor.getString(cursor.getColumnIndex("revid"));
                missingRevs.remove(revId);
            }
        } catch (SQLException e) {
            throw new DatastoreException(e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
        return missingRevs;
    }
}
