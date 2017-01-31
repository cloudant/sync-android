/*
 * Copyright © 2015 IBM Corp. All rights reserved.
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

import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Retrieve all docIds from the database.
 */
public class GetAllDocumentIdsCallable implements SQLCallable<List<String>> {
    @Override
    public List<String> call(SQLDatabase db) throws Exception {
        List<String> docIds = new ArrayList<String>();
        String sql = "SELECT docs.docid FROM revs, docs " +
                "WHERE deleted = 0 AND current = 1 AND docs.doc_id = revs.doc_id";
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            while (cursor.moveToNext()) {
                docIds.add(cursor.getString(0));
            }
        } catch (SQLException sqe) {
            throw new DocumentStoreException(sqe);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
        return docIds;
    }
}
