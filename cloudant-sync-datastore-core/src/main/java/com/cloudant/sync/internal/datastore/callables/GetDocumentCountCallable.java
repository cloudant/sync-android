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

import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.internal.datastore.DatabaseImpl;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Get the total number of Documents in the database.
 *
 * Documents where all revisions are deleted are not counted.
 *
 * @api_private
 */
public class GetDocumentCountCallable implements SQLCallable<Integer> {

    private static final Logger logger = Logger.getLogger(DatabaseImpl.class.getCanonicalName());

    @Override
    public Integer call(SQLDatabase db) throws Exception {
        String sql = "SELECT COUNT(DISTINCT doc_id) FROM revs WHERE current=1 AND deleted=0";
        Cursor cursor = null;
        int result = 0;
        try {
            cursor = db.rawQuery(sql, null);
            if (cursor.moveToFirst()) {
                result = cursor.getInt(0);
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error getting document count", e);
            throw new DocumentStoreException(e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
        return result;
    }

}
