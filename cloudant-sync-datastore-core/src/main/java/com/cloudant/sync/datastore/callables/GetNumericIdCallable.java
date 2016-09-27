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

import com.cloudant.sync.datastore.DatabaseImpl;
import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Get the internal numeric ID for a given Document ID
 *
 * @api_private
 */
public class GetNumericIdCallable implements SQLCallable<Long> {

    private String id;

    private static final Logger logger = Logger.getLogger(DatabaseImpl.class.getCanonicalName());

    /**
     * @param id Document ID to fetch the internal numeric ID for
     */
    public GetNumericIdCallable(String id) {
        this.id = id;
    }

    public Long call(SQLDatabase db) throws DatastoreException {
        Cursor cursor = null;
        try {
            String sql = DatabaseImpl.GET_DOC_NUMERIC_ID;
            cursor = db.rawQuery(sql, new String[]{id});
            if (cursor.moveToFirst()) {
                long sequence = cursor.getLong(0);
                return sequence;
            } else {
                return -1L;
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error sequence with id: " + id);
            throw new DatastoreException(String.format("Could not find sequence with id %s", id),
                    e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }
}
