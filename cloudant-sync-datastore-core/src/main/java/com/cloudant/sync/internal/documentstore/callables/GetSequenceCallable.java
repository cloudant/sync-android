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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Get the internal sequence number for a given a revision
 *
 * @api_private
 */
public class GetSequenceCallable implements SQLCallable<Long> {

    private String id;
    private String rev;

    private static final Logger logger = Logger.getLogger(DatabaseImpl.class.getCanonicalName());

    /**
     * @param id  Document ID of the revision to fetch the sequence for
     * @param rev Revision ID of the revision to fetch the sequence for
     */
    public GetSequenceCallable(String id, String rev) {
        this.id = id;
        this.rev = rev;
    }

    public Long call(SQLDatabase db) throws DocumentStoreException {
        Cursor cursor = null;
        try {
            String[] args = (rev == null) ? new String[]{id} : new String[]{id, rev};
            String sql = (rev == null) ? CallableSQLConstants.GET_METADATA_CURRENT_REVISION :
                    CallableSQLConstants.GET_METADATA_GIVEN_REVISION;
            cursor = db.rawQuery(sql, args);
            if (cursor.moveToFirst()) {
                long sequence = cursor.getLong(cursor.getColumnIndex("sequence"));
                return sequence;
            } else {
                return -1L;
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error sequence with id: " + id + "and rev " + rev, e);
            throw new DocumentStoreException(String.format("Could not find sequence with id %s at " +
                    "revision %s", id, rev), e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }
}
