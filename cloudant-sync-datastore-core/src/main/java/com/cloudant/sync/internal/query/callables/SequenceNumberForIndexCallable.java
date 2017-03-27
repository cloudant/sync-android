/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 */

package com.cloudant.sync.internal.query.callables;

import com.cloudant.sync.internal.query.QueryConstants;
import com.cloudant.sync.internal.query.QueryImpl;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Retrieves the last sequence for a query index.
 */
public class SequenceNumberForIndexCallable implements SQLCallable<Long> {
    private final String indexName;
    private static final Logger logger = Logger.getLogger(SequenceNumberForIndexCallable.class.getName());

    public SequenceNumberForIndexCallable(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public Long call(SQLDatabase database) {
        long result = 0;
        String sql = String.format("SELECT last_sequence FROM %s WHERE index_name = ?",
                QueryConstants.INDEX_METADATA_TABLE_NAME);
        Cursor cursor = null;
        try {
            cursor = database.rawQuery(sql, new String[]{indexName});
            if (cursor.getCount() > 0) {
                // All rows for a given index will have the same last_sequence
                cursor.moveToNext();
                result = cursor.getLong(0);
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error getting last sequence number. ", e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
        return result;
    }
}
