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

import java.util.logging.Logger;

/**
 * Created by tomblench on 29/08/2017.
 */

public class PurgeCallable implements SQLCallable<Void> {

    private static final Logger logger = Logger.getLogger(PurgeCallable.class.getCanonicalName());

    private String documentId;

    private String revisionId;

    public PurgeCallable(String documentId, String revisionId) {
        this.documentId = documentId;
        this.revisionId = revisionId;
    }

    @Override
    public Void call(SQLDatabase db) throws Exception {
        System.out.println("PurgeCallable query");
        // list indexes and delete affected rows
        String sqlIndexNames = String.format("SELECT DISTINCT index_name FROM %s",
                QueryConstants.INDEX_METADATA_TABLE_NAME);
        Cursor cursorIndexNames = null;
        int nRowsTotal = 0;
        try {
            cursorIndexNames = db.rawQuery(sqlIndexNames, new String[]{});
            while (cursorIndexNames.moveToNext()) {
                String indexName = cursorIndexNames.getString(0);
                String tableName = QueryImpl.tableNameForIndex(indexName);
                int nRows = db.delete(tableName, "_id = ? and _rev = ?", new String[]{documentId, revisionId});
                if (nRows != 1) {
                    logger.warning(String.format("purge expected to delete 1 row but actually deleted %d rows",
                            nRows));
                }
                nRowsTotal += nRows;
            }
            logger.info(String.format("purged %d rows", nRowsTotal));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursorIndexNames);
        }
        return null;
    }

}
