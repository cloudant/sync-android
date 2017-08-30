package com.cloudant.sync.internal.query.callables;

import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.internal.query.QueryConstants;
import com.cloudant.sync.internal.query.QueryImpl;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.query.Index;

import java.util.ArrayList;
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
            cursorIndexNames.close();
        }
        return null;
    }

}
