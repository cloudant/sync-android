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

import com.cloudant.sync.internal.query.QueryImpl;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.query.QueryException;

import java.sql.SQLException;

/**
 * Created by rhys on 05/01/2017.
 */
public class DeleteIndexCallable implements SQLCallable<Void> {
    private final String indexName;

    public DeleteIndexCallable(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public Void call(SQLDatabase database) throws QueryException {
        try {
            // Drop the index table
            String tableName = QueryImpl.tableNameForIndex(indexName);
            String sql = String.format("DROP TABLE \"%s\"", tableName);
            database.execSQL(sql);

            // Delete the metadata entries
            String where = " index_name = ? ";
            database.delete(QueryImpl.INDEX_METADATA_TABLE_NAME, where, new String[]{indexName});
        } catch (SQLException e) {
            String msg = String.format("Failed to delete index: %s", indexName);
            throw new QueryException(msg, e);
        }
        return null;
    }
}
