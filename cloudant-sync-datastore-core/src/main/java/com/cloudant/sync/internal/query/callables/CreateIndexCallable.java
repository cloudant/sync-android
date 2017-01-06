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

import com.cloudant.sync.internal.android.ContentValues;
import com.cloudant.sync.internal.query.QueryImpl;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.util.Misc;
import com.cloudant.sync.query.FieldSort;
import com.cloudant.sync.query.Index;
import com.cloudant.sync.query.IndexType;
import com.cloudant.sync.query.QueryException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Created by rhys on 06/01/2017.
 */
public class CreateIndexCallable implements SQLCallable<Void> {
    private final List<FieldSort> fieldNamesList;
    private final Index index;

    public CreateIndexCallable(List<FieldSort> fieldNamesList, Index index) {
        this.fieldNamesList = fieldNamesList;
        this.index = index;
    }

    @Override
    public Void call(SQLDatabase database) throws QueryException {

        // Insert metadata table entries
        for (FieldSort fieldName : fieldNamesList) {
            ContentValues parameters = new ContentValues();
            parameters.put("index_name", index.indexName);
            parameters.put("index_type", index.indexType.toString());
            parameters.put("index_settings", CreateIndexCallable.settingsAsJSON(index));
            parameters.put("field_name", fieldName.field);
            parameters.put("last_sequence", 0);
            long rowId = database.insert(QueryImpl.INDEX_METADATA_TABLE_NAME,
                    parameters);
            if (rowId < 0) {
                throw new QueryException("Error inserting index metadata");
            }
        }

        // Create SQLite data structures to support the index
        // For JSON index type create a SQLite table and a SQLite index
        // For TEXT index type create a SQLite virtual table
        List<String> columnList = new ArrayList<String>();
        for (FieldSort field : fieldNamesList) {
            columnList.add("\"" + field.field + "\"");
        }

        List<String> statements = new ArrayList<String>();
        if (index.indexType == IndexType.TEXT) {
            String settings = String.format("tokenize=%s", index.tokenize);
            statements.add(createVirtualTableStatementForIndex(index.indexName,
                    columnList,
                    Collections.singletonList(settings)));
        } else {
            statements.add(createIndexTableStatementForIndex(index.indexName, columnList));
            statements.add(createIndexIndexStatementForIndex(index.indexName, columnList));
        }
        for (String statement : statements) {
            try {
                database.execSQL(statement);
            } catch (SQLException e) {
                String msg = String.format("Index creation error occurred (%s):", statement);
                throw new QueryException(msg, e);
            }
        }
        return null;
    }

    /**
     * This method generates the virtual table create SQL for the specified index.
     * Note:  Any column that contains an '=' will cause the statement to fail
     * because it triggers SQLite to expect that a parameter/value is being passed in.
     *
     * @param indexName     the index name to be used when creating the SQLite virtual table
     * @param columns       the columns in the table
     * @param indexSettings the special settings to apply to the virtual table -
     *                      (only 'tokenize' is current supported)
     * @return the SQL to create the SQLite virtual table
     */
    private String createVirtualTableStatementForIndex(String indexName,
                                                       List<String> columns,
                                                       List<String> indexSettings) {
        String tableName = String.format(Locale.ENGLISH, "\"%s\"", QueryImpl
                .tableNameForIndex(indexName));
        String cols = Misc.join(",", columns);
        String settings = Misc.join(",", indexSettings);

        return String.format("CREATE VIRTUAL TABLE %s USING FTS4 ( %s, %s )", tableName,
                cols,
                settings);
    }

    private String createIndexTableStatementForIndex(String indexName, List<String> columns) {
        String tableName = String.format(Locale.ENGLISH, "\"%s\"", QueryImpl.tableNameForIndex(indexName));
        String cols = Misc.join(" NONE, ", columns);

        return String.format("CREATE TABLE %s ( %s NONE )", tableName, cols);
    }

    private String createIndexIndexStatementForIndex(String indexName, List<String> columns) {
        String tableName = QueryImpl.tableNameForIndex(indexName);
        String sqlIndexName = tableName.concat("_index");
        String cols = Misc.join(",", columns);

        return String.format(Locale.ENGLISH, "CREATE INDEX \"%s\" ON \"%s\" ( %s )", sqlIndexName, tableName, cols);
    }

    /**
     * Helper to convert the index settings to a JSON string
     *
     * @return the JSON representation of the index settings
     */
     static String settingsAsJSON(Index i) {
        // this is a trivial enough operation that we don't need a JSON serializer
        if (i.tokenize == null) {
            return "{}";
        }
        return "{\"tokenize\":\""+i.tokenize+"\"}";
    }
}
