/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.sqlite.sqlite4java;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteConstants;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.cloudant.sync.sqlite.Cursor;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * @api_private
 */
public class SQLiteWrapperUtils {

    private static final String LOG_TAG = "SQLiteWrapperUtils";
    private static final Logger logger = Logger.getLogger(SQLiteWrapperUtils.class.getCanonicalName());

    public static Long longForQuery(SQLiteConnection conn, String query)
            throws SQLiteException {
        return SQLiteWrapperUtils.longForQuery(conn, query, null);
    }

    /**
     * Utility method to run the query on the db and return the value in the
     * first column of the first row.
     */
    public static Long longForQuery(SQLiteConnection conn, String query, Object[] bindArgs)
            throws SQLiteException {
        SQLiteStatement stmt = null;
        try {
            stmt = conn.prepare(query);
            if (bindArgs != null && bindArgs.length > 0) {
                stmt = SQLiteWrapperUtils.bindArguments(stmt, bindArgs);
            }
            if (stmt.step()) {
                return stmt.columnLong(0);
            } else {
                throw new IllegalStateException("Query failed to return any result: " + query);
            }
        } finally {
            SQLiteWrapperUtils.disposeQuietly(stmt);
        }
    }

    /**
     * Utility method to run the query on the db and return the value in the
     * first column of the first row.
     */
    public static int intForQuery(SQLiteConnection conn, String query, Object[] bindArgs) throws SQLiteException {
        SQLiteStatement stmt = null;
        try {
            stmt = bindArguments(conn.prepare(query), bindArgs);
            if (stmt.step()) {
                return stmt.columnInt(0);
            } else {
                throw new IllegalStateException("Query failed to return any result: " + query);
            }
        } finally {
            SQLiteWrapperUtils.disposeQuietly(stmt);
        }
    }

    public static SQLiteStatement bindArguments(SQLiteStatement stmt, Object[] bindArgs)
            throws SQLiteException {

        if(bindArgs == null) {
            bindArgs = new Object[]{};
        }

        final int count = bindArgs != null ? bindArgs.length : 0;
        if (count != stmt.getBindParameterCount()) {
            throw new IllegalArgumentException(
                    "Expected " + stmt.getBindParameterCount() + " bind arguments but "
                            + bindArgs.length + " were provided.");
        }
        if (count == 0) {
            return stmt;
        }

        for (int i = 0; i < count; i++) {
            final Object arg = bindArgs[i];
            switch (DBUtils.getTypeOfObject(arg)) {
                case Cursor.FIELD_TYPE_NULL:
                    stmt.bindNull(i + 1);
                    break;
                case Cursor.FIELD_TYPE_INTEGER:
                    stmt.bind(i + 1, ((Number) arg).longValue());
                    break;
                case Cursor.FIELD_TYPE_FLOAT:
                    stmt.bind(i + 1, ((Number) arg).doubleValue());
                    break;
                case Cursor.FIELD_TYPE_BLOB:
                    stmt.bind(i + 1, (byte[]) arg);
                    break;
                case Cursor.FIELD_TYPE_STRING:
                default:
                    if (arg instanceof Boolean) {
                        // Provide compatibility with legacy applications which may pass
                        // Boolean values in bind args.
                        stmt.bind(i + 1, ((Boolean) arg).booleanValue() ? 1 : 0);
                    } else {
                        stmt.bind(i + 1, arg.toString());
                    }
                    break;
            }
        }

        return stmt;
    }

    static void disposeQuietly(SQLiteStatement stmt) {
        if (stmt != null && !stmt.isDisposed()) {
            try {
                stmt.dispose();
            } catch (Throwable e) {}
        }
    }

    public static SQLiteCursor buildSQLiteCursor(SQLiteConnection conn, String sql, Object[] bindArgs)
            throws SQLiteException {
        SQLiteStatement stmt = null;
        try {
            stmt = bindArguments(conn.prepare(sql), bindArgs);
            List<String> columnNames = null;
            List<Tuple> resultSet = new ArrayList<Tuple>();
            while (!stmt.hasStepped() || stmt.hasRow()) {
                if (!stmt.step()) {
                    break;
                }
                if (columnNames == null) {
                    columnNames = getColumnNames(stmt);
                }

                Tuple t = getDataRow(stmt);
                logger.finest("Tuple: "+ t.toString());
                resultSet.add(t);
            }
            return new SQLiteCursor(columnNames, resultSet);
        } finally {
            SQLiteWrapperUtils.disposeQuietly(stmt);
        }
    }

    static Tuple getDataRow(SQLiteStatement stmt) throws SQLiteException {
        logger.entering("com.cloudant.sync.sqlite.sqlite4java.SQLiteWrapperUtils","getDataRow",stmt);
        Tuple result = new Tuple(getColumnTypes(stmt));
        for (int i = 0; i < stmt.columnCount(); i++) {
            Integer type = stmt.columnType(i);
//            Log.v(LOG_TAG, "i: " + i + ", type: " + mapColumnType(type) + ", expected type: " + result.getType(i));
            switch (type) {
                case SQLiteConstants.SQLITE_NULL:
                    result.put(i);
                    break;
                case SQLiteConstants.SQLITE_TEXT:
                    result.put(i, stmt.columnString(i));
                    break;
                case SQLiteConstants.SQLITE_INTEGER:
                    result.put(i, stmt.columnLong(i));
                    break;
                case SQLiteConstants.SQLITE_FLOAT:
                    result.put(i, Double.valueOf(stmt.columnDouble(i)).floatValue());
                    break;
                case SQLiteConstants.SQLITE_BLOB:
                    result.put(i, stmt.columnBlob(i));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported data type: " + type);
            }
        }
        return result;
    }

    static List<String> getColumnNames(SQLiteStatement stmt) throws SQLiteException {
//        Log.v(LOG_TAG, "getColumnNames()");
        List<String> columnNames = new ArrayList<String>();
        int columnCount = stmt.columnCount();
        for (int i = 0; i < columnCount; i++) {
            columnNames.add(i, stmt.getColumnName(i));
        }
//        Log.v(LOG_TAG, "columnNames:" + columnNames);
        return columnNames;
    }

    static List<Integer> getColumnTypes(SQLiteStatement stmt) throws SQLiteException {
//        Log.v(LOG_TAG, "getColumnTypes()");
        List<Integer> columnTypes = new ArrayList<Integer>();
        int columnCount = stmt.columnCount();
        for (int i = 0; i < columnCount; i++) {
            columnTypes.add(i, mapColumnType(stmt.columnType(i)));
        }
//        Log.v(LOG_TAG, "columnTypes:" + columnTypes);
        return columnTypes;
    }

    static int mapColumnType(int columnType) {
        switch (columnType) {
            case SQLiteConstants.SQLITE_NULL:
                return Cursor.FIELD_TYPE_NULL;
            case SQLiteConstants.SQLITE_TEXT:
                return Cursor.FIELD_TYPE_STRING;
            case SQLiteConstants.SQLITE_INTEGER:
                return Cursor.FIELD_TYPE_INTEGER;
            case SQLiteConstants.SQLITE_FLOAT:
                return Cursor.FIELD_TYPE_FLOAT;
            case SQLiteConstants.SQLITE_BLOB:
                return Cursor.FIELD_TYPE_BLOB;
            default:
                throw new IllegalArgumentException("Unsupported data type? :" + columnType);
        }
    }
}
