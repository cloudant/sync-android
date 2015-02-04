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
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.cloudant.sync.sqlite.ContentValues;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.io.File;
import java.sql.SQLException;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Very simple implementation of SQLDatabase backed by sqlite4java. This is mainly used for testing.
 *
 * Since sqlite4java's SQLiteConnection does not really support multi-threading, as a matter of face, the
 * SQLiteConnection can only be used by the thread create/open it, ThreadLocal<SQLiteConnection> is created, so that
 * each thread (if there is any) is using their own connection.
 *
 * All the connection are closed when displose() is called.
 */
public class SQLiteWrapper extends SQLDatabase {

    private final static String LOG_TAG = "SQLiteWrapper";
    private static final Logger logger = Logger.getLogger(SQLiteWrapper.class.getCanonicalName());

    private static final String[] CONFLICT_VALUES = new String[]
            {"", " OR ROLLBACK ", " OR ABORT ", " OR FAIL ", " OR IGNORE ", " OR REPLACE "};

    private final String databaseFilePath;

    private SQLiteConnection localConnection;

    /**
     * Tracks whether the current nested set of transactions has had any
     * failed transactions so far.
     */
    private Boolean transactionNestedSetSuccess = Boolean.FALSE;

    /**
     * Stack to track whether the current transaction is successful.
     * As transactions are started, their status is pushed onto the stack.
     * When complete, the status is popped and used to update
     * {@see SQLiteWrapper#transactionNestedSetSuccess}
     */
    private Stack<Boolean> transactionStack = new Stack<Boolean>();

    public SQLiteWrapper(String databaseFilePath) {
        this.databaseFilePath = databaseFilePath;
    }

    public static SQLiteWrapper openSQLiteWrapper(String databaseFilePath) {
        SQLiteWrapper db = new SQLiteWrapper(databaseFilePath);
        db.open();
        return db;
    }

    public String getDatabaseFile() {
        return this.databaseFilePath;
    }

    SQLiteConnection getConnection() {
        if (localConnection == null) {
            localConnection = createNewConnection();
        }

        return localConnection;
    }

    SQLiteConnection createNewConnection() {
        try {
            SQLiteConnection conn = new SQLiteConnection(new File(this.databaseFilePath));
            conn.open();
            conn.setBusyTimeout(30*1000);
            return conn;
        } catch (SQLiteException ex) {
            throw new IllegalStateException("Failed to open database.", ex);
        }
    }

    @Override
    public void open() {
    }

    @Override
    public void compactDatabase() {
        try {
            this.execSQL("VACUUM");
        } catch (SQLException e) {
            String error = "Fatal error running 'VACUUM', the database is probably malfunctioning.";
            throw new IllegalStateException(error);
        }
    }

    @Override
    public int getVersion() {
        try {
            return SQLiteWrapperUtils.longForQuery(getConnection(), "PRAGMA user_version;").intValue();
        } catch (SQLiteException e) {
            throw new IllegalStateException("Can not query for the user_version?");
        }
    }

    @Override
    public boolean isOpen() {
        return getConnection().isOpen();
    }

    @Override
    public void beginTransaction() {
        Preconditions.checkState(this.isOpen(), "db must be open");

        // All transaction state variables are thread-local,
        // so we don't have to lock.

        // Start new set of nested transactions
        if(this.transactionStack.size() == 0) {
            try {
                this.execSQL("BEGIN EXCLUSIVE;");
            } catch (SQLException e) {
                String error = "Fatal error running 'BEGIN', the database is probably malfunctioning.";
                throw new IllegalStateException(error);
            }

            // We assume the set as a whole is successful. If any of the
            // transactions in the set fail, this will be set to false
            // before we commit or rollback.
            transactionNestedSetSuccess = true;
        }

        // This is set to true by setTransactionSuccessful(), if that method
        // is called. If it's still false at the end of this transaction,
        // transactionNestedSetSuccess is set to false.
        transactionStack.push(false);
    }

    @Override
    public void endTransaction() {
        Preconditions.checkState(this.isOpen(), "db must be open");
        Preconditions.checkState(this.transactionStack.size() >= 1,
                "TransactionStatus stack must not be empty");

        // All transaction state variables are thread-local,
        // so we don't have to lock.

        Boolean success = this.transactionStack.pop();
        if (!success) {
            transactionNestedSetSuccess =false;
        }

        if(this.transactionStack.size() == 0) {
            // We've reached the top of the stack, and need to commit or
            // rollback. At this point transactionNestedSetSuccess will be true
            // iff no transactions in the set failed.
            try {
                if (transactionNestedSetSuccess) {
                    this.execSQL("COMMIT;");
                } else {
                    this.execSQL("ROLLBACK;");
                }
            } catch (java.sql.SQLException e) {
                try {
                    this.execSQL("ROLLBACK;");
                } catch (Exception e2) {
                    String error = "Fatal error running 'ROLLBACK', the database is probably malfunctioning.";
                    throw new IllegalStateException(error);
                }
            }
        }
    }

    @Override
    public void setTransactionSuccessful() {
        Preconditions.checkState(this.isOpen(), "db must be open");

        // Pop the false value off and replace it with true.
        // As the stack is thread-local, this is thread-safe
        // and we need not lock.
        this.transactionStack.pop();
        this.transactionStack.push(true);
    }

    @Override
    public void close() {
        // it's not possible to call dispose from other threads
        // so the best we can do is call dispose on the connection
        // for the same thread as us
        SQLiteConnection conn = localConnection;
        if (conn != null && !conn.isDisposed()) {
            conn.dispose();
        }
    }

    @Override
    public void execSQL(String sql) throws SQLException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(sql.trim()),
                "Input SQL can not be empty String.");
        try {
            getConnection().exec(sql);
        } catch (SQLiteException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void execSQL(String sql, Object[] bindArgs) throws SQLException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(sql.trim()),
                "Input SQL can not be empty String.");
        SQLiteStatement stmt = null;
        try {
            stmt = this.getConnection().prepare(sql);
            stmt = SQLiteWrapperUtils.bindArguments(stmt, bindArgs);
            while (stmt.step()) {
            }
        } catch (SQLiteException e) {
            throw new SQLException(e);
        } finally {
            SQLiteWrapperUtils.disposeQuietly(stmt);
        }
    }

    @Override
    public int update(String table, ContentValues values, String whereClause, String[] whereArgs) {
        if (values == null || values.size() == 0) {
            throw new IllegalArgumentException("Empty values");
        }

        try {
            String updateQuery = QueryBuilder.buildUpdateQuery(table, values, whereClause, whereArgs);
            Object[] bindArgs = QueryBuilder.buildBindArguments(values, whereArgs);
            this.executeSQLStatement(updateQuery, bindArgs);
            return getConnection().getChanges();
        } catch (SQLiteException e) {
            logger.log(Level.SEVERE, String.format("Error updating: %1, %2, %3, %4", table,
                    values, whereClause, whereArgs), e);
            return -1;
        }
    }

    @Override
    public SQLiteCursor rawQuery(String sql, String[] bindArgs) throws SQLException {
        try {
            return SQLiteWrapperUtils.buildSQLiteCursor(getConnection(), sql, bindArgs);
        } catch (SQLiteException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int delete(String table, String whereClause, String[] whereArgs) {
        try {
            String sql = new StringBuilder("DELETE FROM ")
                    .append(table)
                    .append(!Strings.isNullOrEmpty(whereClause) ? " WHERE " +
                            whereClause : "")
                    .toString();
            this.executeSQLStatement(sql, whereArgs);
            return getConnection().getChanges();
        } catch (SQLiteException e) {
            return 0;
        }
    }

    @Override
    public long insertWithOnConflict(String table, ContentValues initialValues, int conflictAlgorithm) {
        int size = (initialValues != null && initialValues.size() > 0)
                ? initialValues.size() : 0;

        if (size == 0) {
            throw new IllegalArgumentException("SQLite does not support to insert an all null row");
        }

        try {
            StringBuilder sql = new StringBuilder();
            sql.append("INSERT ");
            sql.append(CONFLICT_VALUES[conflictAlgorithm]);
            sql.append(" INTO ");
            sql.append(table);
            sql.append('(');


            Object[] bindArgs  = new Object[size];
            int i = 0;
            for (String colName : initialValues.keySet()) {
                sql.append((i > 0) ? "," : "");
                sql.append(colName);
                bindArgs[i++] = initialValues.get(colName);
            }
            sql.append(')');
            sql.append(" VALUES (");
            for (i = 0; i < size; i++) {
                sql.append((i > 0) ? ",?" : "?");
            }

            sql.append(')');
            this.executeSQLStatement(sql.toString(), bindArgs);
            return getConnection().getLastInsertId();
        } catch (SQLiteException e) {
            logger.log(Level.SEVERE, String.format("Error inserting to: %s, %s, %s", table,
                    initialValues, CONFLICT_VALUES[conflictAlgorithm]), e);
            return -1;
        }
    }

    @Override
    public long insert(String table, ContentValues initialValues) {
        return insertWithOnConflict(table, initialValues, CONFLICT_NONE);
    }

    private void executeSQLStatement(String sql, Object[] values) throws SQLiteException{
        SQLiteStatement stmt = getConnection().prepare(sql);
        stmt = SQLiteWrapperUtils.bindArguments(stmt, values);
        while (stmt.step()) {
        }
    }
}
