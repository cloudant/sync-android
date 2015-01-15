/**
 * Copyright (C) 2013 Cloudant, Inc.
 *
 * Copyright (C) 2006 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudant.sync.sqlite;

import java.sql.SQLException;

public abstract class SQLDatabase {

    public String filename;


    /**
     * When a constraint violation occurs, an immediate ROLLBACK occurs,
     * thus ending the current transaction, and the command aborts with a
     * return code of SQLITE_CONSTRAINT. If no transaction is active
     * (other than the implied transaction that is created on every command)
     * then this algorithm works the same as ABORT.
     */
    public static final int CONFLICT_ROLLBACK = 1;

    /**
     * When a constraint violation occurs,no ROLLBACK is executed
     * so changes from prior commands within the same transaction
     * are preserved. This is the default behavior.
     */
    public static final int CONFLICT_ABORT = 2;

    /**
     * When a constraint violation occurs, the command aborts with a return
     * code SQLITE_CONSTRAINT. But any changes to the database that
     * the command made prior to encountering the constraint violation
     * are preserved and are not backed out.
     */
    public static final int CONFLICT_FAIL = 3;

    /**
     * When a constraint violation occurs, the one row that contains
     * the constraint violation is not inserted or changed.
     * But the command continues executing normally. Other rows before and
     * after the row that contained the constraint violation continue to be
     * inserted or updated normally. No error is returned.
     */
    public static final int CONFLICT_IGNORE = 4;

    /**
     * When a UNIQUE constraint violation occurs, the pre-existing rows that
     * are causing the constraint violation are removed prior to inserting
     * or updating the current row. Thus the insert or update always occurs.
     * The command continues executing normally. No error is returned.
     * If a NOT NULL constraint violation occurs, the NULL value is replaced
     * by the default value for that column. If the column has no default
     * value, then the ABORT algorithm is used. If a CHECK constraint
     * violation occurs then the IGNORE algorithm is used. When this conflict
     * resolution strategy deletes rows in order to satisfy a constraint,
     * it does not invoke delete triggers on those rows.
     * This behavior might change in a future release.
     */
    public static final int CONFLICT_REPLACE = 5;

    /**
     * Use the following when no conflict action is specified.
     */
    public static final int CONFLICT_NONE = 0;

    /**
     * Execute a single SQL statement that is NOT a SELECT/INSERT/UPDATE/DELETE.
     *
     * @param sql the SQL statement to be executed. Multiple statements separated by semicolons are
     * not supported.
     * @param bindArgs only byte[], String, Long and Double are supported in bindArgs.
     * @throws java.sql.SQLException if the SQL string is invalid
     */
    public abstract void execSQL(String sql, Object[] bindArgs) throws SQLException;

    /**
     * Execute a single SQL statement that is NOT a SELECT
     * or any other SQL statement that returns data.
     *
     * @param sql the SQL statement to be executed. Multiple statements separated by semicolons are
     * not supported.
     * @throws java.sql.SQLException if the SQL string is invalid
     */
    public abstract void execSQL(String sql) throws SQLException;

    /**
     * For SQLite database, this is to call:
     *
     *         this.execSQL("VACUUM");
     *
     * @throws java.sql.SQLException
     */
    public abstract void compactDatabase();

    /**
     * Gets the database version, and SQLDatabase's version is defined as:</p>
     *
     * <pre>    PRAGMA user_version;</pre>
     *
     * @return the database version
     *
     * @see <a href="http://www.sqlite.org/pragma.html#pragma_schema_version">SQLite user_version</a>
     */
    public abstract int getVersion();

    /**
     * Open the database
     */
    public abstract void open();

    /**
     * Close the database
     */
    public abstract void close();

    /**
     * @return true if the database is open
     */
    public abstract boolean isOpen();

    /**
     * Begins a transaction in EXCLUSIVE mode.
     * <p>
     * Transactions can be nested.
     * When the outer transaction is ended all of
     * the work done in that transaction and all of the nested transactions will be committed or
     * rolled back. The changes will be rolled back if any transaction is ended without being
     * marked as clean (by calling setTransactionSuccessful). Otherwise they will be committed.
     * </p>
     * <p>Here is the standard idiom for transactions:
     *
     * <pre>
     *   db.beginTransaction();
     *   try {
     *     ...
     *     db.setTransactionSuccessful();
     *   } finally {
     *     db.endTransaction();
     *   }
     * </pre>
     */
     public abstract void beginTransaction();

    /**
     * End a transaction. See beginTransaction for notes about how to use this and when transactions
     * are committed and rolled back.
     */
     public abstract void endTransaction();

    /**
     * Marks the current transaction as successful. Do not do any more database work between
     * calling this and calling endTransaction. Do as little non-database work as possible in that
     * situation too. If any errors are encountered between this and endTransaction the transaction
     * will still be committed.
     *
     * @throws IllegalStateException if the current thread is not in a transaction or the
     * transaction is already marked as successful.
     */
     public abstract void setTransactionSuccessful();

    /**
     * Convenience method for updating rows in the database.
     *
     * @param table the table to update in
     * @param values a map from column names to new column values. null is a
     *            valid value that will be translated to NULL.
     * @param whereClause the optional WHERE clause to apply when updating.
     *            Passing null will update all rows.
     * @return the number of rows affected
     */
    public abstract int update(String table, com.cloudant.sync.sqlite.ContentValues values,
                      String whereClause, String[] whereArgs);

    /**
     * Runs the provided SQL and returns a {@link com.cloudant.sync.sqlite.Cursor} over the result set.
     *
     * @param sql the SQL query. The SQL string must not be ; terminated
     * @param selectionArgs You may include ?s in where clause in the query,
     *     which will be replaced by the values from selectionArgs. The
     *     values will be bound as Strings.
     * @return A {@link com.cloudant.sync.sqlite.Cursor} object, which is positioned before the first entry. Note that
     * {@link com.cloudant.sync.sqlite.Cursor}s are not synchronized, see the documentation for more details.
     */
    public abstract Cursor rawQuery(String sql, String[] selectionArgs) throws SQLException;

    /**
     * Convenience method for deleting rows in the database.
     *
     * @param table the table to delete from
     * @param whereClause the optional WHERE clause to apply when deleting.
     *            Passing null will delete all rows. And, do not include
     *            "where" in the clause.
     * @return the number of rows affected if a whereClause is passed in, 0
     *         otherwise. To delete all rows and get a count pass "1" as the
     *         whereClause.
     */
    public abstract int delete(String table, String whereClause, String[] whereArgs);

    /**
     * Convenience method for inserting a row into the database.
     *
     * @param table the table to insert the row into
     *
     * @param values this map contains the initial column values for the
     *            row. The keys should be the column names and the values the
     *            column values
     * @return the row ID of the newly inserted row, or -1 if an error occurred
     */
    public abstract long insert(String table, com.cloudant.sync.sqlite.ContentValues values);

    /**
     *
     * @param table
     * @param initialValues
     * @param conflictAlgorithm
     *
     * @return the row ID of the newly inserted row OR the primary key of the existing row if the input param 'conflictAlgorithm' = CONFLICT_IGNORE OR -1 if any error
     */
    public abstract long insertWithOnConflict(String table, ContentValues initialValues, int conflictAlgorithm);

}