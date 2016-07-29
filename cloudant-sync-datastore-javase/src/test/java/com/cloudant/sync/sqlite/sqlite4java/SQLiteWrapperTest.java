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
import com.cloudant.android.ContentValues;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.SQLDatabaseTestUtils;
import com.cloudant.sync.util.TestUtils;
import com.google.common.base.Strings;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;

public class SQLiteWrapperTest {

    private String database_dir ;

    // Single thread, single connection for now
    private SQLiteConnection conn = null;
    private SQLiteWrapper database = null;

    private final static String doc_table_name = "docs";
    private final static String create_doc_table = " CREATE TABLE docs (  doc_id INTEGER PRIMARY KEY, doc_name TEXT NOT NULL" +
            ", desc TEXT, balance REAL NOT NULL, data BLOB); ";
    private final static String create_rev_table = " CREATE TABLE revs (  rev_id INTEGER PRIMARY KEY )";

    private final static String DELETE_REVS_TABLE = "DROP TABLE revs;";
    private final static String DELETE_DOCS_TABLE = "DROP TABLE docs;";

    private final static String insert_into_docs = " INSERT INTO docs VALUES (?, ?, ?, ?, ?)";

    @Before
    public void setUp() throws Exception {
        this.database_dir = TestUtils.createTempTestingDir(SQLiteWrapperTest.class.getName());

        this.database = (SQLiteWrapper)TestUtils.createEmptyDatabase(database_dir, SQLiteWrapperTest.class.getName());
        this.database.open();
        this.conn = this.database.getConnection();
    }

    @After
    public void tearDown() throws Exception {
        database.close();
        TestUtils.deleteDatabaseQuietly(database);
        TestUtils.deleteTempTestingDir(database_dir);
    }

    @Test
    public void verify_config() throws SQLiteException {
        Assert.assertTrue(conn.getAutoCommit());
    }

    @Test
    public void compactDatabase() throws Exception {
        database.compactDatabase();
    }

    @Test
    public void getVersion_NA_NA() throws Exception {
        database.execSQL("PRAGMA user_version = 101;");
        Assert.assertTrue(database.getVersion() == 101);
    }

    @Test
    public void compact_NA_NA() throws Exception {
        database.compactDatabase();
    }

    @Test
    public void beginTransaction_transactionFailed_rollback() throws Exception {
        database.beginTransaction();
        try {
            database.execSQL(create_doc_table);
        } finally {
            database.endTransaction();
        }

        SQLDatabaseTestUtils.assertTablesNotExist(this.database, "docs");
    }

    @Test
    public void beginTransaction_transactionSuccess_commit() throws Exception {
        database.beginTransaction();
        try {
            database.execSQL(create_doc_table);
            database.setTransactionSuccessful();
        } finally {
            database.endTransaction();
        }
        SQLDatabaseTestUtils.assertTablesExist(this.database, "docs");
    }

    /**
     * Nested transaction structure:
     *
     * 1 -> 2
     *
     */
    @Test
    public void nestedTransaction_twoChainedTransactionAndAllTransactionSuccess_commit() throws SQLException {
        this.database.beginTransaction();
        try {
            this.nestedSuccessfulTransaction(create_doc_table);
            this.database.setTransactionSuccessful();
        } finally {
            this.database.endTransaction();
        }

        SQLDatabaseTestUtils.assertTablesExist(this.database, "docs");
    }

    /**
     * Nested transaction structure:
     *
     * 1 -> 2
     *
     * And, transaction 2 failed.
     */
    @Test
    public void nestedTransaction_twoChainedTransactionAndOneChildFailed_rollback() throws SQLException {
        this.database.beginTransaction();
        try {
            this.nestedFailTransaction(create_doc_table);
            this.database.setTransactionSuccessful();
        } finally {
            this.database.endTransaction();
        }

        SQLDatabaseTestUtils.assertTablesNotExist(this.database, "docs");
    }

    private void nestedSuccessfulTransaction(String sql) throws SQLException {
        this.database.beginTransaction();
        try {
            this.database.execSQL(sql);
            this.database.setTransactionSuccessful();
        } finally {
            this.database.endTransaction();
        }
    }

    private void nestedFailTransaction(String sql) throws SQLException {
        this.database.beginTransaction();
        try {
            this.database.execSQL(sql);
        } finally {
            this.database.endTransaction();
        }
    }

    /**
     * Nested transaction structure:
     *
     * 1 -> 2 -> 3
     *
     */
    @Test
    public void nestedTransaction_threeChainedTransactionAndAllTransactionSuccess_commit() throws SQLException {
        this.database.beginTransaction();
        try {
            this.database.beginTransaction();
            try {
                this.database.execSQL(create_doc_table);
                this.nestedSuccessfulTransaction(create_rev_table);
                this.database.setTransactionSuccessful();
            } finally {
                this.database.endTransaction();
            }
            this.database.setTransactionSuccessful();
        } finally {
            this.database.endTransaction();
        }

        SQLDatabaseTestUtils.assertTablesExist(this.database, "docs", "revs");
    }

    /**
     * Nested transaction structure:
     *
     * 1 -> 2 -> 3
     *
     *
     * And, transaction 3 failed.
     */
    @Test
    public void nestedTransaction_threeChainedTransactionAndChildThreeFailed_rollback() throws SQLException {
        this.database.beginTransaction();
        try {
            this.database.beginTransaction();
            try {
                this.database.execSQL(create_doc_table);
                this.nestedFailTransaction(create_rev_table);
                this.database.setTransactionSuccessful();
            } finally {
                this.database.endTransaction();
            }
            this.database.setTransactionSuccessful();
        } finally {
            this.database.endTransaction();
        }

        SQLDatabaseTestUtils.assertTablesNotExist(this.database, "docs", "revs");
    }

    /**
     * Nested transaction structure:
     *
     * 1 -> 2 -> 3
     *
     *        _3_S_
     *    _2_/     \_F_
     * _1/             \_S__
     *
     * And, transaction 2 failed.
     */
    @Test
    public void nestedTransaction_threeChainedTransactionAndChildTwoFailed_rollback() throws SQLException {
        this.database.beginTransaction();
        try {
            this.database.beginTransaction();
            try {
                this.database.execSQL(create_doc_table);
                this.nestedSuccessfulTransaction(create_rev_table);
            } finally {
                this.database.endTransaction();
            }
            this.database.setTransactionSuccessful();
        } finally {
            this.database.endTransaction();
        }

        SQLDatabaseTestUtils.assertTablesNotExist(this.database, "docs", "revs");
    }

    /**
     * Nested transaction structure:
     *
     * 1 -> 2
     *   \-> 3
     */
    @Test
    public void nestedTransaction_twoLevelThreeTransactionAndAllTransactionSuccess_commit() throws SQLException {
        this.database.beginTransaction();
        try {
            this.nestedSuccessfulTransaction(create_doc_table);
            this.nestedSuccessfulTransaction(create_rev_table);
            this.database.setTransactionSuccessful();
        } finally {
            this.database.endTransaction();
        }

        SQLDatabaseTestUtils.assertTablesExist(this.database, "docs", "revs");
    }

    /**
     * Nested transaction structure:
     *
     * 1 -> 2
     *   \-> 3
     *
     * But here we simulate the case where a method completes all its
     * transactions successfully before calling another nested transaction.
     */
    @Test
    public void nestedTransaction_markSuccessBeforeFailedTransaction_rollback() throws SQLException {
        this.database.beginTransaction();
        try {
            this.nestedSuccessfulTransaction(create_doc_table);
            this.database.setTransactionSuccessful();
            this.nestedFailTransaction(create_rev_table);
        } finally {
            this.database.endTransaction();
        }

        SQLDatabaseTestUtils.assertTablesNotExist(this.database, "docs", "revs");
    }

    /**
     * Nested transaction structure:
     *
     * 1 -> 2
     *   \-> 3
     *
     * But here we simulate the case where a method completes all its
     * transactions successfully before calling another nested transaction.
     */
    @Test
    public void nestedTransaction_markSuccessBeforeSuccessfulTransaction_commit() throws SQLException {
        this.database.beginTransaction();
        try {
            this.nestedSuccessfulTransaction(create_doc_table);
            this.database.setTransactionSuccessful();
            this.nestedSuccessfulTransaction(create_rev_table);
        } finally {
            this.database.endTransaction();
        }

        SQLDatabaseTestUtils.assertTablesExist(this.database, "docs", "revs");
    }

    /**
     * Nested transaction structure:
     *
     * 1 -> 2
     *   \-> 3
     *
     * And, transaction 3 failed.
     */
    @Test
    public void nestedTransaction_twoLevelThreeTransactionAndOneChildFailed_rollback() throws SQLException {
        this.database.beginTransaction();
        try {
            this.nestedSuccessfulTransaction(create_doc_table);
            this.nestedFailTransaction(create_rev_table);
            this.database.setTransactionSuccessful();
        } finally {
            this.database.endTransaction();
        }

        SQLDatabaseTestUtils.assertTablesNotExist(this.database, "docs", "revs");
    }

    /**
     * Make sure we can run multiple transactions:
     *
     *      _2_S_                   _2_F_                   _2_S_
     *  _1_/     \_S_           _1_/     \_S_           _1_/     \_S_
     * /             \_commits_/             \_rollback_/             \_commit
     *
     */
    @Test
    public void nestedTransaction_serialTransactions() throws SQLException {
        this.database.beginTransaction();
        try {
            this.nestedSuccessfulTransaction(create_rev_table);
            this.database.setTransactionSuccessful();
        } finally {
            this.database.endTransaction();
        }
        SQLDatabaseTestUtils.assertTablesExist(this.database, "revs");

        this.database.beginTransaction();
        try {
            this.nestedFailTransaction(DELETE_REVS_TABLE);
            this.database.setTransactionSuccessful();
        } finally {
            this.database.endTransaction();
        }
        SQLDatabaseTestUtils.assertTablesExist(this.database, "revs");

        this.database.beginTransaction();
        try {
            this.nestedSuccessfulTransaction(DELETE_REVS_TABLE);
            this.database.setTransactionSuccessful();
        } finally {
            this.database.endTransaction();
        }
        SQLDatabaseTestUtils.assertTablesNotExist(this.database, "revs");
    }

    /**
     * Test a transaction works in a different thread to the one that the
     * database was constructed in.
     *
     * Transaction structure:
     *
     *        _3_S_
     *    _2_/     \_S_
     * _1/             \_S__
     *
     * This test is ignored because we can't close connections in SQLite4java
     * from a thread other than the thread which created the connection. This
     * means that the test always fails in the teardown stage.
     *
     * TODO: Fix threading issues for SQLite4Java
     */
    @Test
    @Ignore
    public void nestedTransaction_runInNewThread()
            throws SQLException, InterruptedException {

        Thread t = new Thread() {
            @Override
            public void run() {
                database.beginTransaction();
                try {
                    nestedSuccessfulTransaction(create_doc_table);
                    nestedSuccessfulTransaction(create_rev_table);
                    database.setTransactionSuccessful();
                } catch (SQLException ex) {
                    // do nothing
                } finally {
                    database.endTransaction();
                }
            }
        };

        t.start();
        t.join();

        SQLDatabaseTestUtils.assertTablesExist(this.database, "docs", "revs");
    }

    @Test(expected = IllegalArgumentException.class)
    public void execSQL_emptySQL_exception() throws SQLException {
        database.execSQL(" ");
    }

    @Test
    public void execSQL_someCreateTablesStatements_NA() throws Exception {
        database.execSQL(create_doc_table);
    }

    void prepareDatabaseForTesting() {
        try {
            database.execSQL(create_doc_table);
            database.execSQL(insert_into_docs, new Object[]{1, "haha", "this is great!", "1990.001", "this is a blob".getBytes()});
            database.execSQL(insert_into_docs, new Object[]{2, "haha", "", "-0.09999", "this is another blob".getBytes()});
            database.execSQL(insert_into_docs, new Object[]{3, "hihi", "", "1.0", "".getBytes()});
            database.execSQL(insert_into_docs, new Object[]{4, "hehe", "", "1.0", null});
        } catch (SQLException e) {
            Assert.fail("Can not prepare database for testing: " + e.getMessage());;
        }
    }

    @Test
    public void update() throws Exception {
        prepareDatabaseForTesting();

        int n = SQLiteWrapperUtils.intForQuery(conn, "SELECT count(*) FROM docs", new Object[]{});
        Assert.assertEquals(4, n);

        int n2 = SQLiteWrapperUtils.intForQuery(conn, "SELECT count(*) FROM docs WHERE doc_name = ?", new Object[]{"haha"});
        Assert.assertEquals(2, n2);

        ContentValues cv = new ContentValues();
        cv.put("doc_name", "yaya");
        String whereClause = " doc_name = ? ";
        String[] whereArgs = new String[]{"haha"};
        Assert.assertEquals(2, database.update(doc_table_name, cv, whereClause, whereArgs));

        ContentValues cv2 = new ContentValues();
        cv2.put("doc_name", "lala");
        String whereClause2 = " doc_id = ? ";
        String[] whereArgs2 = new String[]{"3"};
        Assert.assertEquals(1, database.update(doc_table_name, cv2, whereClause2, whereArgs2));
    }

    @Test
    public void rawQuery() throws Exception {
        prepareDatabaseForTesting();

        SQLiteCursor cursor = database.rawQuery("SELECT * FROM docs WHERE doc_name = ?",
                new String[]{"haha"});

        Assert.assertTrue(cursor.getCount() == 2);

        Assert.assertTrue(cursor.moveToFirst());

        Assert.assertEquals(Cursor.FIELD_TYPE_INTEGER, cursor.columnType(0));
        Assert.assertEquals(Cursor.FIELD_TYPE_STRING, cursor.columnType(1));
        Assert.assertEquals("doc_id", cursor.columnName(0));
        Assert.assertEquals("doc_name", cursor.columnName(1));

        Assert.assertTrue(1 == cursor.getInt(0));
        Assert.assertTrue("haha".equals(cursor.getString(1)));
        Assert.assertTrue("this is great!".equals(cursor.getString(2)));
        Assert.assertEquals(1990.001f, cursor.getFloat(3), 0.000001f);
        Assert.assertTrue(Arrays.equals("this is a blob".getBytes(), cursor.getBlob(4)));

        cursor.moveToNext();
        Assert.assertTrue(2 == cursor.getInt(0));
        Assert.assertTrue("haha".equals(cursor.getString(1)));
        Assert.assertTrue(Strings.isNullOrEmpty(cursor.getString(2)));
        Assert.assertEquals(-0.09999f, cursor.getFloat(3), 0.000001f);
        Assert.assertTrue(Arrays.equals("this is another blob".getBytes(), cursor.getBlob(4)));
    }

    @Test
    public void rawQuery_inClause() throws Exception {
        prepareDatabaseForTesting();

        SQLiteCursor cursor = database.rawQuery("SELECT * FROM docs WHERE doc_name IN ( ?, ?, ?)",
                new String[]{"haha", "hihi", "hehe"});

        Assert.assertEquals(4, cursor.getCount());
    }

    @Test
    public void delete() {
        prepareDatabaseForTesting();
        int i = database.delete("docs", " doc_name = ? ", new String[]{"haha"});
        Assert.assertEquals(2, i);
    }

    @Test
    public void insert() {
        prepareDatabaseForTesting();

        ContentValues cv = new ContentValues();
        cv.put("doc_id", 101);
        cv.put("doc_name", "kaka");
        cv.put("balance", "-299.99");

        long id = database.insert(doc_table_name, cv);
        Assert.assertTrue(101 == id);

        ContentValues cv2 = new ContentValues();
        cv2.put("doc_id", 201);
        cv2.put("doc_name", "kaka");
        cv2.put("balance", "-299.99");
        long id2 = database.insert(doc_table_name, cv2);
        Assert.assertTrue(201 == id2);

        long id3 = database.insert(doc_table_name, cv2);
        Assert.assertTrue( id3 == -1);
    }

    @Test
    public void insertWithOnConflict() {
        prepareDatabaseForTesting();

        {
            ContentValues cv = new ContentValues();
            cv.put("doc_id", 101);
            cv.put("doc_name", "kaka");
            cv.put("balance", "-299.99");

            long id = database.insertWithOnConflict(doc_table_name, cv, SQLDatabase.CONFLICT_NONE);
            Assert.assertEquals(101, id);
        }

        {
            ContentValues cv2 = new ContentValues();
            cv2.put("doc_id", 101);
            cv2.put("doc_name", "kaka");
            cv2.put("balance", "-299.99");

            long id2 = database.insertWithOnConflict(doc_table_name, cv2, SQLDatabase.CONFLICT_REPLACE);
            Assert.assertEquals(101, id2);
        }
    }

    @Test(expected = SQLException.class)
    public void close_queryAfterClose() throws SQLException {
        this.database.close();
        this.database.execSQL("PRAGMA user_version = 101;");
    }
}
