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

package com.cloudant.sync.datastore;

import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLDatabaseFactory;
import com.cloudant.sync.sqlite.sqlite4java.SQLiteWrapper;
import com.cloudant.sync.util.SQLDatabaseTestUtils;
import com.cloudant.sync.util.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Set;

public class SQLDatabaseFactoryTest {

    String database_dir;
    private String database_file = "database_initializer_test";

    String SCHEMA_1 =
            "    CREATE TABLE person ( " +
            "        id INTEGER PRIMARY KEY, " +
            "        name TEXT UNIQUE NOT NULL); " +
            "    CREATE INDEX person_id ON person(id); " +
            "    CREATE TABLE address ( " +
            "        id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "        person INTEGER NOT NULL REFERENCES person(id) ON DELETE CASCADE, " +
            "        address TEXT NOT NULL); " +
            "    CREATE INDEX address_id ON address(id); " +
            "    CREATE INDEX address_person ON address(person); " +
            "    PRAGMA user_version = 1";


    String SCHEMA_2 =
            "    CREATE TABLE email ( " +
            "        person INTEGER PRIMARY KEY REFERENCES person(id), " +
            "        email TEXT UNIQUE NOT NULL); " +
            "    PRAGMA user_version = 2";

    SQLiteWrapper database = null;

    @Before
    public void setUp() throws Exception {
        database_dir = TestUtils.createTempTestingDir(SQLDatabaseFactoryTest.class.getName());
        database = TestUtils.createEmptyDatabase(database_dir, database_file);
    }

    @After
    public void tearDown() throws Exception {
        TestUtils.deleteDatabaseQuietly(database);
        TestUtils.deleteTempTestingDir(database_dir);
    }

    @Test
    public void initialized_schemaVersion1_version1TablesShouldBeCreated() throws Exception {
        String[] version1Tables = new String[] { "person", "address" };
        SQLDatabaseFactory.updateSchema(database, SCHEMA_1, 1);
        verifyForeignKeyEnabled(database);
        Assert.assertEquals(1, database.getVersion());
        verifyAllTablesCreated(version1Tables);
    }

    @Test
    public void initialize_schemaVersion1And2_version2TablesShouldBeCreated() throws Exception {
        String[] version2Tables = new String[] { "person", "address", "email" };
        SQLDatabaseFactory.updateSchema(database, SCHEMA_1, 1);
        SQLDatabaseFactory.updateSchema(database, SCHEMA_2, 2);
        verifyForeignKeyEnabled(database);
        Assert.assertEquals(2, database.getVersion());
        verifyAllTablesCreated(version2Tables);
    }

    private void verifyAllTablesCreated(String[] tables) throws SQLException {
        Set<String> expectedTables = SQLDatabaseTestUtils.getAllTableNames(database);
        for (String table : tables) {
            Assert.assertTrue(expectedTables.contains(table));
        }
    }

    private void verifyForeignKeyEnabled(SQLDatabase database) throws SQLException {
        Cursor cursor = database.rawQuery("PRAGMA foreign_keys;", new String[]{});
        Assert.assertTrue(cursor.moveToNext());
        Assert.assertEquals(1, cursor.getInt(0));
    }
}
