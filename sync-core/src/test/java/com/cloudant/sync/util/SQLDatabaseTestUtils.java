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

package com.cloudant.sync.util;

import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import org.junit.Assert;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;

public class SQLDatabaseTestUtils {

    public static void assertTablesExist(SQLDatabase db, String... tables) throws SQLException {
        Set<String> allTables = getAllTableNames(db);
        for(String table: tables) {
            Assert.assertThat(allTables, hasItem(table));
        }
    }

    public static void assertTablesNotExist(SQLDatabase db, String... tables) throws SQLException {
        Set<String> allTables = getAllTableNames(db);
        for(String table: tables) {
            Assert.assertThat(allTables, not(hasItem(table)));
        }
    }

    public static Set<String> getAllTableNames(SQLDatabase db) throws SQLException {
        Cursor cursor = null;
        try {
            cursor = db.rawQuery("SELECT name FROM sqlite_master WHERE type='table'", new String[]{});
            Set<String> tables = new HashSet<String>();
            while(cursor.moveToNext()) {
                tables.add(cursor.getString(0));
            }
            return tables;
        } finally {

        }
    }

}
