/**
 * Original iOS version by  Jens Alfke, ported to Android by Marty Schoch
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 *
 * Modifications for this distribution by Cloudant, Inc., Copyright (c) 2013 Cloudant, Inc.
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

package com.cloudant.sync.sqlite;

import com.cloudant.sync.util.Misc;
import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.SQLException;

/**
 * Factory class to create @{link SQLDatabase}, and update schema
 */
public class SQLDatabaseFactory {

    /**
     * Return {@code SQLDatabase} for the given dbFilename
     *
     * @param dbFilename full file path of the db file
     * @return {@code SQLDatabase} for the give filename
     * @throws IOException if the file does not exists, and also
     *         can not be created
     */
    public static SQLDatabase openSqlDatabase(String dbFilename) throws IOException {
        makeSureFileExists(dbFilename);
        if(Misc.isRunningOnAndroid()) {
            try {
                Class c = Class.forName("com.cloudant.sync.sqlite.android.AndroidSQLite");
                Method m = c.getMethod("createAndroidSQLite", String.class);
                return (SQLDatabase)m.invoke(null, dbFilename);
            } catch (Exception e) {
                return null;
            }
        } else {
            try {
                Class c = Class.forName("com.cloudant.sync.sqlite.sqlite4java.SQLiteWrapper");
                Method m = c.getMethod("openSQLiteWrapper", String.class);
                return (SQLDatabase)m.invoke(null, dbFilename);
            } catch (Exception e) {
                return null;
            }
        }
    }

    /**
     * <p>Update schema for give {@code SQLDatabase}</p>
     *
     * <p>Each input schema has a version, if the database's version is
     * smaller in the schema version, the schema statements are executed.</p>
     *
     * <p>SQLDatabase's version is defined as:</p>
     *
     * <pre>    PRAGMA user_version;</pre>
     *
     * <p>In the schema statements, it must contains statement to update
     * database version:</p>
     *
     * <pre>    PRAGMA user_version = version</pre>
     *
     *
     * @param database
     * @param schema
     * @param version
     * @throws SQLException
     *
     * @see com.cloudant.sync.sqlite.SQLDatabase#getVersion()
     */
    public static void updateSchema(SQLDatabase database, String[] schema, int version)
            throws SQLException {
        Preconditions.checkArgument(version > 0, "Schema version number must be positive");

        // Stuff we need to do every time the database opens because it is not
        // persistent in sqlite
        database.execSQL("PRAGMA foreign_keys = ON;");
        int dbVersion = database.getVersion();
        if(dbVersion < version) {
            executeStatements(database, schema, version);
        }
    }

    private static int executeStatements(SQLDatabase database, String[] statements, int version)
            throws SQLException {
        try {
            database.beginTransaction();
            for (String statement : statements) {
                database.execSQL(statement);
            }
            database.execSQL("PRAGMA user_version = " + version + ";");

            database.setTransactionSuccessful();
            return database.getVersion();
        } finally {
            database.endTransaction();
        }
    }

    private static void makeSureFileExists(String dbFilename) throws IOException {
        File dbFile = new File(dbFilename);
        File dbDir = dbFile.getParentFile();
        if(!dbDir.exists()) {
            if(!dbDir.mkdirs()) {
                throw new IOException("Can not create the directory for datastore.");
            }
        }
        if (!dbFile.exists()) {
            dbFile.createNewFile();
        }
    }
}
