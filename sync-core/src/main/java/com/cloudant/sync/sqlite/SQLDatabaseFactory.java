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

import com.cloudant.sync.datastore.encryption.KeyProvider;
import com.cloudant.sync.datastore.encryption.NullKeyProvider;
import com.cloudant.sync.util.DatabaseUtils;
import com.cloudant.sync.util.Misc;
import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Factory class to create @{link SQLDatabase}, and update schema
 */
public class SQLDatabaseFactory {

    private final static Logger logger = Logger.getLogger(SQLDatabaseFactory.class.getCanonicalName());

    /**
     * SQLCipher-based implementation for creating database.
     * @param dbFilename full file path of the db file
     * @param provider Key provider object storing the SQLCipher key
     *                 Supply a NullKeyProvider to use a non-encrypted database.
     * @return {@code SQLDatabase} for the given filename
     * @throws IOException if the file does not exists, and also
     *         can not be created
     */
    public static SQLDatabase createSQLDatabase(String dbFilename, KeyProvider provider) throws IOException {

        boolean runningOnAndroid =  Misc.isRunningOnAndroid();
        boolean useSqlCipher = (provider.getEncryptionKey() != null);

        makeSureFileExists(dbFilename);

        try {

            if (runningOnAndroid) {
                if (useSqlCipher) {
                    return (SQLDatabase) Class.forName("com.cloudant.sync.sqlite.android.AndroidSQLCipherSQLite")
                            .getMethod("createAndroidSQLite", String.class, KeyProvider.class)
                            .invoke(null, new Object[]{dbFilename, provider});
                } else {
                    return (SQLDatabase) Class.forName("com.cloudant.sync.sqlite.android.AndroidSQLite")
                            .getMethod("createAndroidSQLite", String.class)
                            .invoke(null, dbFilename);
                }
            } else {
                if (useSqlCipher) {
                    throw new UnsupportedOperationException("No SQLCipher-based database implementation for Java SE");
                } else {
                    return (SQLDatabase) Class.forName("com.cloudant.sync.sqlite.sqlite4java.SQLiteWrapper")
                            .getMethod("openSQLiteWrapper", String.class)
                            .invoke(null, dbFilename);
                }
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to load database module", e);
            return null;
        }

    }

    /**
     * SQLCipher-based implementation for opening database.
     * @param dbFilename full file path of the db file
     * @param provider Key provider object storing the SQLCipher key
     *                 Supply a NullKeyProvider to use a non-encrypted database.
     * @return {@code SQLDatabase} for the given filename
     * @throws IOException if the file does not exists, and also
     *         can not be created
     */
    public static SQLDatabase openSqlDatabase(String dbFilename, KeyProvider provider) throws IOException {

        boolean runningOnAndroid =  Misc.isRunningOnAndroid();
        boolean useSqlCipher = (provider.getEncryptionKey() != null);

        makeSureFileExists(dbFilename);

        try {

            if (runningOnAndroid) {
                if (useSqlCipher) {
                    SQLDatabase result = (SQLDatabase) Class.forName("com.cloudant.sync.sqlite.android.AndroidSQLCipherSQLite")
                            .getMethod("openAndroidSQLite", String.class, KeyProvider.class)
                            .invoke(null, new Object[]{dbFilename, provider});

                    if (validateOpenedDatabase(result)) {
                        return result;
                    } else {
                        return null;
                    }
                } else {
                    return (SQLDatabase) Class.forName("com.cloudant.sync.sqlite.android.AndroidSQLite")
                            .getMethod("createAndroidSQLite", String.class)
                            .invoke(null, dbFilename);
                }
            } else {
                if (useSqlCipher) {
                    throw new UnsupportedOperationException("No SQLCipher-based database implementation for Java SE");
                } else {
                    return (SQLDatabase) Class.forName("com.cloudant.sync.sqlite.sqlite4java.SQLiteWrapper")
                            .getMethod("openSQLiteWrapper", String.class)
                            .invoke(null, dbFilename);
                }
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to load database module", e);
            return null;
        }
    }

    /**
     * This method runs a simple SQL query to validate the opened database
     * is readable. In particular, this is useful for testing the key we
     * passed SQLCipher is the correct key.
     *
     * @param db database to check is readable
     * @return true if database passes validation, false otherwise
     */
    private static boolean validateOpenedDatabase(SQLDatabase db) {
        Cursor c = null;
        try {
            c = db.rawQuery("SELECT count(*) FROM sqlite_master", null);
            if (c.moveToFirst()) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Error performing database start up validation", ex);
            return false;
        } finally {
            DatabaseUtils.closeCursorQuietly(c);
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
        database.beginTransaction();
        try {
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
