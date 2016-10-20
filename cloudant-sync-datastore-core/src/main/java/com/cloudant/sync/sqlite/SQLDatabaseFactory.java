/**
 * Copyright © 2013, 2016 IBM Corp. All rights reserved.
 *
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
import com.cloudant.sync.datastore.migrations.Migration;
import com.cloudant.sync.util.DatabaseUtils;
import com.cloudant.sync.util.Misc;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Factory class to create {@link SQLDatabase}, and update schema
 *
 * @api_private
 */
public class SQLDatabaseFactory {

    public static final boolean FTS_AVAILABLE;
    private static final String FTS_CHECK_TABLE_NAME = "_t_cloudant_sync_query_fts_check";
    private final static Logger logger = Logger.getLogger(SQLDatabaseFactory.class.getCanonicalName());

    static {
        FTS_AVAILABLE = isFtsAvailable();
    }

    private static boolean isFtsAvailable() {
        SQLDatabase tempInMemoryDB = null;
        try {
            tempInMemoryDB = internalCreateSQLDatabase(null, new NullKeyProvider());
            tempInMemoryDB.beginTransaction();
            try {
                tempInMemoryDB.execSQL(String.format("CREATE VIRTUAL TABLE %s USING FTS4 ( col )",
                        FTS_CHECK_TABLE_NAME));
                return true;
            } finally {
                // End the transaction and rollback the virtual table we created because we never
                // set transaction success.
                tempInMemoryDB.endTransaction();
            }
        } catch (SQLException sqle) {
            return false;
        } finally {
            if (tempInMemoryDB != null) {
                tempInMemoryDB.close();
            }
        }
    }

    /**
     * SQLCipher-based implementation for creating database.
     * @param dbFilename full file path of the db file
     * @param provider Key provider object storing the SQLCipher key
     *                 Supply a NullKeyProvider to use a non-encrypted database.
     * @return {@code SQLDatabase} for the given filename
     * @throws IOException if the file does not exists, and also
     *         can not be created
     * @throws SQLException if the database cannot be opened.
     */
    public static SQLDatabase createSQLDatabase(String dbFilename, KeyProvider provider) throws IOException, SQLException {
        makeSureFileExists(dbFilename);
        return internalCreateSQLDatabase(dbFilename, provider);
    }

    /**
     * Internal method for creating a SQLDatabase that allows a null filename to create an in-memory
     * database which can be useful for performing checks, but creating in-memory databases is not
     * permitted from outside of this class hence the private visibility.
     *
     * @param dbFilename full file path of the db file or {@code null} for an in-memory database
     * @param provider Key provider or {@link NullKeyProvider}. Must be {@link NullKeyProvider}
     *                 if dbFilename is {@code null} i.e. for internal in-memory databases.
     * @return {@code SQLDatabase} for the given filename
     * @throws SQLException - if the database cannot be opened
     */
    private static SQLDatabase internalCreateSQLDatabase(String dbFilename, KeyProvider provider) throws SQLException {

        boolean runningOnAndroid =  Misc.isRunningOnAndroid();
        boolean useSqlCipher = (provider.getEncryptionKey() != null);

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
            throw new SQLException("Failed to load database module", e);
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
     * @throws SQLException If the database could not be opened.
     */
    public static SQLDatabase openSqlDatabase(String dbFilename, KeyProvider provider) throws IOException, SQLException {

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
                        throw new SQLException("Database could not be opened, invalid key.");
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
            throw new SQLException("Failed to load database module", e);
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
     * <p>Update schema for {@code SQLDatabase}</p>
     *
     * <p>Each input schema has a version, if the database's version is
     * smaller than {@code version}, the schema statements are executed.</p>
     *
     * <p>SQLDatabase's version is defined stored in {@code user_version} in the
     * database:</p>
     *
     * <pre>PRAGMA user_version;</pre>
     *
     * <p>This method updates {@code user_version} if the migration is successful.</p>
     *
     * @param database database to perform migration in.
     * @param migration migration to perform.
     * @param version the version this migration migrates to.
     *
     * @throws SQLException if migration fails
     *
     * @see com.cloudant.sync.sqlite.SQLDatabase#getVersion()
     */
    public static void updateSchema(SQLDatabase database, Migration migration, int version)
            throws SQLException {
        Misc.checkArgument(version > 0, "Schema version number must be positive");
        // ensure foreign keys are enforced in the case that we are up to date and no migration happen
        database.execSQL("PRAGMA foreign_keys = ON;");
        int dbVersion = database.getVersion();
        if(dbVersion < version) {
            // switch off foreign keys during the migration - so that we don't get caught out by
            // "ON DELETE CASCADE" constraints etc
            database.execSQL("PRAGMA foreign_keys = OFF;");
            database.beginTransaction();
            try {
                try {
                    migration.runMigration(database);
                    database.execSQL("PRAGMA user_version = " + version + ";");

                    database.setTransactionSuccessful();
                } catch (Exception ex) {
                    // don't set the transaction successful, so it'll rollback
                    throw new SQLException(
                            String.format("Migration from %1$d to %2$d failed.", dbVersion, version),
                            ex);
                }
            } finally {
                database.endTransaction();
                // re-enable foreign keys
                database.execSQL("PRAGMA foreign_keys = ON;");
            }

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
            if(!dbFile.createNewFile()){
                throw new IOException("Can not create datastore.");
            }
        }
    }
}
