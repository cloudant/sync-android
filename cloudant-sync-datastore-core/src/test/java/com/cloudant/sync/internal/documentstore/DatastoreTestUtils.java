/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.internal.documentstore;

import com.cloudant.sync.documentstore.encryption.NullKeyProvider;
import com.cloudant.sync.internal.documentstore.migrations.SchemaOnlyMigration;
import com.cloudant.sync.internal.sqlite.SQLDatabaseFactory;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

/**
 * DatastoreTestUtils for this package.
 */
public class DatastoreTestUtils {

    private final static String DATABASE_FILE_EXT = ".sqlite4java";

    public static SQLDatabase createDatabase(String database_dir, String database_file)
            throws IOException, SQLException {
        String path = database_dir + File.separator + database_file + DATABASE_FILE_EXT;
        File dbFile = new File(path);
        FileUtils.touch(dbFile);
        SQLDatabase database = SQLDatabaseFactory.openSQLDatabase(dbFile,
                new NullKeyProvider());
        SQLDatabaseFactory.updateSchema(database,
                new SchemaOnlyMigration(DatastoreConstants.getSchemaVersion3()), 3);
        SQLDatabaseFactory.updateSchema(database,
                new SchemaOnlyMigration(DatastoreConstants.getSchemaVersion4()), 4);
        SQLDatabaseFactory.updateSchema(database,
                new SchemaOnlyMigration(DatastoreConstants.getSchemaVersion5()), 5);
        return database;
    }
}
