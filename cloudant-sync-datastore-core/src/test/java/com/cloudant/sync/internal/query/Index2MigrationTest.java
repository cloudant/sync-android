/*
 * Copyright © 2017 IBM Corp. All rights reserved.
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

package com.cloudant.sync.internal.query;

import static org.junit.Assert.assertEquals;

import com.cloudant.sync.internal.documentstore.Database200MigrationTest;
import com.cloudant.sync.internal.documentstore.migrations.SchemaOnlyMigration;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.sqlite.SQLDatabaseFactory;
import com.cloudant.sync.query.FieldSort;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.Future;

public class Index2MigrationTest extends AbstractIndexTestBase {

    /**
     * Version 2 of the index includes an additional index_settings column, which defaults to a null
     * value. This method creates a version 2 index and then moves the table to a temporary table.
     * It rolls the index database version back to 0 and steps forward the schema versions; creating
     * a version 1 index table. It then copies the created index metadata (excluding the new column)
     * from the temporary table into the version 1 table having the effect of creating the required
     * index with version metadata.
     *
     * @throws Exception
     * @see Database200MigrationTest#setUp()
     */
    @Before
    public void createIndexAndRollback() throws Exception {

        // Create an index
        String indexName = im.createJsonIndex(Arrays.<FieldSort>asList(new FieldSort("name")),
                "basic").indexName;
        assertEquals("The \"basic\" index should have been created.", "basic", indexName);

        // Make it look like a version 1 index
        Future<Void> rollback = indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {

            @Override
            public Void call(SQLDatabase db) throws Exception {
                // Messy because SQLite doesn't support dropping a column
                // Copy the index data to a tmp location
                String tmpTable = "tmp" + QueryConstants.INDEX_METADATA_TABLE_NAME;
                db.execSQL("ALTER TABLE " + QueryConstants.INDEX_METADATA_TABLE_NAME + " RENAME " +
                        "TO " + tmpTable);
                // Reset to version 0
                db.execSQL("PRAGMA user_version=0;");
                // Create the index metadata table at version 1
                SQLDatabaseFactory.updateSchema(db, new SchemaOnlyMigration(QueryConstants
                        .getSchemaVersion1()), 1);
                // Copy the data from the tmp table into the new version 1 table
                db.execSQL("INSERT INTO " + QueryConstants.INDEX_METADATA_TABLE_NAME +
                        " SELECT index_name, index_type, field_name, last_sequence FROM " +
                        tmpTable);
                // Drop the tmp table
                db.execSQL("DROP TABLE " + tmpTable);
                return null;
            }
        });
        // Await the rollback completing
        rollback.get();
    }

    /**
     * Test for issue https://github.com/cloudant/sync-android/issues/527 where a NPE could be
     * encountered when trying to use an index from v1 in v2.
     *
     * @throws Exception
     */
    @Test
    public void testIndexMigrate1to2() throws Exception {
        // Update to version 2
        indexManagerDatabaseQueue.updateSchema(new SchemaOnlyMigration(QueryConstants
                .getSchemaVersion2()), 2);

        //List the indexes
        im.listIndexes();
    }
}
