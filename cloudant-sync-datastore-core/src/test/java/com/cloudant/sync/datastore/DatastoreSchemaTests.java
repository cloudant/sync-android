/**
 * Copyright (c) 2014 Cloudant, Inc. All rights reserved.
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

import com.cloudant.android.ContentValues;
import com.cloudant.sync.datastore.encryption.NullKeyProvider;
import com.cloudant.sync.datastore.migrations.MigrateDatabase6To100;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLDatabaseQueue;
import com.cloudant.sync.sqlite.SQLQueueCallable;
import com.cloudant.sync.util.TestUtils;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.concurrent.ExecutionException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Tests specific to database migrations and schema.
 */
public class DatastoreSchemaTests {

    @Test
    public void testSchema4UuidsDifferent() {
        // privateUuid and publicUuid have to be unique on the info table for each DB
        String[] schema_Alpha = DatastoreConstants.getSchemaVersion4();
        String[] schema_Beta  = DatastoreConstants.getSchemaVersion4();
        assertThat("Schemas should be different", schema_Alpha, not(equalTo(schema_Beta)));
    }

    @Test
    public void testSchemasSame() {
        // for contrast with above test, schema 3 and 5 are identical each time
        String[] schema_Alpha3 = DatastoreConstants.getSchemaVersion3();
        String[] schema_Beta3  = DatastoreConstants.getSchemaVersion3();
        assertThat("Schemas should be the same", schema_Alpha3, equalTo(schema_Beta3));

        String[] schema_Alpha5 = DatastoreConstants.getSchemaVersion5();
        String[] schema_Beta5  = DatastoreConstants.getSchemaVersion5();
        assertThat("Schemas should be the same", schema_Alpha5, equalTo(schema_Beta5));
    }


    @Test
    /**
     * Test the specific migration of a version 6 database to a version 7 database.
     * This migration should add the attachments_key_filename table, and populate
     * it with the existing keys from the attachments table as both key and filename.
     *
     * fixture/datastores-user_version6.zip contains a known database, at version 6 and
     * with two attachments.
     */
    public void migrateVersion6To100() throws DatastoreNotCreatedException, ExecutionException,
            InterruptedException, IOException {
        // Extract database to temp folder
        File temp_folder = new File(TestUtils.createTempTestingDir(this.getClass().getName()));
        File zippedVersion6 = f("fixture/datastores-user_version6.zip");
        Assert.assertTrue(unzipToDirectory(zippedVersion6, temp_folder));

        final String dbPath = j(temp_folder.getAbsolutePath(), "datastores", "testdb", "db.sync");
        SQLDatabaseQueue queue = new SQLDatabaseQueue(dbPath, new NullKeyProvider());

        queue.updateSchema(new MigrateDatabase6To100(), 100);

        // Check migration worked
        queue.submit(new SQLQueueCallable<Object>() {
            @Override
            public Object call(SQLDatabase db) throws Exception {

                Assert.assertEquals("DB version should be 100", 100, db.getVersion());

                Cursor c;

                // Confirm attachments_key_filename exists
                c = db.rawQuery("SELECT count(*) FROM sqlite_master WHERE type=\"table\" AND " +
                        "name=\"attachments_key_filename\";", null);
                c.moveToFirst();
                Assert.assertEquals(1, c.getInt(0));
                c.close();

                // Confirm attachments & attachments_key_filename have the right number of rows
                c = db.rawQuery("SELECT count(*) FROM attachments;", null);
                c.moveToFirst();
                Assert.assertEquals(2, c.getInt(0));
                c.close();
                c = db.rawQuery("SELECT count(*) FROM attachments_key_filename;", null);
                c.moveToFirst();
                Assert.assertEquals(2, c.getInt(0));
                c.close();

                // Confirm keys in attachments are as expected
                c = db.rawQuery("SELECT key FROM attachments WHERE " +
                        "key=x'68c3b7058dee64eba568746d396e42d7b7a1895b';", null);
                Assert.assertEquals("attachments table has 68c3b70 key", c.getCount(), 1);
                c.close();

                c = db.rawQuery("SELECT key FROM attachments WHERE " +
                        "key=x'd55f9ac778baf2256fa4de87aac61f590ebe66e0';", null);
                Assert.assertEquals("attachments table has d55f9ac key", c.getCount(), 1);
                c.close();

                // We should have new table attachments_key_filename with two rows,
                // which should be
                // key & filename both equal to:
                // 68c3b7058dee64eba568746d396e42d7b7a1895b
                // d55f9ac778baf2256fa4de87aac61f590ebe66e0
                c = db.rawQuery("SELECT key,filename FROM attachments_key_filename WHERE " +
                        "key=\"68c3b7058dee64eba568746d396e42d7b7a1895b\";", null);
                Assert.assertEquals("attachments_key_filename table has 68c3b70 key", c.getCount
                        (), 1);
                c.moveToFirst();
                Assert.assertEquals("68c3b7058dee64eba568746d396e42d7b7a1895b", c.getString(0));
                Assert.assertEquals("68c3b7058dee64eba568746d396e42d7b7a1895b", c.getString(1));
                c.close();

                c = db.rawQuery("SELECT key,filename FROM attachments_key_filename WHERE " +
                        "key=\"d55f9ac778baf2256fa4de87aac61f590ebe66e0\";", null);
                Assert.assertEquals("attachments_key_filename table has d55f9ac key", c.getCount
                        (), 1);
                c.moveToFirst();
                Assert.assertEquals("d55f9ac778baf2256fa4de87aac61f590ebe66e0", c.getString(0));
                Assert.assertEquals("d55f9ac778baf2256fa4de87aac61f590ebe66e0", c.getString(1));
                c.close();

                // Test we can't insert duplicate key
                ContentValues cv = new ContentValues();
                cv.put("key", "d55f9ac778baf2256fa4de87aac61f590ebe66e0");
                cv.put("filename", "nonExistentValue");
                Assert.assertEquals("Could insert duplicate key",
                        -1, db.insert("attachments_key_filename", cv));

                // Test we can't insert duplicate filename
                cv = new ContentValues();
                cv.put("key", "nonExistentValue");
                cv.put("filename", "d55f9ac778baf2256fa4de87aac61f590ebe66e0");
                Assert.assertEquals("Could insert duplicate key",
                        -1, db.insert("attachments_key_filename", cv));

                return null;
            }
        }).get();

        queue.shutdown();

        TestUtils.deleteTempTestingDir(temp_folder.getAbsolutePath());
    }


    @Test
    /**
     * Ensure database is migrated to version 100 or above when opening.
     *
     * fixture/datastores-user_version6.zip contains a known database, at version 6 and
     * with two attachments.
     */
    public void migrationToAtLeast100OnDatastoreOpen() throws DatastoreNotCreatedException, ExecutionException,
            InterruptedException, IOException {
        // Extract database to temp folder
        File temp_folder = new File(TestUtils.createTempTestingDir(this.getClass().getName()));
        File zippedVersion6 = f("fixture/datastores-user_version6.zip");
        Assert.assertTrue(unzipToDirectory(zippedVersion6, temp_folder));

        // Datastore manager the temp folder
        DatastoreImpl datastore = (DatastoreImpl) DatastoreManager.getInstance(
                new File(temp_folder, "datastores").getAbsolutePath())
                .openDatastore("testdb");

        try {
            // Check migration worked
            datastore.runOnDbQueue(new SQLQueueCallable<Object>() {
                @Override
                public Object call(SQLDatabase db) throws Exception {
                    Assert.assertTrue("DB version should be 100 or more", db.getVersion() >= 100);
                    return null;
                }
            }).get();
        } finally {
            datastore.close();
            TestUtils.deleteTempTestingDir(temp_folder.getAbsolutePath());
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")  // mkdirs result should be fine
    private boolean unzipToDirectory(File zipPath, File outputDirectory) {
        try {

            ZipFile zipFile = new ZipFile(zipPath);
            try {
                Enumeration<? extends ZipEntry> entries = zipFile.entries();
                while (entries.hasMoreElements()) {
                    ZipEntry entry = entries.nextElement();
                    File entryDestination = new File(outputDirectory, entry.getName());
                    if (entry.isDirectory())
                        entryDestination.mkdirs();
                    else {
                        entryDestination.getParentFile().mkdirs();
                        InputStream in = zipFile.getInputStream(entry);
                        OutputStream out = new FileOutputStream(entryDestination);
                        IOUtils.copy(in, out);
                        IOUtils.closeQuietly(in);
                        out.close();
                    }
                }
            } finally {
                zipFile.close();
            }

            return true;

        } catch (Exception ex) {
            return false;
        }
    }

    private static String j(String... components) {
        String path = components[0];
        String[] remainder = Arrays.copyOfRange(components, 1, components.length);
        for (String pathComponent : remainder) {
            path = path + File.separator + pathComponent;
        }
        return path;
    }

    private static File f(String filename) {
        return TestUtils.loadFixture(filename);
    }

}
