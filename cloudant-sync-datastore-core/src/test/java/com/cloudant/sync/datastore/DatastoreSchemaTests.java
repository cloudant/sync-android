/**
 * Copyright © 2014, 2016 IBM Corp. All rights reserved.
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
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.util.DatabaseUtils;
import com.cloudant.sync.util.TestUtils;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
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
        String[] schema_Beta = DatastoreConstants.getSchemaVersion4();
        assertThat("Schemas should be different", schema_Alpha, not(equalTo(schema_Beta)));
    }

    @Test
    public void testSchemasSame() {
        // for contrast with above test, schema 3 and 5 are identical each time
        String[] schema_Alpha3 = DatastoreConstants.getSchemaVersion3();
        String[] schema_Beta3 = DatastoreConstants.getSchemaVersion3();
        assertThat("Schemas should be the same", schema_Alpha3, equalTo(schema_Beta3));

        String[] schema_Alpha5 = DatastoreConstants.getSchemaVersion5();
        String[] schema_Beta5 = DatastoreConstants.getSchemaVersion5();
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
    public void migrateVersion6To100() throws Exception {
        // Extract database to temp folder
        File temp_folder = new File(TestUtils.createTempTestingDir(this.getClass().getName()));
        File zippedVersion6 = f("fixture/datastores-user_version6.zip");
        Assert.assertTrue(unzipToDirectory(zippedVersion6, temp_folder));

        final String dbPath = j(temp_folder.getAbsolutePath(), "datastores", "testdb", "db.sync");
        SQLDatabaseQueue queue = new SQLDatabaseQueue(dbPath, new NullKeyProvider());

        queue.updateSchema(new MigrateDatabase6To100(), 100);

        // Check migration worked
        queue.submit(new SQLCallable<Object>() {
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
    public void migrationToAtLeast100OnDatastoreOpen() throws DatastoreNotCreatedException,
            ExecutionException,
            InterruptedException, IOException {
        // Extract database to temp folder
        File temp_folder = new File(TestUtils.createTempTestingDir(this.getClass().getName()));
        File zippedVersion6 = f("fixture/datastores-user_version6.zip");
        Assert.assertTrue(unzipToDirectory(zippedVersion6, temp_folder));

        // Datastore manager the temp folder
        DatabaseImpl datastore = (DatabaseImpl) DocumentStore.getInstance(
                new File(temp_folder+"/datastores", "testdb")).database;

        try {
            // Check migration worked
            datastore.runOnDbQueue(new SQLCallable<Object>() {
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
                    if (entry.isDirectory()) {
                        entryDestination.mkdirs();
                    } else {
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

    /**
     * <p>
     * Tests the migration of an existing datastore "corrupted" by the issues 329 and 326. Some
     * attachments have also been added to exercise that code. The database contains many documents
     * that have multiple identical revisions, some of which are deleted (and because of 326) have
     * no current winner. Others have multiple identical revisions, but are otherwise normal.
     * </p>
     * <p>
     * The special case documents are:
     * </p>
     * <ul>
     * <li>
     * 300 d834ca038de24bf0ac9f708fcdb63e21 1-abea4faccec4430685dd2065f28a7dd9:
     * sequence 594 (lowest) has 3 identical attachment entries, seq 847 has no attachments
     * </li>
     * <li>
     * 307 a2359c3503e34c008ec448834583e482 1-b83f5c820588498ca2228282237d4f90:
     * sequence 1051 (highest) has 2 identical attachment entries, seq 606 has no attachments
     * </li>
     * <li>
     * 308 badad1b3842e4056b013587b49c93308 1-aaf13ff7c5dc44ba8626f684fb73b9d3:
     * sequence 607 (lowest) 1050 (highest) each have one copy of each of two different attachments
     * </li>
     * </ul>
     * <p>
     *
     * 300:594
     *   -underground.txt
     *   -underground.txt
     *   -underground.txt
     * 300:847
     *
     * 307:606
     * 307:1061
     *   -underground.txt
     *   -underground.txt
     *
     * 308:607
     *   -underground.txt
     *   -bonsai-boston.jpg
     * 308:1050
     *   -underground.txt
     *   -bonsai-boston.jpg
     * </p>
     *
     * @throws Exception
     */
    @Test
    public void migration100DatabaseWithDuplicatesTo200() throws Exception {
        // Extract database to temp folder
        File temp_folder = new File(TestUtils.createTempTestingDir(this.getClass().getName()));
        File zippedWithDups = f("fixture/v100WithDuplicates.zip");
        Assert.assertTrue(unzipToDirectory(zippedWithDups, temp_folder));

        // Open the v100WithDuplicates datastore. Opening should perform the 100 to 200 migration.
        DatabaseImpl datastore = (DatabaseImpl) DocumentStore.getInstance(
                new File(temp_folder + "/datastores", "v100DBWithDuplicates")).database;

        try {

            // Validate that the version was updated
            Integer version = datastore.runOnDbQueue(new SQLCallable<Integer>() {
                @Override
                public Integer call(SQLDatabase db) throws Exception {
                    return db.getVersion();
                }
            }).get();
            Assert.assertTrue("DB version should be 200 or more", version >= 200);

            // Validate that there are no documents not marked as current
            List<Long> result = datastore.runOnDbQueue(new SQLCallable<List<Long>>() {

                @Override
                public List<Long> call(SQLDatabase db) throws Exception {
                    List<Long> idsWithNoCurrent = new ArrayList<Long>();
                    Cursor c = null;
                    try {
                        c = db.rawQuery("SELECT doc_id FROM (SELECT doc_id, SUM(current) AS s " +
                                "FROM revs GROUP BY doc_id) WHERE s=0;", null);
                        while (c.moveToNext()) {
                            idsWithNoCurrent.add(c.getLong(0));
                        }
                        return idsWithNoCurrent;
                    } finally {
                        DatabaseUtils.closeCursorQuietly(c);
                    }
                }
            }).get();

            Assert.assertEquals("There should be no documents that don't have a winning " +
                    "revision", 0, result.size());

            // Validate that there are no documents that have duplicate revision IDs

            result = datastore.runOnDbQueue(new SQLCallable<List<Long>>() {

                @Override
                public List<Long> call(SQLDatabase db) throws Exception {
                    List<Long> docsWithDuplicateRevisions = new ArrayList<Long>();
                    Cursor c = null;
                    try {
                        c = db.rawQuery("SELECT doc_id FROM revs GROUP BY doc_id, revid HAVING " +
                                "COUNT(*) > 1", null);
                        while (c.moveToNext()) {
                            docsWithDuplicateRevisions.add(c.getLong(0));
                        }
                        return docsWithDuplicateRevisions;
                    } finally {
                        DatabaseUtils.closeCursorQuietly(c);
                    }
                }
            }).get();

            Assert.assertEquals("There should be no documents that have duplicate revisions", 0,
                    result.size());

            // Document with id d834ca038de24bf0ac9f708fcdb63e21 has duplicated attachments on
            // lowest seq.
            // Validate that after migration only one remains and it is of the correct name
            List<? extends Attachment> attachments = datastore.attachmentsForRevision(datastore
                    .getDocument("d834ca038de24bf0ac9f708fcdb63e21"));
            Assert.assertEquals("There should only be one copy of the attachment", 1, attachments
                    .size());
            Attachment a = attachments.get(0);
            Assert.assertEquals("The attachment name should be correct", "underground.txt", a.name);

            // Document with id a2359c3503e34c008ec448834583e482 has duplicated attachments on
            // highest seq.
            // Validate that after migration only one remains and it is of the correct name
            attachments = datastore.attachmentsForRevision(datastore
                    .getDocument("a2359c3503e34c008ec448834583e482"));
            Assert.assertEquals("There should only be one copy of the attachment", 1, attachments
                    .size());
            a = attachments.get(0);
            Assert.assertEquals("The attachment name should be correct", "underground.txt", a.name);

            // Document with id badad1b3842e4056b013587b49c93308 has duplicated attachments across
            // two seqs.
            // Validate that after migration two different attachments remain
            attachments = datastore.attachmentsForRevision(datastore
                    .getDocument("badad1b3842e4056b013587b49c93308"));
            Assert.assertEquals("There should only be two attachments", 2, attachments
                    .size());
            Assert.assertNotEquals("The attachment names should be different", attachments.get(0)
                    .name, attachments.get(1).name);

        } finally {
            datastore.close();
        }
    }

    /**
     * Test that we can migrate an auto-generated complex database from schema version 100
     * The database has the following characteristics:
     * - 25 docs, for each doc:
     * - Max tree depth 7
     * - Multiple leaf nodes (conflicts)
     * - Attachments
     * - Deleted revs
     * @throws Exception
     */
    @Test
    public void migration100ComplexDatabaseWithoutDuplicatesTo200() throws Exception {
        // Extract database to temp folder
        File temp_folder = new File(TestUtils.createTempTestingDir(this.getClass().getName()));
        File zippedComplexDatabase = f("fixture/v100ComplexWithoutDuplicates.zip");
        Assert.assertTrue(unzipToDirectory(zippedComplexDatabase, temp_folder));

        // Open the v100WithDuplicates datastore. Opening should perform the 100 to 200 migration.
        DatabaseImpl datastore = (DatabaseImpl) DocumentStore.getInstance(
                new File(temp_folder + "/datastores", "v100ComplexWithoutDuplicates")).database;

        try {

            // Validate that the version was updated
            Integer version = datastore.runOnDbQueue(new SQLCallable<Integer>() {
                @Override
                public Integer call(SQLDatabase db) throws Exception {
                    return db.getVersion();
                }
            }).get();
            Assert.assertTrue("DB version should be 200 or more", version >= 200);

            // Validate that there are no documents not marked as current
            List<Long> result = datastore.runOnDbQueue(new SQLCallable<List<Long>>() {

                @Override
                public List<Long> call(SQLDatabase db) throws Exception {
                    List<Long> idsWithNoCurrent = new ArrayList<Long>();
                    Cursor c = null;
                    try {
                        c = db.rawQuery("SELECT doc_id FROM (SELECT doc_id, SUM(current) AS s " +
                                "FROM revs GROUP BY doc_id) WHERE s=0;", null);
                        while (c.moveToNext()) {
                            idsWithNoCurrent.add(c.getLong(0));
                        }
                        return idsWithNoCurrent;
                    } finally {
                        DatabaseUtils.closeCursorQuietly(c);
                    }
                }
            }).get();

            Assert.assertEquals("There should be no documents that don't have a winning " +
                    "revision", 0, result.size());

            // Validate that there are no documents that have duplicate revision IDs
            result = datastore.runOnDbQueue(new SQLCallable<List<Long>>() {

                @Override
                public List<Long> call(SQLDatabase db) throws Exception {
                    List<Long> docsWithDuplicateRevisions = new ArrayList<Long>();
                    Cursor c = null;
                    try {
                        c = db.rawQuery("SELECT doc_id FROM revs GROUP BY doc_id, revid HAVING " +
                                "COUNT(*) > 1", null);
                        while (c.moveToNext()) {
                            docsWithDuplicateRevisions.add(c.getLong(0));
                        }
                        return docsWithDuplicateRevisions;
                    } finally {
                        DatabaseUtils.closeCursorQuietly(c);
                    }
                }
            }).get();

            Assert.assertEquals("There should be no documents that have duplicate revisions", 0,
                    result.size());

            // These asserts have been derived from the auto-generated database
            // if the database is re-created then these asserts will need to be changed
            assertLeafCount(datastore, "Document0",1);
            assertAttachmentCount(datastore, "Document0",0);
            assertWinner(datastore, "Document0","1-root");

            assertLeafCount(datastore, "Document1",211);
            assertAttachmentCount(datastore, "Document1",562);
            assertWinner(datastore, "Document1","7-rootbdacdc");

            assertLeafCount(datastore, "Document2",231);
            assertAttachmentCount(datastore, "Document2",591);
            assertWinner(datastore, "Document2","7-rootbaadce");

            assertLeafCount(datastore, "Document3",520);
            assertAttachmentCount(datastore, "Document3",1447);
            assertWinner(datastore, "Document3","7-rooteadcaa");

            assertLeafCount(datastore, "Document4",1);
            assertAttachmentCount(datastore, "Document4",0);
            assertWinner(datastore, "Document4","1-root");

            assertLeafCount(datastore, "Document5",76);
            assertAttachmentCount(datastore, "Document5",146);
            assertWinner(datastore, "Document5","7-rootabcaae");

            assertLeafCount(datastore, "Document6",297);
            assertAttachmentCount(datastore, "Document6",929);
            assertWinner(datastore, "Document6","7-rootbddddc");

            assertLeafCount(datastore, "Document7",412);
            assertAttachmentCount(datastore, "Document7",1309);
            assertWinner(datastore, "Document7","7-rootebcdde");

            assertLeafCount(datastore, "Document8",489);
            assertAttachmentCount(datastore, "Document8",1332);
            assertWinner(datastore, "Document8","7-rootcdbceb");

            assertLeafCount(datastore, "Document9",265);
            assertAttachmentCount(datastore, "Document9",775);
            assertWinner(datastore, "Document9","7-rootbadedb");

            assertLeafCount(datastore, "Document10",509);
            assertAttachmentCount(datastore, "Document10",1677);
            assertWinner(datastore, "Document10","7-rootcebdcc");

            assertLeafCount(datastore, "Document11",357);
            assertAttachmentCount(datastore, "Document11",1092);
            assertWinner(datastore, "Document11","7-rootecbaeb");

            assertLeafCount(datastore, "Document12",540);
            assertAttachmentCount(datastore, "Document12",1537);
            assertWinner(datastore, "Document12","7-rootdcddcb");

            assertLeafCount(datastore, "Document13",303);
            assertAttachmentCount(datastore, "Document13",880);
            assertWinner(datastore, "Document13","7-rootebbbbc");

            assertLeafCount(datastore, "Document14",1);
            assertAttachmentCount(datastore, "Document14",0);
            assertWinner(datastore, "Document14","1-root");

            assertLeafCount(datastore, "Document15",392);
            assertAttachmentCount(datastore, "Document15",1127);
            assertWinner(datastore, "Document15","7-rootcdddbd");

            assertLeafCount(datastore, "Document16",1);
            assertAttachmentCount(datastore, "Document16",0);
            assertWinner(datastore, "Document16","1-root");

            assertLeafCount(datastore, "Document17",326);
            assertAttachmentCount(datastore, "Document17",1051);
            assertWinner(datastore, "Document17","7-rootcddcbe");

            assertLeafCount(datastore, "Document18",124);
            assertAttachmentCount(datastore, "Document18",379);
            assertWinner(datastore, "Document18","7-rootadecbd");

            assertLeafCount(datastore, "Document19",1);
            assertAttachmentCount(datastore, "Document19",0);
            assertWinner(datastore, "Document19","1-root");

            assertLeafCount(datastore, "Document20",248);
            assertAttachmentCount(datastore, "Document20",724);
            assertWinner(datastore, "Document20","7-rootbecbec");

            assertLeafCount(datastore, "Document21",49);
            assertAttachmentCount(datastore, "Document21",140);
            assertWinner(datastore, "Document21","7-rootabaeac");

            assertLeafCount(datastore, "Document22",1);
            assertAttachmentCount(datastore, "Document22",0);
            assertWinner(datastore, "Document22","1-root");

            assertLeafCount(datastore, "Document23",258);
            assertAttachmentCount(datastore, "Document23",805);
            assertWinner(datastore, "Document23","7-rootbddabd");

            assertLeafCount(datastore, "Document24",840);
            assertAttachmentCount(datastore, "Document24",2358);
            assertWinner(datastore, "Document24","7-rootedbebe");

        } finally {
            datastore.close();
        }
    }

    // utility methods used by tests
    private void assertLeafCount(Database database, String docId, int expectedLeafCount) {
        int actualLeafCount = ((DatabaseImpl) database).getAllRevisionsOfDocument(docId).
                leafRevisions().size();
        Assert.assertEquals("Should get the expected number of leaf revisions", expectedLeafCount,
                actualLeafCount);
    }

    private void assertAttachmentCount(Database database, String docId,
                                       int expectedAttachmentCount) {
        int actualAttachmentCount = 0;
        Set<String> revIds = ((DatabaseImpl) database).getAllRevisionsOfDocument(docId).
                leafRevisionIds();
        for (String revId : revIds) {
            try {
                actualAttachmentCount += database.getDocument(docId, revId).attachments.size();
            } catch (DocumentNotFoundException dnfe) {
                Assert.fail("Failed to find revision for document ID " + docId + " and revision ID "
                        + revId);
            }
        }
        Assert.assertEquals("Should get the expected number of attachments",
                expectedAttachmentCount,
                actualAttachmentCount);
    }

    private void assertWinner(Database database, String docId, String expectedRevId) {
        try {
            String actualRevId = database.getDocument(docId).getRevision();
            Assert.assertEquals("Should get the expected winning revision ID", expectedRevId,
                    actualRevId);
        } catch (DocumentNotFoundException dnfe) {
            Assert.fail("Failed to find any revisions for document ID "+docId);
        }
    }
}
