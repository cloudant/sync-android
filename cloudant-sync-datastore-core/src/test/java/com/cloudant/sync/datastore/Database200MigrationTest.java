/**
 * Copyright (C) 2016 IBM Corp. All rights reserved.
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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import com.cloudant.android.ContentValues;
import com.cloudant.sync.datastore.migrations.MigrateDatabase100To200;
import com.cloudant.sync.datastore.migrations.MigrateDatabase6To100;
import com.cloudant.sync.datastore.migrations.SchemaOnlyMigration;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLDatabaseQueue;
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.util.DatabaseUtils;
import com.cloudant.sync.util.TestUtils;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by rhys on 15/08/2016.
 */
public class Database200MigrationTest {

    private DatabaseImpl ds;
    private DocumentRevision rootRevision;
    private String dir;
    private final String duplicateRevs = "INSERT INTO revs (doc_id, parent, current, deleted, " +
            "available, revid, json) SELECT doc_id, parent, current, deleted, available, revid, " +
            "json FROM revs where revs.revid = ?";

    @Before
    public void setUp() throws Exception {
        dir = TestUtils.createTempTestingDir(this.getClass().getName());

        ds = (DatabaseImpl) (DocumentStore.getInstance(new File(dir, "complexTree"))).database;

        // Return DB to schema version 100:

        // This allows these tests to run since they rely on doing things which are not possible in
        // the latest schema version (200), where extra UNIQUE constraints have been added

        // This is a hack but because we can't control the schema upgrade process (it automatically
        // happens when the datastore is opened). The easiest way to do it is to drop all tables and
        // step through the upgrades to the version we want.
        ds.runOnDbQueue(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                try {
                    db.execSQL("PRAGMA foreign_keys = OFF");
                    db.execSQL("DROP TABLE views;");
                    db.execSQL("DROP TABLE revs;");
                    db.execSQL("DROP TABLE docs;");
                    db.execSQL("DROP TABLE attachments;");
                    db.execSQL("DROP TABLE maps;");
                    db.execSQL("DROP TABLE replicators;");
                    db.execSQL("DROP TABLE localdocs;");
                    db.execSQL("DROP TABLE info;");
                    db.execSQL("DROP TABLE attachments_key_filename;");
                    db.execSQL("PRAGMA foreign_keys = ON");
                    db.execSQL("PRAGMA user_version=0;");
                } catch (Exception e) {
                    Assert.fail("Failed to drop tables in setup method: "+e);
                }
                return null;
            }
        }).get();

        SQLDatabaseQueue q = getQueue();
        q.updateSchema(new SchemaOnlyMigration(DatastoreConstants.getSchemaVersion3()), 3);
        q.updateSchema(new SchemaOnlyMigration(DatastoreConstants.getSchemaVersion4()), 4);
        q.updateSchema(new SchemaOnlyMigration(DatastoreConstants.getSchemaVersion5()), 5);
        q.updateSchema(new SchemaOnlyMigration(DatastoreConstants.getSchemaVersion6()), 6);
        q.updateSchema(new MigrateDatabase6To100(), 100);

        Map<String, String> body = new HashMap<String, String>();
        body.put("hello", "world");

        DocumentRevision rev = new DocumentRevision();
        rev.setBody(DocumentBodyFactory.create(body));
        rootRevision = ds.createDocumentFromRevision(rev);
    }

    @After
    public void tearDown() throws Exception {
        ds.close();
        TestUtils.deleteTempTestingDir(dir);
    }

    /**
     * Tree:
     *
     * abc:
     *
     * 1-xxxxxxx
     *
     * 1-xxxxxxx
     *
     * 1-abc
     *
     * @throws Exception
     */
    @Test
    public void testDatabaseWithComplexTree() throws Exception {

        // sets up the datastore with 1-x and 1-x revisions (Duplicates)

        final String updateCurrent = "UPDATE revs SET current=0 where sequence=2";

        ds.runOnDbQueue(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                db.execSQL(duplicateRevs, new String[]{rootRevision.getRevision()});
                db.execSQL(updateCurrent, new String[]{});
                return null;
            }
        }).get();

        DocumentRevision rev = new DocumentRevision(rootRevision.id, "1-abc");
        rev.setBody(rootRevision.getBody());
        ((DatabaseImpl) ds).forceInsert(rev, "1-abc");
        runMigration();


        int count = revisionCount();
        Assert.assertThat("Only 2 revisions should be present in the database.", count, is(2));

        // Check the revs are what we expect.
        Assert.assertThat(getRevs(), containsInAnyOrder("1-abc", rootRevision.revision));

    }

    /**
     * Tree:
     *
     * abc:
     *
     * 1-xxxxxxx
     *
     * 1-xxxxxxx -------> 2-yyyyyyyy
     * @throws Exception
     */
    @Test
    public void testDatabaseWithForest() throws Exception {

        // sets up the datastore with 1-x and 1-x revisions (Duplicates) with the second 1-x
        // having a revision on it.

        final String updateCurrent = "UPDATE revs SET current=0 where sequence=2";

        ds.runOnDbQueue(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                db.execSQL(duplicateRevs, new String[]{rootRevision.getRevision()});
                db.execSQL(updateCurrent, new String[]{});
                return null;
            }
        }).get();

        Map<String, Object> body = rootRevision.getBody().asMap();
        body.put("My", "otherBody");
        rootRevision.setBody(DocumentBodyFactory.create(body));
        DocumentRevision rev = ds.updateDocumentFromRevision(rootRevision);


        final String changeParent = "UPDATE revs SET parent=2 where parent = 1";
        ds.runOnDbQueue(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                db.execSQL(changeParent, new String[]{});
                return null;
            }
        }).get();

        runMigration();


        int count = revisionCount();
        Assert.assertThat("Only 2 revisions should be in the database.", count, is(2));
        Assert.assertThat(getRevs(), containsInAnyOrder(rootRevision.revision, rev.revision));
    }

    /**
     * tree:
     *
     * abc:
     *
     * 1-xxxxxxxx
     *
     * 1-xxxxxxxx -----> 2-yyyyyy
     *
     * 1-xxxxxxxx
     *
     * @throws Exception
     */
    @Test
    public void testDatabaseWithForestOfDuplicates() throws Exception {

        // sets up the datastore with three(!) 1-x(Duplicates)

        final String updateCurrent = "UPDATE revs SET current=0 where sequence=2 or sequence=3";

        ds.runOnDbQueue(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                db.execSQL(duplicateRevs, new String[]{rootRevision.getRevision()});
                db.execSQL(duplicateRevs, new String[]{rootRevision.getRevision()});
                db.execSQL(updateCurrent, new String[]{});
                return null;
            }
        }).get();

        Map<String, Object> body = rootRevision.getBody().asMap();
        body.put("My", "otherBody");
        rootRevision.setBody(DocumentBodyFactory.create(body));
        DocumentRevision rev = ds.updateDocumentFromRevision(rootRevision);


        final String changeParent = "UPDATE revs SET parent=2 where parent = 1";
        ds.runOnDbQueue(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                db.execSQL(changeParent, new String[]{});
                return null;
            }
        }).get();

        runMigration();


        int count = revisionCount();
        Assert.assertThat("Only two revisions are in the database", count, is(2));
        Assert.assertThat(getRevs(), containsInAnyOrder(rootRevision.revision, rev.revision));
    }

    /**
     * tree:
     *
     * abc:
     *
     * 1-xxxxxxx -----> 2-yyyyyyyyy
     *              |
     *              --> 2-yyyyyyyyy
     * @throws Exception
     */
    @Test
    public void testDatabaseWithSecondLevelDuplicates() throws Exception {

        // sets up the datastore with 1 1-x and with 2 2-x.
        Map<String, Object> body = rootRevision.getBody().asMap();
        body.put("My", "otherBody");
        rootRevision.setBody(DocumentBodyFactory.create(body));
        final DocumentRevision rev = ds.updateDocumentFromRevision(rootRevision); // 2x

        final String updateCurrent = "UPDATE revs SET current=0 where sequence=2 or sequence=3";

        ds.runOnDbQueue(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                db.execSQL(duplicateRevs, new String[]{rev.getRevision()});
                db.execSQL(updateCurrent, new String[]{});
                return null;
            }
        }).get();

        runMigration();


        int count = revisionCount();
        Assert.assertThat("Only 2 revisions are in the database.", count, is(2));
        Assert.assertThat(getRevs(), containsInAnyOrder(rootRevision.revision, rev.revision));
    }

    /**
     *
     *  Tree:
     *
     *  abc:
     *
     *  1-xxxxxx --------> 2-yyyyyyyy -------> 3-zzzzzzzzz
     *
     *  1-xxxxxx --------> 2-yyyyyyyy -------> 3-zzzzzzzzz
     *
     * @throws Exception
     */
    @Test
    public void testDatabaseWith3LevelCompleteTreeDuplication() throws Exception {

        // sets up the datastore with two tress of 3 nodes, duplicated
        Map<String, Object> body = rootRevision.getBody().asMap();
        body.put("My", "otherBody");
        rootRevision.setBody(DocumentBodyFactory.create(body));
        final DocumentRevision rev = ds.updateDocumentFromRevision(rootRevision); // 2x

        body.put("My", "thirdBody");
        rev.setBody(DocumentBodyFactory.create(body));
        DocumentRevision updated = ds.updateDocumentFromRevision(rev);

        final String sql = "INSERT INTO revs (doc_id, parent, current, deleted, available, revid," +
                " json) SELECT doc_id, parent, current, deleted, available, revid, json FROM revs";
        final String updateCurrent = "UPDATE revs SET current=0";

        ds.runOnDbQueue(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                db.execSQL(sql, new String[]{});
                db.execSQL(updateCurrent, new String[]{});
                return null;
            }
        }).get();


        final String changeParent = "UPDATE revs SET parent=4 where parent = 1 AND sequence = 5";
        final String changeOtherParent = "UPDATE revs SET parent =5 where parent = 2 AND sequence" +
                " = 6";
        ds.runOnDbQueue(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                db.execSQL(changeParent, new String[]{});
                db.execSQL(changeOtherParent, new String[]{});
                return null;
            }
        }).get();

        runMigration();


        int count = revisionCount();
        Assert.assertThat("Only one of the duplicated trees should have been removed", count, is
                (3));
        Assert.assertThat(getRevs(), containsInAnyOrder(rootRevision.revision, rev.revision,
                updated.revision));
    }

    /**
     *  Tree with the structure
     *
     *  abc:
     *
     *  1-xxxxx --------> 2-zzzzzzzz -----> 3-yyyyyyyyy (deleted)
     *
     *  1-xxxxx --------> 2-yyyyyyyyy (deleted)
     *
     * NOTE: Due to implementation, we can't check if the revs here are the ones we expect since
     * we don't get a list of revs back from the datastore when we perform a bulk delete.
     * @throws Exception
     */
    @Test
    public void testDatabaseWith2LevelTreeOneDelete() throws Exception {

        Map<String, Object> body = rootRevision.getBody().asMap();
        body.put("My", "otherBody");
        rootRevision.setBody(DocumentBodyFactory.create(body));
        final DocumentRevision rev = ds.updateDocumentFromRevision(rootRevision); // 2x

        final String updateCurrent = "UPDATE revs SET current=0";

        ds.runOnDbQueue(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                db.execSQL(duplicateRevs, new String[]{rootRevision.getRevision()});
                db.execSQL(updateCurrent, new String[]{});
                return null;
            }
        }).get();

        ds.deleteDocument(rev.id);


        final String changeParent = "UPDATE revs SET parent=3 where sequence = 5";
        ds.runOnDbQueue(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                db.execSQL(changeParent, new String[]{});
                return null;
            }
        }).get();

        runMigration();


        int count = revisionCount();
        Assert.assertThat("Only 4 revs should be in the database.", count, is(4));
    }

    /**
     * Test the migration with a tree:
     *
     *  abc:
     *
     *   1-xxxx ----------> 2-yyyyyyyy -> 3-zzzzzzz
     *              |
     *              ------> 2-yyyyyyyy
     *
     * @throws Exception
     */
    @Test
    public void testDatabaseWithSecondLevelDuplicatesExtraNodesAttached() throws Exception {

        // sets up the datastore with 1 1-x and with 2 2-x.
        Map<String, Object> body = rootRevision.getBody().asMap();
        body.put("My", "otherBody");
        rootRevision.setBody(DocumentBodyFactory.create(body));
        final DocumentRevision rev = ds.updateDocumentFromRevision(rootRevision); // 2x

        final String updateCurrent = "UPDATE revs SET current=0;";
        final String setCurrent = "UPDATE revs SET current=1 where sequence=2";

        ds.runOnDbQueue(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                db.execSQL(duplicateRevs, new String[]{rev.getRevision()});
                db.execSQL(updateCurrent, new String[]{});
                db.execSQL(setCurrent, new String[]{});
                return null;
            }
        }).get();


        body.put("myAwesomeness", "level 0");
        rev.setBody(DocumentBodyFactory.create(body));
        final DocumentRevision secondUpdate = ds.updateDocumentFromRevision(rev);
        // parent needs to be changed for the above, then a new revision will need to be genreated.
        final String changeParent = "UPDATE revs SET parent=2 where parent = 3";
        ds.runOnDbQueue(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                db.execSQL(changeParent, new String[]{});
                return null;
            }
        }).get();


        runMigration();


        int count = revisionCount();
        Assert.assertThat("Only 3 revisions are in the database.", count, is(3));
        Assert.assertThat(getRevs(), containsInAnyOrder(rootRevision.revision, rev.revision,
                secondUpdate.revision));
    }

    /**
     *  Tests the migration with a database with the following structure
     *
     *   abc:
     *   1-xxxx
     *
     *   def:
     *   1-xxxx
     *
     * @throws Exception
     */
    @Test
    public void testDatabaseWithTwoDDifferentDocsSameRev() throws Exception {

        Map<String, Object> body = rootRevision.getBody().asMap();
        body.put("My", "otherBody");

        DocumentRevision rev = new DocumentRevision("awesomeness", rootRevision.revision,
                rootRevision.getBody());
        ds.forceInsert(rev, rootRevision.getRevision());

        runMigration();


        int count = revisionCount();
        Assert.assertThat("Only 2 revisions should be in the database.", count, is(2));
        Assert.assertThat("2 documents shouldbe in the datastore.", ds.getDocumentCount(), is(2));
    }

    /**
     * Tests if the migration works when the rev tree is set up like:
     *
     *  id: abc
     *
     *   1-xxxx
     *
     *   1-xxxxx
     *
     *
     *  id: def
     *
     *  1-xxx ----> 2 xyz
     *
     * @throws Exception
     */
    @Test
    public void testDatabaseWithTwoDDifferentDocsSameRevOneTwoNodeTree() throws Exception {

        Map<String, Object> body = rootRevision.getBody().asMap();
        body.put("My", "otherBody");

        final String updateCurrent = "UPDATE revs SET current=0 where sequence=2";

        ds.runOnDbQueue(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                db.execSQL(duplicateRevs, new String[]{rootRevision.getRevision()});
                db.execSQL(updateCurrent, new String[]{});
                return null;
            }
        }).get();


        DocumentRevision rev = new DocumentRevision("awesomeness", rootRevision.revision,
                rootRevision.getBody());
        ds.forceInsert(rev, rootRevision.getRevision());

        rev = ds.getDocument("awesomeness");
        rev.setBody(DocumentBodyFactory.create(body));
        DocumentRevision update = ds.updateDocumentFromRevision(rev);

        runMigration();


        int count = revisionCount();
        Assert.assertThat("Only 3 revisions should be in the database", count, is(3));
        Assert.assertThat("Only two documents should be in the datastore", ds.getDocumentCount(),
                is(2));
        Assert.assertThat(getRevs(), containsInAnyOrder(rootRevision.revision, rev.revision,
                update.revision));
    }


    /**
     * <p>
     * This test is for https://github.com/cloudant/sync-android/issues/326. The test nefariously
     * creates a tree with two roots with identical revision IDs (replicating the type of tree
     * created by a bug with parallel replications and force insert). The two revisions have
     * different bodies to help us assert easily which is which (note that revision IDs were not
     * calculated based on body content in this library at the time this test was created).
     * </p>
     * <p>
     * Once those two revisions are in place a deletion of the revision ID is force inserted as would
     * happen during a replication. When the bug was in place pickWinner did not choose a new winner
     * and the tree was left without a current winner revision for the document. The correct
     * behaviour is for the non-deleted duplicate revision to win.
     * </p>
     *
     * @throws Exception
     */
    @Test
    public void testPickWinnerOfConflicts_TwoOfSameRoot_OneDeleted() throws Exception {

        final DocumentBodyImpl bodyOne = new DocumentBodyImpl(
                FileUtils.readFileToByteArray(TestUtils.loadFixture("fixture/document_1.json")));
        final DocumentBodyImpl bodyTwo = new DocumentBodyImpl(
                FileUtils.readFileToByteArray(TestUtils.loadFixture("fixture/document_2.json")));

        final String OBJECT_ID = "object_id";

        DocumentRevision rev1a = new DocumentRevisionBuilder().setDocId(OBJECT_ID).setRevId("1-x").setBody(bodyOne).build();
        DocumentRevision rev2 = new DocumentRevisionBuilder().setDocId(OBJECT_ID).setRevId("2-x").setDeleted(true).build();

        // Insert the same document twice, so we have two identical roots
        // We give them different bodies so we can tell them apart

        // Force insert the first
        ds.forceInsert(rev1a, "1-x");

        // fetch back the document we just inserted because we need the numeric id
        final long doc_id = ds.getDocument(OBJECT_ID).getInternalNumericId();

        // Now insert a duplicate (don't use forceInsert because that will protect against
        // duplicate entries).
        ds.runOnDbQueue(new SQLCallable<Long>() {
            @Override
            public Long call(SQLDatabase db) throws Exception {
                ContentValues args = new ContentValues();
                args.put("doc_id", doc_id);
                args.put("revid", "1-x");
                args.put("json", bodyTwo.asBytes());
                return db.insert("revs", args);
            }
        }).get();

        // Now force insert the deleted
        ds.forceInsert(rev2, "1-x", "2-x");

        // Get the document
        DocumentRevision doc = ds.getDocument(OBJECT_ID);
        // If there is no winner an exception would be thrown.

        // We favour non-deleted nodes so the winner should be a 1-x
        Assert.assertEquals("The document winner should be the expected revision", "1-x", doc
                .getRevision());
        // The winner should be the one with body two because the 2-x deleted revision should graft
        // onto the lowest seq of the two identical 1-x revisions.
        Assert.assertEquals("The document winner should have the expected body", bodyTwo.asMap(),
                doc.getBody().asMap());
    }

    private void runMigration() throws NoSuchFieldException, IllegalAccessException {
        getQueue().updateSchema(new MigrateDatabase100To200(DatastoreConstants.getSchemaVersion200()), 201);
    }

    private int revisionCount() throws InterruptedException, ExecutionException {
        return ds.runOnDbQueue(new SQLCallable<Integer>() {
            @Override
            public Integer call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                int count1 = 0;
                try {
                    cursor = db.rawQuery("SELECT * FROM revs", new String[]{});
                    while (cursor.moveToNext()) {
                        count1++;
                    }
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }

                return count1;
            }
        }).get();
    }


    private SQLDatabaseQueue getQueue() throws NoSuchFieldException, IllegalAccessException {
        Class<? extends Database> clazz = ds.getClass();
        Field field = clazz.getDeclaredField("queue");
        field.setAccessible(true);
        return (SQLDatabaseQueue) field.get(ds);
    }

    private List<String> getRevs() throws InterruptedException, ExecutionException {
        return ds.runOnDbQueue(new SQLCallable<List<String>>() {
            @Override
            public List<String> call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                List<String> revs = new ArrayList<String>();
                try {
                    cursor = db.rawQuery("SELECT revid FROM revs", null);
                    while (cursor.moveToNext()) {
                        revs.add(cursor.getString(0));
                    }
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
                return revs;
            }
        }).get();
    }

}
