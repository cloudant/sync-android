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

import com.cloudant.sync.datastore.migrations.MigrateDatabase100To200;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLDatabaseQueue;
import com.cloudant.sync.sqlite.SQLQueueCallable;
import com.cloudant.sync.util.DatabaseUtils;
import com.cloudant.sync.util.TestUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

    private DatastoreImpl ds;
    private DatastoreManager manager;
    private DocumentRevision rootRevision;
    private String dir;
    private final String duplicateRevs = "INSERT INTO revs (doc_id, parent, current, deleted, " +
            "available, revid, json) SELECT doc_id, parent, current, deleted, available, revid, " +
            "json FROM revs where revs.revid = ?";

    @Before
    public void setUp() throws Exception {
        dir = TestUtils.createTempTestingDir(this.getClass().getName());
        manager = DatastoreManager.getInstance(dir);
        ds = (DatastoreImpl) manager.openDatastore("complexTree");

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

        ds.runOnDbQueue(new SQLQueueCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                db.execSQL(duplicateRevs, new String[]{rootRevision.getRevision()});
                db.execSQL(updateCurrent, new String[]{});
                return null;
            }
        }).get();

        DocumentRevision rev = new DocumentRevision(rootRevision.id, "1-abc");
        rev.setBody(rootRevision.getBody());
        ((DatastoreImpl) ds).forceInsert(rev, "1-abc");
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

        ds.runOnDbQueue(new SQLQueueCallable<Void>() {
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
        ds.runOnDbQueue(new SQLQueueCallable<Void>() {
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

        ds.runOnDbQueue(new SQLQueueCallable<Void>() {
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
        ds.runOnDbQueue(new SQLQueueCallable<Void>() {
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

        ds.runOnDbQueue(new SQLQueueCallable<Void>() {
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

        ds.runOnDbQueue(new SQLQueueCallable<Void>() {
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
        ds.runOnDbQueue(new SQLQueueCallable<Void>() {
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

        ds.runOnDbQueue(new SQLQueueCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                db.execSQL(duplicateRevs, new String[]{rootRevision.getRevision()});
                db.execSQL(updateCurrent, new String[]{});
                return null;
            }
        }).get();

        ds.deleteDocument(rev.id);


        final String changeParent = "UPDATE revs SET parent=3 where sequence = 5";
        ds.runOnDbQueue(new SQLQueueCallable<Void>() {
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

        ds.runOnDbQueue(new SQLQueueCallable<Void>() {
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
        ds.runOnDbQueue(new SQLQueueCallable<Void>() {
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

        ds.runOnDbQueue(new SQLQueueCallable<Void>() {
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


    private void runMigration() throws NoSuchFieldException, IllegalAccessException {
        getQueue().updateSchema(new MigrateDatabase100To200(), 201);
    }

    private int revisionCount() throws InterruptedException, ExecutionException {
        return ds.runOnDbQueue(new SQLQueueCallable<Integer>() {
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
        Class<? extends Datastore> clazz = ds.getClass();
        Field field = clazz.getDeclaredField("queue");
        field.setAccessible(true);
        return (SQLDatabaseQueue) field.get(ds);
    }

    private List<String> getRevs() throws InterruptedException, ExecutionException {
        return ds.runOnDbQueue(new SQLQueueCallable<List<String>>() {
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
