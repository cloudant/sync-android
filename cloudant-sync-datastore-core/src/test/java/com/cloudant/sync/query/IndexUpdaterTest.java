//  Copyright (c) 2014 Cloudant. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.cloudant.sync.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentException;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.util.DatabaseUtils;
import com.cloudant.sync.util.TestUtils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

@RunWith(Parameterized.class)
public class IndexUpdaterTest extends AbstractIndexTestBase {

    private static final String TEXT_INDEX_EXECUTION = "Execute TEXT index update tests";
    private static final String JSON_INDEX_EXECUTION = "Execute JSON index update tests";

    private String testType = null;

    List<String> fields;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() throws Exception {
        return Arrays.asList(new Object[][]{ { TEXT_INDEX_EXECUTION },
                                             { JSON_INDEX_EXECUTION } });
    }

    public IndexUpdaterTest(String testType) {
        this.testType = testType;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        fields = null;
    }

    @Test
    public void updateIndexNoIndexName() throws Exception {
        createIndex("basic", Arrays.<Object>asList("name"));
        assertThat(IndexUpdater.updateIndex(null, fields, ds, indexManagerDatabaseQueue), is(false));
    }

    @Test
    public void updateOneFieldIndex() throws Exception {
        createIndex("basic", Arrays.<Object>asList("name"));

        assertThat(getIndexSequenceNumber("basic"), is(0l));

        String table = IndexManager.tableNameForIndex("basic");
        final String sql = String.format("SELECT * FROM %s", table);

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(0));
                    return null;
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
            }
        }).get();



        DocumentRevision rev = new DocumentRevision("id123");
        // body content: { "name" : "mike" }
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        final DocumentRevision saved;
        saved = ds.createDocumentFromRevision(rev);


        assertThat(IndexUpdater.updateIndex("basic", fields, ds, indexManagerDatabaseQueue), is(true));
        assertThat(getIndexSequenceNumber("basic"), is(1l));

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(1));
                    assertThat(cursor.getColumnCount(), is(3));
                    while (cursor.moveToNext()) {
                        assertThat(cursor.columnName(0), is("_id"));
                        assertThat(cursor.getString(0), is("id123"));
                        assertThat(cursor.columnName(1), is("_rev"));
                        assertThat(cursor.getString(1), is(saved.getRevision()));
                        assertThat(cursor.columnName(2), is("name"));
                        assertThat(cursor.getString(2), is("mike"));
                    }
                    return null;
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
            }
        }).get();

    }


    @Test
    public void updateOneFieldIndexMultithreaded() throws Exception {
        createIndex("basic", Arrays.<Object>asList("name"));

        assertThat(getIndexSequenceNumber("basic"), is(0l));

        String table = IndexManager.tableNameForIndex("basic");
        final String sql = String.format("SELECT * FROM %s", table);

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(0));
                    return null;
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
            }
        }).get();


        final int nDocs = 50;
        final int nThreads = 8;
        final int nUpdates = 10; // update index after how many docs?

        // populate nDocs documents per thread, across nThreads simultaneously
        // and update index in batches (update occurs every nUpdates docs)

        final CountDownLatch latch = new CountDownLatch(nThreads);

        class PopulateThread extends Thread {
            @Override
            public void run() {
                // try to get all threads to start simultaneously
                latch.countDown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Assert.fail(e.toString());
                }
                for (int i = 0; i < nDocs; i++) {
                    DocumentRevision rev = new DocumentRevision(String.format("id_%d_%s",i,Thread.currentThread().getName()));
                    Map<String, Object> bodyMap = new HashMap<String, Object>();
                    if (i % 2 == 0) {
                        bodyMap.put("name", "mike");
                    } else {
                        bodyMap.put("name", "tom");
                    }
                    rev.setBody(DocumentBodyFactory.create(bodyMap));
                    try {
                        ds.createDocumentFromRevision(rev);
                    } catch (DocumentException de) {
                        Assert.fail("Exception thrown when creating revision " + de);
                    }
                    // batch up index updates
                    if (i % nUpdates == nUpdates-1) {
                        try {
                            IndexUpdater.updateIndex("basic", fields, ds, indexManagerDatabaseQueue);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }

        List<Thread> threads = new ArrayList<Thread>();

        // Create, start and wait for the threads to complete
        for (int i = 0; i < nThreads; i++) {
            threads.add(new PopulateThread());
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }

        // catch any missing index updates (needed where nDocs % nUpdates != 0)
        IndexUpdater.updateIndex("basic", fields, ds, indexManagerDatabaseQueue);

        // now the "basic" index should be up to date, check the sequence number
        // and the values in the index

        assertThat(getIndexSequenceNumber("basic"), is((long)nDocs*nThreads));

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                int mikes = 0;
                int toms = 0;
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(nDocs*nThreads));
                    assertThat(cursor.getColumnCount(), is(3));
                    while (cursor.moveToNext()) {
                        assertThat(cursor.columnName(0), is("_id"));
                        assertThat(cursor.columnName(1), is("_rev"));
                        assertThat(cursor.columnName(2), is("name"));
                        int docNum = Integer.valueOf(cursor.getString(0).split("_")[1]);
                        // even document numbers should have name==mike, odd name==tom
                        if (docNum % 2 == 0) {
                            assertThat(cursor.getString(2), is("mike"));
                            mikes++;
                        } else {
                            assertThat(cursor.getString(2), is("tom"));
                            toms++;
                        }
                    }
                    // check that the number of values on the name field is correct
                    assertThat(mikes, is(nDocs/2*nThreads + nDocs%2*nThreads));
                    assertThat(toms, is(nDocs/2*nThreads));
                    return null;
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
            }
        }).get();

    }

    @Test
    public void updateTwoFieldIndex() throws Exception {
        createIndex("basic", Arrays.<Object>asList("name", "age"));

        assertThat(getIndexSequenceNumber("basic"), is(0l));

        String table = IndexManager.tableNameForIndex("basic");
        final String sql = String.format("SELECT * FROM %s", table);

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(0));
                    return null;
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
            }
        }).get();

        DocumentRevision rev = new DocumentRevision("id123");
        // body content: { "name" : "mike", "age" : 12 }
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        final DocumentRevision saved;
        saved = ds.createDocumentFromRevision(rev);

        assertThat(IndexUpdater.updateIndex("basic", fields, ds, indexManagerDatabaseQueue), is(true));
        assertThat(getIndexSequenceNumber("basic"), is(1l));

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(1));
                    assertThat(cursor.getColumnCount(), is(4));
                    while (cursor.moveToNext()) {
                        assertThat(cursor.columnName(0), is("_id"));
                        assertThat(cursor.getString(0), is("id123"));
                        assertThat(cursor.columnName(1), is("_rev"));
                        assertThat(cursor.getString(1), is(saved.getRevision()));
                        assertThat(cursor.columnName(2), is("name"));
                        assertThat(cursor.getString(2), is("mike"));
                        assertThat(cursor.columnName(3), is("age"));
                        assertThat(cursor.getInt(3), is(12));
                    }
                    return null;
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
            }
        }).get();

    }

    @Test
    public void updateMultiFieldIndex() throws Exception {
        createIndex("basic", Arrays.<Object>asList("name", "age", "pet", "car"));

        assertThat(getIndexSequenceNumber("basic"), is(0l));

        String table = IndexManager.tableNameForIndex("basic");
        final String sql = String.format("SELECT * FROM %s", table);

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(0));
                    return null;
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
            }
        }).get();


        DocumentRevision rev = new DocumentRevision("id123");
        // body content: { "name" : "mike",
        //                 "age" : 12,
        //                 "pet" : "cat",
        //                 "car" : "mini",
        //                 "ignored" : "something" }
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        bodyMap.put("car", "mini");
        bodyMap.put("ignored", "something");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        final DocumentRevision saved;
        saved = ds.createDocumentFromRevision(rev);

        assertThat(IndexUpdater.updateIndex("basic", fields, ds, indexManagerDatabaseQueue), is(true));
        assertThat(getIndexSequenceNumber("basic"), is(1l));

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(1));
                    assertThat(cursor.getColumnCount(), is(6));
                    while (cursor.moveToNext()) {
                        assertThat(cursor.columnName(0), is("_id"));
                        assertThat(cursor.getString(0), is("id123"));
                        assertThat(cursor.columnName(1), is("_rev"));
                        assertThat(cursor.getString(1), is(saved.getRevision()));
                        assertThat(cursor.columnName(2), is("name"));
                        assertThat(cursor.getString(2), is("mike"));
                        assertThat(cursor.columnName(3), is("age"));
                        assertThat(cursor.getInt(3), is(12));
                        assertThat(cursor.columnName(4), is("pet"));
                        assertThat(cursor.getString(4), is("cat"));
                        assertThat(cursor.columnName(5), is("car"));
                        assertThat(cursor.getString(5), is("mini"));
                    }
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
                return null;
            }
        }).get();

    }

    @Test
    public void updateMultiFieldIndexMissingFields() throws Exception {
        createIndex("basic", Arrays.<Object>asList("name", "age", "pet", "car"));

        assertThat(getIndexSequenceNumber("basic"), is(0l));

        String table = IndexManager.tableNameForIndex("basic");
        final String sql = String.format("SELECT * FROM %s", table);

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(0));
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
                return null;
            }
        }).get();


        DocumentRevision rev = new DocumentRevision("id123");
        // body content: { "name" : "mike",  "age" : 12, "pet" : "cat", "ignored" : "something" }
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        bodyMap.put("ignored", "something");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        final DocumentRevision saved;
        saved = ds.createDocumentFromRevision(rev);

        assertThat(IndexUpdater.updateIndex("basic", fields, ds, indexManagerDatabaseQueue), is(true));
        assertThat(getIndexSequenceNumber("basic"), is(1l));

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    Assert.assertEquals(1, cursor.getCount());
                    Assert.assertEquals(6, cursor.getColumnCount());
                    while (cursor.moveToNext()) {
                        assertThat(cursor.columnName(0), is("_id"));
                        assertThat(cursor.getString(0), is("id123"));
                        assertThat(cursor.columnName(1), is("_rev"));
                        assertThat(cursor.getString(1), is(saved.getRevision()));
                        assertThat(cursor.columnName(2), is("name"));
                        assertThat(cursor.getString(2), is("mike"));
                        assertThat(cursor.columnName(3), is("age"));
                        assertThat(cursor.getInt(3), is(12));
                        assertThat(cursor.columnName(4), is("pet"));
                        assertThat(cursor.getString(4), is("cat"));
                        assertThat(cursor.columnName(5), is("car"));
                        assertThat(cursor.getString(5), is(nullValue()));
                    }
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
                return null;
            }
        }).get();

    }

    @Test
    public void updateMultiFieldIndexWithBlankRow() throws Exception {
        createIndex("basic", Arrays.<Object>asList("car", "van"));

        assertThat(getIndexSequenceNumber("basic"), is(0l));

        String table = IndexManager.tableNameForIndex("basic");
        final String sql = String.format("SELECT * FROM %s", table);

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(0));
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
                return null;
            }
        }).get();


        DocumentRevision rev = new DocumentRevision("id123");
        // body content: { "name" : "mike",  "age" : 12, "pet" : "cat", "ignored" : "something" }
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        bodyMap.put("ignored", "something");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        final DocumentRevision saved;
        saved = ds.createDocumentFromRevision(rev);

        assertThat(IndexUpdater.updateIndex("basic", fields, ds, indexManagerDatabaseQueue), is(true));
        assertThat(getIndexSequenceNumber("basic"), is(1l));

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(1));
                    assertThat(cursor.getColumnCount(), is(4));
                    while (cursor.moveToNext()) {
                        assertThat(cursor.columnName(0), is("_id"));
                        assertThat(cursor.getString(0), is("id123"));
                        assertThat(cursor.columnName(1), is("_rev"));
                        assertThat(cursor.getString(1), is(saved.getRevision()));
                        assertThat(cursor.columnName(2), is("car"));
                        assertThat(cursor.getString(2), is(nullValue()));
                        assertThat(cursor.columnName(3), is("van"));
                        assertThat(cursor.getString(3), is(nullValue()));
                    }
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
                return null;
            }
        }).get();

    }

    @Test
    public void indexSingleArrayFieldWhenIndexingArrays() throws Exception {
        createIndex("basic", Arrays.<Object>asList("name", "pet"));

        assertThat(getIndexSequenceNumber("basic"), is(0l));

        Assert.assertEquals(0, getIndexSequenceNumber("basic"));

        String table = IndexManager.tableNameForIndex("basic");
        final String sql = String.format("SELECT * FROM %s", table);

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(0));
                    return null;
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
            }
        }).get();



        DocumentRevision rev = new DocumentRevision("id123");
        // body content: { "name" : "mike",  "pet" : [ "cat", "dog", "parrot" ] }
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        List<String> pets = Arrays.asList("cat", "dog", "parrot");
        bodyMap.put("pet", pets);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        final DocumentRevision saved;
        saved = ds.createDocumentFromRevision(rev);

        assertThat(IndexUpdater.updateIndex("basic", fields, ds, indexManagerDatabaseQueue), is(true));
        assertThat(getIndexSequenceNumber("basic"), is(1l));

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(3));
                    assertThat(cursor.getColumnCount(), is(4));
                    List<String> petList = new ArrayList<String>();
                    while (cursor.moveToNext()) {
                        assertThat(cursor.columnName(0), is("_id"));
                        assertThat(cursor.getString(0), is("id123"));
                        assertThat(cursor.columnName(1), is("_rev"));
                        assertThat(cursor.getString(1), is(saved.getRevision()));
                        assertThat(cursor.columnName(2), is("name"));
                        assertThat(cursor.getString(2), is("mike"));
                        assertThat(cursor.columnName(3), is("pet"));
                        assertThat(cursor.getString(3), is(notNullValue()));
                        petList.add(cursor.getString(3));
                    }
                    assertThat(petList, containsInAnyOrder("cat", "dog", "parrot"));
                    return null;
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
            }
        }).get();

    }

    @Test
    public void indexSingleArrayFieldWhenIndexingArraysInSubDoc() throws Exception {
        createIndex("basic", Arrays.<Object>asList("name", "pet.species"));

        assertThat(getIndexSequenceNumber("basic"), is(0l));

        String table = IndexManager.tableNameForIndex("basic");
        final String sql = String.format("SELECT * FROM %s", table);

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(0));
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
                return null;
            }
        }).get();


        DocumentRevision rev = new DocumentRevision("id123");
        // body content: { "name" : "mike",  "pet" : { "species" : [ "cat", "dog" ] } }
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        List<String> species = Arrays.asList("cat", "dog");
        Map<String, Object> pets = new HashMap<String, Object>();
        pets.put("species", species);
        bodyMap.put("pet", pets);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        final DocumentRevision saved;
        saved = ds.createDocumentFromRevision(rev);

        assertThat(IndexUpdater.updateIndex("basic", fields, ds, indexManagerDatabaseQueue), is(true));
        assertThat(getIndexSequenceNumber("basic"), is(1l));

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(2));
                    assertThat(cursor.getColumnCount(), is(4));
                    List<String> petList = new ArrayList<String>();
                    while (cursor.moveToNext()) {
                        assertThat(cursor.columnName(0), is("_id"));
                        assertThat(cursor.getString(0), is("id123"));
                        assertThat(cursor.columnName(1), is("_rev"));
                        assertThat(cursor.getString(1), is(saved.getRevision()));
                        assertThat(cursor.columnName(2), is("name"));
                        assertThat(cursor.getString(2), is("mike"));
                        assertThat(cursor.columnName(3), is("pet.species"));
                        assertThat(cursor.getString(3), is(notNullValue()));
                        petList.add(cursor.getString(3));
                    }
                    assertThat(petList, containsInAnyOrder("cat", "dog"));
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
                return null;
            }
        }).get();

    }

    @Test
    public void rejectsDocsWithMultipleArrays() throws Exception {
        createIndex("basic", Arrays.<Object>asList("name", "pet", "pet2"));

        assertThat(getIndexSequenceNumber("basic"), is(0l));

        String table = IndexManager.tableNameForIndex("basic");
        final String sql = String.format("SELECT * FROM %s", table);

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(0));
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
                return null;
            }
        }).get();

        DocumentRevision goodRev = new DocumentRevision("id123");
        // body content: { "name" : "mike", "pet" : [ "cat", "dog", "parrot" ] }
        Map<String, Object> goodBodyMap = new HashMap<String, Object>();
        goodBodyMap.put("name", "mike");
        List<String> pets = Arrays.asList("cat", "dog", "parrot");
        goodBodyMap.put("pet", pets);
        goodRev.setBody(DocumentBodyFactory.create(goodBodyMap));
        final DocumentRevision saved;

        DocumentRevision badRev = new DocumentRevision("id456");
        // body content: { "name" : "mike",
        //                 "pet" : [ "cat", "dog", "parrot" ],
        //                 "pet2" : [ "cat", "dog", "parrot" ] }
        Map<String, Object> badBodyMap = new HashMap<String, Object>();
        badBodyMap.put("name", "mike");
        badBodyMap.put("pet", pets);
        badBodyMap.put("pet2", pets);
        badRev.setBody(DocumentBodyFactory.create(badBodyMap));
        saved = ds.createDocumentFromRevision(goodRev);
        ds.createDocumentFromRevision(badRev);

        assertThat(IndexUpdater.updateIndex("basic", fields, ds, indexManagerDatabaseQueue), is(true));
        assertThat(getIndexSequenceNumber("basic"), is(2l));

        // Document id123 is successfully indexed. 
        // Document id456 is rejected due to multiple arrays.
        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(3));
                    assertThat(cursor.getColumnCount(), is(5));
                    List<String> petList = new ArrayList<String>();
                    while (cursor.moveToNext()) {
                        assertThat(cursor.columnName(0), is("_id"));
                        assertThat(cursor.getString(0), is("id123"));
                        assertThat(cursor.columnName(1), is("_rev"));
                        assertThat(cursor.getString(1), is(saved.getRevision()));
                        assertThat(cursor.columnName(2), is("name"));
                        assertThat(cursor.getString(2), is("mike"));
                        assertThat(cursor.columnName(3), is("pet"));
                        assertThat(cursor.getString(3), is(notNullValue()));
                        petList.add(cursor.getString(3));
                        assertThat(cursor.columnName(4), is("pet2"));
                        assertThat(cursor.getString(4), is(nullValue()));
                    }
                    assertThat(petList, containsInAnyOrder("cat", "dog", "parrot"));
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
                return null;
            }
        }).get();

    }

    @Test
    public void indexSingleArrayFieldWithEmptyValue() throws Exception {
        createIndex("basic", Arrays.<Object>asList("name", "car", "pet"));

        assertThat(getIndexSequenceNumber("basic"), is(0l));

        String table = IndexManager.tableNameForIndex("basic");
        final String sql = String.format("SELECT * FROM %s", table);

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(0));
                    return null;
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
            }
        }).get();



        DocumentRevision rev = new DocumentRevision("id123");
        // body content: { "name" : "mike", "pet" : [] }
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("pet", new ArrayList<Object>());
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        final DocumentRevision saved;
        saved = ds.createDocumentFromRevision(rev);

        assertThat(IndexUpdater.updateIndex("basic", fields, ds, indexManagerDatabaseQueue), is(true));
        assertThat(getIndexSequenceNumber("basic"), is(1l));

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    Assert.assertEquals(1, cursor.getCount());
                    Assert.assertEquals(5, cursor.getColumnCount());
                    while (cursor.moveToNext()) {
                        assertThat(cursor.columnName(0), is("_id"));
                        assertThat(cursor.getString(0), is("id123"));
                        assertThat(cursor.columnName(1), is("_rev"));
                        assertThat(cursor.getString(1), is(saved.getRevision()));
                        assertThat(cursor.columnName(2), is("name"));
                        assertThat(cursor.getString(2), is("mike"));
                        assertThat(cursor.columnName(3), is("car"));
                        assertThat(cursor.getString(3), is(nullValue()));
                        assertThat(cursor.columnName(4), is("pet"));
                        assertThat(cursor.getString(4), is(nullValue()));
                    }
                    return null;
                }
                finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
            }
        }).get();


    }

    @Test
    public void indexSingleArrayFieldInSubDocWithEmptyValue() throws Exception {
        createIndex("basic", Arrays.<Object>asList("name", "car", "pet.species"));

        assertThat(getIndexSequenceNumber("basic"), is(0l));

        String table = IndexManager.tableNameForIndex("basic");
        final String sql = String.format("SELECT * FROM %s", table);

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(0));
                    return null;
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
            }
        }).get();

        DocumentRevision rev = new DocumentRevision("id123");
        // body content: { "name" : "mike",  "pet" : { "species" : [] } }
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        Map<String, Object> pets = new HashMap<String, Object>();
        pets.put("species", new ArrayList<Object>());
        bodyMap.put("pet", pets);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        final DocumentRevision saved;
        saved = ds.createDocumentFromRevision(rev);

        assertThat(IndexUpdater.updateIndex("basic", fields, ds, indexManagerDatabaseQueue), is(true));
        assertThat(getIndexSequenceNumber("basic"), is(1l));

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sql, new String[]{});
                    Assert.assertEquals(1, cursor.getCount());
                    Assert.assertEquals(5, cursor.getColumnCount());
                    while (cursor.moveToNext()) {
                        assertThat(cursor.columnName(0), is("_id"));
                        assertThat(cursor.getString(0), is("id123"));
                        assertThat(cursor.columnName(1), is("_rev"));
                        assertThat(cursor.getString(1), is(saved.getRevision()));
                        assertThat(cursor.columnName(2), is("name"));
                        assertThat(cursor.getString(2), is("mike"));
                        assertThat(cursor.columnName(3), is("car"));
                        assertThat(cursor.getString(3), is(nullValue()));
                        assertThat(cursor.columnName(4), is("pet.species"));
                        assertThat(cursor.getString(4), is(nullValue()));
                    }
                    return null;
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
            }
        }).get();
    }

    @Test
    public void updateAllIndexes() throws Exception {
        DocumentRevision rev = new DocumentRevision("mike12");
        // body content: { "name" : "mike",  "age" : 12, "pet" : "cat" }
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("mike23");
        // body content: { "name" : "mike",  "age" : 23, "pet" : "parrot" }
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 23);
        bodyMap.put("pet", "cat");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("mike34");
        // body content: { "name" : "mike",  "age" : 34, "pet" : "dog" }
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "dog");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("john72");
        // body content: { "name" : "john",  "age" : 72, "pet" : "fish" }
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("age", 72);
        bodyMap.put("pet", "fish");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fred34");
        // body content: { "name" : "fred",  "age" : 34, "pet" : "snake" }
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "snake");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fred12");
        // body content: { "name" : "fred",  "age" : 12 }
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        // Test index updates for multiple json indexes as well as
        // index updates for co-existing json and text indexes.
        if (testType.equals(TEXT_INDEX_EXECUTION)) {
            createIndex("basic", Arrays.<Object>asList("age", "pet", "name"), IndexType.TEXT);
        } else {
            createIndex("basic", Arrays.<Object>asList("age", "pet", "name"), IndexType.JSON);
        }
        createIndex("basicName", Arrays.<Object>asList("name"), IndexType.JSON);

        im.updateAllIndexes();

        assertThat(getIndexSequenceNumber("basic"), is(6l));
        assertThat(getIndexSequenceNumber("basicName"), is(6l));

        String basicTable = IndexManager.tableNameForIndex("basic");
        String basicNameTable = IndexManager.tableNameForIndex("basicName");
        final String sqlBasic = String.format("SELECT * FROM %s", basicTable);
        final String sqlBasicName = String.format("SELECT * FROM %s", basicNameTable);

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sqlBasic, new String[]{});
                    assertThat(cursor.getCount(), is(6));
                    assertThat(cursor.getColumnCount(), is(5));
                    while (cursor.moveToNext()) {
                        assertThat(cursor.columnName(0), is("_id"));
                        assertThat(cursor.columnName(1), is("_rev"));
                        assertThat(cursor.columnName(2), is("age"));
                        assertThat(cursor.columnName(3), is("pet"));
                        assertThat(cursor.columnName(4), is("name"));
                    }
                    DatabaseUtils.closeCursorQuietly(cursor);

                    cursor = null;

                    cursor = db.rawQuery(sqlBasicName, new String[]{});
                    assertThat(cursor.getCount(), is(6));
                    assertThat(cursor.getColumnCount(), is(3));
                    while (cursor.moveToNext()) {
                        assertThat(cursor.columnName(0), is("_id"));
                        assertThat(cursor.columnName(1), is("_rev"));
                        assertThat(cursor.columnName(2), is("name"));
                    }
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
                return null;
            }
        }).get();



        rev = new DocumentRevision("newdoc");
        rev.setBody(DocumentBodyFactory.EMPTY);
        ds.createDocumentFromRevision(rev);

        im.updateAllIndexes();

        assertThat(getIndexSequenceNumber("basic"), is(7l));
        assertThat(getIndexSequenceNumber("basicName"), is(7l));

        indexManagerDatabaseQueue.submit(new SQLCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    cursor = db.rawQuery(sqlBasic, new String[]{});
                    assertThat(cursor.getCount(), is(7));
                    assertThat(cursor.getColumnCount(), is(5));
                    while (cursor.moveToNext()) {
                        assertThat(cursor.columnName(0), is("_id"));
                        assertThat(cursor.columnName(1), is("_rev"));
                        assertThat(cursor.columnName(2), is("age"));
                        assertThat(cursor.columnName(3), is("pet"));
                        assertThat(cursor.columnName(4), is("name"));
                    }

                    DatabaseUtils.closeCursorQuietly(cursor);
                    cursor = null;
                    cursor = db.rawQuery(sqlBasicName, new String[]{});
                    assertThat(cursor.getCount(), is(7));
                    assertThat(cursor.getColumnCount(), is(3));
                    while (cursor.moveToNext()) {
                        assertThat(cursor.columnName(0), is("_id"));
                        assertThat(cursor.columnName(1), is("_rev"));
                        assertThat(cursor.columnName(2), is("name"));
                    }
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
                return null;
            }
        }).get();

    }

    private long getIndexSequenceNumber(String indexName) throws Exception {
        String where = String.format("index_name = \"%s\" group by last_sequence", indexName);
        final String sql = String.format("SELECT last_sequence FROM %s where %s",
                                   IndexManager.INDEX_METADATA_TABLE_NAME,
                                   where);

       return indexManagerDatabaseQueue.submit(new SQLCallable<Long>() {
            @Override
            public Long call(SQLDatabase db) throws Exception {
                Cursor cursor = null;
                try {
                    Long lastSequence = 0l;
                    cursor = db.rawQuery(sql, new String[]{});
                    assertThat(cursor.getCount(), is(1));
                    while (cursor.moveToNext()) {
                        lastSequence = cursor.getLong(0);
                    }
                    return lastSequence;
                } finally {
                    DatabaseUtils.closeCursorQuietly(cursor);
                }
            }
        }).get();
    }

    private void createIndex(String indexName, List<Object> fieldNames) {
        if (testType.equals(TEXT_INDEX_EXECUTION)) {
            createIndex(indexName, fieldNames, IndexType.TEXT);
        } else {
            createIndex(indexName, fieldNames, IndexType.JSON);
        }
    }

    @SuppressWarnings("unchecked")
    private void createIndex(String indexName, List<Object> fieldNames, IndexType indexType) {
        assertThat(im.ensureIndexed(fieldNames, indexName, indexType), is(indexName));

        Map<String, Object> indexes = im.listIndexes();
        assertThat(indexes, hasKey(indexName));

        Map<String, Object> index = (Map<String, Object>) indexes.get(indexName);
        fields = (List<String>) index.get("fields");
        assertThat(fields.size(), is(fieldNames.size() + 2));
        assertThat(fields, hasItems(Arrays.copyOf(fieldNames.toArray(),
                fieldNames.size(),
                String[].class)));
        assertThat(fields, hasItems("_id", "_rev"));
    }

}
