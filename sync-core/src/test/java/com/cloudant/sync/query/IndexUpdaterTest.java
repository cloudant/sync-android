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

import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.MutableDocumentRevision;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.util.DatabaseUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexUpdaterTest extends AbstractIndexTestBase {

    List<String> fields;

    @Override
    public void tearDown() {
        super.tearDown();
        fields = null;
    }

    @Test
    public void updateIndexNoIndexName() {
        createIndex("basic", Arrays.<Object>asList("name"));
        assertThat(IndexUpdater.updateIndex(null, fields, db, ds, im.getQueue()), is(false));
    }

    @Test
    public void updateOneFieldIndex() {
        createIndex("basic", Arrays.<Object>asList("name"));

        assertThat(getIndexSequenceNumber("basic"), is(0l));

        String table = IndexManager.tableNameForIndex("basic");
        String sql = String.format("SELECT * FROM %s", table);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            assertThat(cursor.getCount(), is(0));
        } catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }

        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "id123";
        // body content: { "name" : "mike" }
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        rev.body = DocumentBodyFactory.create(bodyMap);
        BasicDocumentRevision saved = null;
        try {
            saved = ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            Assert.fail(String.format("IOException occurred creating document revision: %s", e));
        }

        assertThat(IndexUpdater.updateIndex("basic", fields, db, ds, im.getQueue()), is(true));
        assertThat(getIndexSequenceNumber("basic"), is(1l));

        cursor = null;
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
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    @Test
    public void updateTwoFieldIndex() {
        createIndex("basic", Arrays.<Object>asList("name", "age"));

        assertThat(getIndexSequenceNumber("basic"), is(0l));

        String table = IndexManager.tableNameForIndex("basic");
        String sql = String.format("SELECT * FROM %s", table);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            assertThat(cursor.getCount(), is(0));
        } catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }

        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "id123";
        // body content: { "name" : "mike", "age" : 12 }
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        rev.body = DocumentBodyFactory.create(bodyMap);
        BasicDocumentRevision saved = null;
        try {
            saved = ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            Assert.fail(String.format("IOException occurred creating document revision: %s", e));
        }

        assertThat(IndexUpdater.updateIndex("basic", fields, db, ds, im.getQueue()), is(true));
        assertThat(getIndexSequenceNumber("basic"), is(1l));

        cursor = null;
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
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    @Test
    public void updateMultiFieldIndex() {
        createIndex("basic", Arrays.<Object>asList("name", "age", "pet", "car"));

        assertThat(getIndexSequenceNumber("basic"), is(0l));

        String table = IndexManager.tableNameForIndex("basic");
        String sql = String.format("SELECT * FROM %s", table);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            assertThat(cursor.getCount(), is(0));
        } catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }

        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "id123";
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
        rev.body = DocumentBodyFactory.create(bodyMap);
        BasicDocumentRevision saved = null;
        try {
            saved = ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            Assert.fail(String.format("IOException occurred creating document revision: %s", e));
        }

        assertThat(IndexUpdater.updateIndex("basic", fields, db, ds, im.getQueue()), is(true));
        assertThat(getIndexSequenceNumber("basic"), is(1l));

        cursor = null;
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
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    @Test
    public void updateMultiFieldIndexMissingFields() {
        createIndex("basic", Arrays.<Object>asList("name", "age", "pet", "car"));

        assertThat(getIndexSequenceNumber("basic"), is(0l));

        String table = IndexManager.tableNameForIndex("basic");
        String sql = String.format("SELECT * FROM %s", table);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            assertThat(cursor.getCount(), is(0));
        } catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }

        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "id123";
        // body content: { "name" : "mike",  "age" : 12, "pet" : "cat", "ignored" : "something" }
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        bodyMap.put("ignored", "something");
        rev.body = DocumentBodyFactory.create(bodyMap);
        BasicDocumentRevision saved = null;
        try {
            saved = ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            Assert.fail(String.format("IOException occurred creating document revision: %s", e));
        }

        assertThat(IndexUpdater.updateIndex("basic", fields, db, ds, im.getQueue()), is(true));
        assertThat(getIndexSequenceNumber("basic"), is(1l));

        cursor = null;
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
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    @Test
    public void updateMultiFieldIndexWithBlankRow() {
        createIndex("basic", Arrays.<Object>asList("car", "van"));

        assertThat(getIndexSequenceNumber("basic"), is(0l));

        String table = IndexManager.tableNameForIndex("basic");
        String sql = String.format("SELECT * FROM %s", table);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            assertThat(cursor.getCount(), is(0));
        } catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }

        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "id123";
        // body content: { "name" : "mike",  "age" : 12, "pet" : "cat", "ignored" : "something" }
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        bodyMap.put("ignored", "something");
        rev.body = DocumentBodyFactory.create(bodyMap);
        BasicDocumentRevision saved = null;
        try {
            saved = ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            Assert.fail(String.format("IOException occurred creating document revision: %s", e));
        }

        assertThat(IndexUpdater.updateIndex("basic", fields, db, ds, im.getQueue()), is(true));
        assertThat(getIndexSequenceNumber("basic"), is(1l));

        cursor = null;
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
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    @Test
    public void indexSingleArrayFieldWhenIndexingArrays() {
        createIndex("basic", Arrays.<Object>asList("name", "pet"));

        assertThat(getIndexSequenceNumber("basic"), is(0l));

        Assert.assertEquals(0, getIndexSequenceNumber("basic"));

        String table = IndexManager.tableNameForIndex("basic");
        String sql = String.format("SELECT * FROM %s", table);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            assertThat(cursor.getCount(), is(0));
        } catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }

        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "id123";
        // body content: { "name" : "mike",  "pet" : [ "cat", "dog", "parrot" ] }
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        List<String> pets = Arrays.asList("cat", "dog", "parrot");
        bodyMap.put("pet", pets);
        rev.body = DocumentBodyFactory.create(bodyMap);
        BasicDocumentRevision saved = null;
        try {
            saved = ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            Assert.fail(String.format("IOException occurred creating document revision: %s", e));
        }

        assertThat(IndexUpdater.updateIndex("basic", fields, db, ds, im.getQueue()), is(true));
        assertThat(getIndexSequenceNumber("basic"), is(1l));

        cursor = null;
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
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    @Test
    public void indexSingleArrayFieldWhenIndexingArraysInSubDoc() {
        createIndex("basic", Arrays.<Object>asList("name", "pet.species"));

        assertThat(getIndexSequenceNumber("basic"), is(0l));

        String table = IndexManager.tableNameForIndex("basic");
        String sql = String.format("SELECT * FROM %s", table);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            assertThat(cursor.getCount(), is(0));
        } catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }

        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "id123";
        // body content: { "name" : "mike",  "pet" : { "species" : [ "cat", "dog" ] } }
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        List<String> species = Arrays.asList("cat", "dog");
        Map<String, Object> pets = new HashMap<String, Object>();
        pets.put("species", species);
        bodyMap.put("pet", pets);
        rev.body = DocumentBodyFactory.create(bodyMap);
        BasicDocumentRevision saved = null;
        try {
            saved = ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            Assert.fail(String.format("IOException occurred creating document revision: %s", e));
        }

        assertThat(IndexUpdater.updateIndex("basic", fields, db, ds, im.getQueue()), is(true));
        assertThat(getIndexSequenceNumber("basic"), is(1l));

        cursor = null;
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
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    @Test
    public void rejectsDocsWithMultipleArrays() {
        createIndex("basic", Arrays.<Object>asList("name", "pet", "pet2"));

        assertThat(getIndexSequenceNumber("basic"), is(0l));

        String table = IndexManager.tableNameForIndex("basic");
        String sql = String.format("SELECT * FROM %s", table);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            assertThat(cursor.getCount(), is(0));
        } catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }

        MutableDocumentRevision goodRev = new MutableDocumentRevision();
        goodRev.docId = "id123";
        // body content: { "name" : "mike", "pet" : [ "cat", "dog", "parrot" ] }
        Map<String, Object> goodBodyMap = new HashMap<String, Object>();
        goodBodyMap.put("name", "mike");
        List<String> pets = Arrays.asList("cat", "dog", "parrot");
        goodBodyMap.put("pet", pets);
        goodRev.body = DocumentBodyFactory.create(goodBodyMap);
        BasicDocumentRevision saved = null;

        MutableDocumentRevision badRev = new MutableDocumentRevision();
        badRev.docId = "id456";
        // body content: { "name" : "mike",
        //                 "pet" : [ "cat", "dog", "parrot" ],
        //                 "pet2" : [ "cat", "dog", "parrot" ] }
        Map<String, Object> badBodyMap = new HashMap<String, Object>();
        badBodyMap.put("name", "mike");
        badBodyMap.put("pet", pets);
        badBodyMap.put("pet2", pets);
        badRev.body = DocumentBodyFactory.create(badBodyMap);
        try {
            saved = ds.createDocumentFromRevision(goodRev);
            ds.createDocumentFromRevision(badRev);
        } catch (IOException e) {
            Assert.fail(String.format("IOException occurred creating document revision: %s", e));
        }

        assertThat(IndexUpdater.updateIndex("basic", fields, db, ds, im.getQueue()), is(true));
        assertThat(getIndexSequenceNumber("basic"), is(2l));

        // Document id123 is successfully indexed. 
        // Document id456 is rejected due to multiple arrays.
        cursor = null;
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
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    @Test
    public void updateAllIndexes() {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "mike12";
        // body content: { "name" : "mike",  "age" : 12, "pet" : "cat" }
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            Assert.fail(String.format("IOException occurred creating document revision: %s", e));
        }

        rev.docId = "mike23";
        // body content: { "name" : "mike",  "age" : 23, "pet" : "parrot" }
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 23);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            Assert.fail(String.format("IOException occurred creating document revision: %s", e));
        }

        rev.docId = "mike34";
        // body content: { "name" : "mike",  "age" : 34, "pet" : "dog" }
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "dog");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            Assert.fail(String.format("IOException occurred creating document revision: %s", e));
        }

        rev.docId = "john72";
        // body content: { "name" : "john",  "age" : 72, "pet" : "fish" }
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("age", 72);
        bodyMap.put("pet", "fish");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            Assert.fail(String.format("IOException occurred creating document revision: %s", e));
        }

        rev.docId = "fred34";
        // body content: { "name" : "fred",  "age" : 34, "pet" : "snake" }
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "snake");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            Assert.fail(String.format("IOException occurred creating document revision: %s", e));
        }

        rev.docId = "fred12";
        // body content: { "name" : "fred",  "age" : 12 }
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            Assert.fail(String.format("IOException occurred creating document revision: %s", e));
        }

        createIndex("basic", Arrays.<Object>asList("age", "pet", "name"));
        createIndex("basicName", Arrays.<Object>asList("name"));

        im.updateAllIndexes();

        assertThat(getIndexSequenceNumber("basic"), is(6l));
        assertThat(getIndexSequenceNumber("basicName"), is(6l));

        String basicTable = IndexManager.tableNameForIndex("basic");
        String basicNameTable = IndexManager.tableNameForIndex("basicName");
        String sqlBasic = String.format("SELECT * FROM %s", basicTable);
        String sqlBasicName = String.format("SELECT * FROM %s", basicNameTable);
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
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sqlBasic, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }

        try {
            cursor = db.rawQuery(sqlBasicName, new String[]{});
            assertThat(cursor.getCount(), is(6));
            assertThat(cursor.getColumnCount(), is(3));
            while (cursor.moveToNext()) {
                assertThat(cursor.columnName(0), is("_id"));
                assertThat(cursor.columnName(1), is("_rev"));
                assertThat(cursor.columnName(2), is("name"));
            }
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sqlBasicName, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }

        rev.docId = "newdoc";
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            Assert.fail(String.format("IOException occurred creating document revision: %s", e));
        }

        im.updateAllIndexes();

        assertThat(getIndexSequenceNumber("basic"), is(7l));
        assertThat(getIndexSequenceNumber("basicName"), is(7l));

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
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sqlBasic, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }

        try {
            cursor = db.rawQuery(sqlBasicName, new String[]{});
            assertThat(cursor.getCount(), is(7));
            assertThat(cursor.getColumnCount(), is(3));
            while (cursor.moveToNext()) {
                assertThat(cursor.columnName(0), is("_id"));
                assertThat(cursor.columnName(1), is("_rev"));
                assertThat(cursor.columnName(2), is("name"));
            }
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sqlBasicName, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    private long getIndexSequenceNumber(String indexName) {
        String where = String.format("index_name = \"%s\" group by last_sequence", indexName);
        String sql = String.format("SELECT last_sequence FROM %s where %s",
                                   IndexManager.INDEX_METADATA_TABLE_NAME,
                                   where);
        Cursor cursor = null;
        Long lastSequence = 0l;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            assertThat(cursor.getCount(), is(1));
            while (cursor.moveToNext()) {
                lastSequence = cursor.getLong(0);
            }
        } catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
        return lastSequence;
    }

    @SuppressWarnings("unchecked")
    private void createIndex(String indexName, List<Object> fieldNames) {
        assertThat(im.ensureIndexed(fieldNames, indexName), is(indexName));

        Map<String, Object> indexes = im.listIndexes();
        assertThat(indexes, hasKey(indexName));

        Map<String, Object> index = (Map<String, Object>) indexes.get(indexName);
        fields = (List<String>) index.get("fields");
        assertThat(fields.size(), is(fieldNames.size() +2));
        assertThat(fields, hasItems(Arrays.copyOf(fieldNames.toArray(),
                                                  fieldNames.size(),
                                                  String[].class)));
        assertThat(fields, hasItems("_id", "_rev"));
    }

}