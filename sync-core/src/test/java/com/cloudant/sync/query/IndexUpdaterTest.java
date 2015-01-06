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
        List<Object> fieldNames = Arrays.<Object>asList("name");
        createIndex("basic", fieldNames);

        Assert.assertFalse(IndexUpdater.updateIndex(null, fields, db, ds, im.getQueue()));
    }

    @Test
    public void updateOneFieldIndex() {
        List<Object> fieldNames = Arrays.<Object>asList("name");
        createIndex("basic", fieldNames);

        Assert.assertEquals(0, getIndexSequenceNumber("basic"));

        String table = IndexManager.tableNameForIndex("basic");
        String sql = String.format("SELECT * FROM %s", table);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            Assert.assertEquals(0, cursor.getCount());
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

        Assert.assertTrue(IndexUpdater.updateIndex("basic", fields, db, ds, im.getQueue()));
        Assert.assertEquals(1, getIndexSequenceNumber("basic"));

        cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            Assert.assertEquals(1, cursor.getCount());
            Assert.assertEquals(3, cursor.getColumnCount());
            while (cursor.moveToNext()) {
                Assert.assertTrue(cursor.columnName(0).equals("_id"));
                Assert.assertTrue(cursor.getString(0).equals("id123"));
                Assert.assertTrue(cursor.columnName(1).equals("_rev"));
                Assert.assertTrue(cursor.getString(1).equals(saved.getRevision()));
                Assert.assertTrue(cursor.columnName(2).equals("name"));
                Assert.assertTrue(cursor.getString(2).equals("mike"));
            }
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    @Test
    public void updateTwoFieldIndex() {
        List<Object> fieldNames = Arrays.<Object>asList("name", "age");
        createIndex("basic", fieldNames);

        Assert.assertEquals(0, getIndexSequenceNumber("basic"));

        String table = IndexManager.tableNameForIndex("basic");
        String sql = String.format("SELECT * FROM %s", table);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            Assert.assertEquals(0, cursor.getCount());
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

        Assert.assertTrue(IndexUpdater.updateIndex("basic", fields, db, ds, im.getQueue()));
        Assert.assertEquals(1, getIndexSequenceNumber("basic"));

        cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            Assert.assertEquals(1, cursor.getCount());
            Assert.assertEquals(4, cursor.getColumnCount());
            while (cursor.moveToNext()) {
                Assert.assertTrue(cursor.columnName(0).equals("_id"));
                Assert.assertTrue(cursor.getString(0).equals("id123"));
                Assert.assertTrue(cursor.columnName(1).equals("_rev"));
                Assert.assertTrue(cursor.getString(1).equals(saved.getRevision()));
                Assert.assertTrue(cursor.columnName(2).equals("name"));
                Assert.assertTrue(cursor.getString(2).equals("mike"));
                Assert.assertTrue(cursor.columnName(3).equals("age"));
                Assert.assertEquals(12, cursor.getInt(3));
            }
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    @Test
    public void updateMultiFieldIndex() {
        List<Object> fieldNames = Arrays.<Object>asList("name", "age", "pet", "car");
        createIndex("basic", fieldNames);

        Assert.assertEquals(0, getIndexSequenceNumber("basic"));

        String table = IndexManager.tableNameForIndex("basic");
        String sql = String.format("SELECT * FROM %s", table);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            Assert.assertEquals(0, cursor.getCount());
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

        Assert.assertTrue(IndexUpdater.updateIndex("basic", fields, db, ds, im.getQueue()));
        Assert.assertEquals(1, getIndexSequenceNumber("basic"));

        cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            Assert.assertEquals(1, cursor.getCount());
            Assert.assertEquals(6, cursor.getColumnCount());
            while (cursor.moveToNext()) {
                Assert.assertTrue(cursor.columnName(0).equals("_id"));
                Assert.assertTrue(cursor.getString(0).equals("id123"));
                Assert.assertTrue(cursor.columnName(1).equals("_rev"));
                Assert.assertTrue(cursor.getString(1).equals(saved.getRevision()));
                Assert.assertTrue(cursor.columnName(2).equals("name"));
                Assert.assertTrue(cursor.getString(2).equals("mike"));
                Assert.assertTrue(cursor.columnName(3).equals("age"));
                Assert.assertEquals(12, cursor.getInt(3));
                Assert.assertTrue(cursor.columnName(4).equals("pet"));
                Assert.assertTrue(cursor.getString(4).equals("cat"));
                Assert.assertTrue(cursor.columnName(5).equals("car"));
                Assert.assertTrue(cursor.getString(5).equals("mini"));
            }
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    @Test
    public void updateMultiFieldIndexMissingFields() {
        List<Object> fieldNames = Arrays.<Object>asList("name", "age", "pet", "car");
        createIndex("basic", fieldNames);

        Assert.assertEquals(0, getIndexSequenceNumber("basic"));

        String table = IndexManager.tableNameForIndex("basic");
        String sql = String.format("SELECT * FROM %s", table);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            Assert.assertEquals(0, cursor.getCount());
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

        Assert.assertTrue(IndexUpdater.updateIndex("basic", fields, db, ds, im.getQueue()));
        Assert.assertEquals(1, getIndexSequenceNumber("basic"));

        cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            Assert.assertEquals(1, cursor.getCount());
            Assert.assertEquals(6, cursor.getColumnCount());
            while (cursor.moveToNext()) {
                Assert.assertTrue(cursor.columnName(0).equals("_id"));
                Assert.assertTrue(cursor.getString(0).equals("id123"));
                Assert.assertTrue(cursor.columnName(1).equals("_rev"));
                Assert.assertTrue(cursor.getString(1).equals(saved.getRevision()));
                Assert.assertTrue(cursor.columnName(2).equals("name"));
                Assert.assertTrue(cursor.getString(2).equals("mike"));
                Assert.assertTrue(cursor.columnName(3).equals("age"));
                Assert.assertEquals(12, cursor.getLong(3));
                Assert.assertTrue(cursor.columnName(4).equals("pet"));
                Assert.assertTrue(cursor.getString(4).equals("cat"));
                Assert.assertTrue(cursor.columnName(5).equals("car"));
                Assert.assertNull(cursor.getString(5));
            }
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    @Test
    public void updateMultiFieldIndexWithBlankRow() {
        List<Object> fieldNames = Arrays.<Object>asList("car", "van");
        createIndex("basic", fieldNames);

        Assert.assertEquals(0, getIndexSequenceNumber("basic"));

        String table = IndexManager.tableNameForIndex("basic");
        String sql = String.format("SELECT * FROM %s", table);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            Assert.assertEquals(0, cursor.getCount());
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

        Assert.assertTrue(IndexUpdater.updateIndex("basic", fields, db, ds, im.getQueue()));
        Assert.assertEquals(1, getIndexSequenceNumber("basic"));

        cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            Assert.assertEquals(1, cursor.getCount());
            Assert.assertEquals(4, cursor.getColumnCount());
            while (cursor.moveToNext()) {
                Assert.assertTrue(cursor.columnName(0).equals("_id"));
                Assert.assertTrue(cursor.getString(0).equals("id123"));
                Assert.assertTrue(cursor.columnName(1).equals("_rev"));
                Assert.assertTrue(cursor.getString(1).equals(saved.getRevision()));
                Assert.assertTrue(cursor.columnName(2).equals("car"));
                Assert.assertNull(cursor.getString(2));
                Assert.assertTrue(cursor.columnName(3).equals("van"));
                Assert.assertNull(cursor.getString(3));
            }
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    @Test
    public void indexSingleArrayFieldWhenIndexingArrays() {
        List<Object> fieldNames = Arrays.<Object>asList("name", "pet");
        createIndex("basic", fieldNames);

        Assert.assertEquals(0, getIndexSequenceNumber("basic"));

        String table = IndexManager.tableNameForIndex("basic");
        String sql = String.format("SELECT * FROM %s", table);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            Assert.assertEquals(0, cursor.getCount());
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

        Assert.assertTrue(IndexUpdater.updateIndex("basic", fields, db, ds, im.getQueue()));
        Assert.assertEquals(1, getIndexSequenceNumber("basic"));

        cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            Assert.assertEquals(3, cursor.getCount());
            Assert.assertEquals(4, cursor.getColumnCount());
            List<String> petList = new ArrayList<String>();
            while (cursor.moveToNext()) {
                Assert.assertTrue(cursor.columnName(0).equals("_id"));
                Assert.assertTrue(cursor.getString(0).equals("id123"));
                Assert.assertTrue(cursor.columnName(1).equals("_rev"));
                Assert.assertTrue(cursor.getString(1).equals(saved.getRevision()));
                Assert.assertTrue(cursor.columnName(2).equals("name"));
                Assert.assertTrue(cursor.getString(2).equals("mike"));
                Assert.assertTrue(cursor.columnName(3).equals("pet"));
                petList.add(cursor.getString(3));
            }
            Assert.assertEquals(3, petList.size());
            Assert.assertTrue(petList.contains("cat"));
            Assert.assertTrue(petList.contains("dog"));
            Assert.assertTrue(petList.contains("parrot"));
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    @Test
    public void indexSingleArrayFieldWhenIndexingArraysInSubDoc() {
        List<Object> fieldNames = Arrays.<Object>asList("name", "pet.species");
        createIndex("basic", fieldNames);

        Assert.assertEquals(0, getIndexSequenceNumber("basic"));

        String table = IndexManager.tableNameForIndex("basic");
        String sql = String.format("SELECT * FROM %s", table);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            Assert.assertEquals(0, cursor.getCount());
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

        Assert.assertTrue(IndexUpdater.updateIndex("basic", fields, db, ds, im.getQueue()));
        Assert.assertEquals(1, getIndexSequenceNumber("basic"));

        cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            Assert.assertEquals(2, cursor.getCount());
            Assert.assertEquals(4, cursor.getColumnCount());
            List<String> petList = new ArrayList<String>();
            while (cursor.moveToNext()) {
                Assert.assertTrue(cursor.columnName(0).equals("_id"));
                Assert.assertTrue(cursor.getString(0).equals("id123"));
                Assert.assertTrue(cursor.columnName(1).equals("_rev"));
                Assert.assertTrue(cursor.getString(1).equals(saved.getRevision()));
                Assert.assertTrue(cursor.columnName(2).equals("name"));
                Assert.assertTrue(cursor.getString(2).equals("mike"));
                Assert.assertTrue(cursor.columnName(3).equals("pet.species"));
                petList.add(cursor.getString(3));
            }
            Assert.assertEquals(2, petList.size());
            Assert.assertTrue(petList.contains("cat"));
            Assert.assertTrue(petList.contains("dog"));
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sql, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

    @Test
    public void rejectsDocsWithMultipleArrays() {
        List<Object> fieldNames = Arrays.<Object>asList("name", "pet", "pet2");
        createIndex("basic", fieldNames);

        Assert.assertEquals(0, getIndexSequenceNumber("basic"));

        String table = IndexManager.tableNameForIndex("basic");
        String sql = String.format("SELECT * FROM %s", table);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            Assert.assertEquals(0, cursor.getCount());
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

        Assert.assertTrue(IndexUpdater.updateIndex("basic",
                                                    fields,
                                                    db,
                                                    ds,
                                                    im.getQueue()));
        Assert.assertEquals(2, getIndexSequenceNumber("basic"));

        // Document id123 is successfully indexed. 
        // Document id456 is rejected due to multiple arrays.
        cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            Assert.assertEquals(3, cursor.getCount());
            Assert.assertEquals(5, cursor.getColumnCount());
            List<String> petList = new ArrayList<String>();
            while (cursor.moveToNext()) {
                Assert.assertTrue(cursor.columnName(0).equals("_id"));
                Assert.assertTrue(cursor.getString(0).equals("id123"));
                Assert.assertTrue(cursor.columnName(1).equals("_rev"));
                Assert.assertTrue(cursor.getString(1).equals(saved.getRevision()));
                Assert.assertTrue(cursor.columnName(2).equals("name"));
                Assert.assertTrue(cursor.getString(2).equals("mike"));
                Assert.assertTrue(cursor.columnName(3).equals("pet"));
                petList.add(cursor.getString(3));
                Assert.assertTrue(cursor.columnName(4).equals("pet2"));
                Assert.assertNull(cursor.getString(4));
            }
            Assert.assertEquals(3, petList.size());
            Assert.assertTrue(petList.contains("cat"));
            Assert.assertTrue(petList.contains("dog"));
            Assert.assertTrue(petList.contains("parrot"));
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

        List<Object> fieldNames = Arrays.<Object>asList("age", "pet", "name");
        createIndex("basic", fieldNames);

        fieldNames = Arrays.<Object>asList("name");
        createIndex("basicName", fieldNames);

        im.updateAllIndexes();

        Assert.assertEquals(6, getIndexSequenceNumber("basic"));
        Assert.assertEquals(6, getIndexSequenceNumber("basicName"));

        String basicTable = IndexManager.tableNameForIndex("basic");
        String basicNameTable = IndexManager.tableNameForIndex("basicName");
        String sqlBasic = String.format("SELECT * FROM %s", basicTable);
        String sqlBasicName = String.format("SELECT * FROM %s", basicNameTable);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sqlBasic, new String[]{});
            Assert.assertEquals(6, cursor.getCount());
            Assert.assertEquals(5, cursor.getColumnCount());
            while (cursor.moveToNext()) {
                Assert.assertTrue(cursor.columnName(0).equals("_id"));
                Assert.assertTrue(cursor.columnName(1).equals("_rev"));
                Assert.assertTrue(cursor.columnName(2).equals("age"));
                Assert.assertTrue(cursor.columnName(3).equals("pet"));
                Assert.assertTrue(cursor.columnName(4).equals("name"));
            }
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sqlBasic, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }

        try {
            cursor = db.rawQuery(sqlBasicName, new String[]{});
            Assert.assertEquals(6, cursor.getCount());
            Assert.assertEquals(3, cursor.getColumnCount());
            while (cursor.moveToNext()) {
                Assert.assertTrue(cursor.columnName(0).equals("_id"));
                Assert.assertTrue(cursor.columnName(1).equals("_rev"));
                Assert.assertTrue(cursor.columnName(2).equals("name"));
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

        Assert.assertEquals(7, getIndexSequenceNumber("basic"));
        Assert.assertEquals(7, getIndexSequenceNumber("basicName"));

        try {
            cursor = db.rawQuery(sqlBasic, new String[]{});
            Assert.assertEquals(7, cursor.getCount());
            Assert.assertEquals(5, cursor.getColumnCount());
            while (cursor.moveToNext()) {
                Assert.assertTrue(cursor.columnName(0).equals("_id"));
                Assert.assertTrue(cursor.columnName(1).equals("_rev"));
                Assert.assertTrue(cursor.columnName(2).equals("age"));
                Assert.assertTrue(cursor.columnName(3).equals("pet"));
                Assert.assertTrue(cursor.columnName(4).equals("name"));
            }
        }catch (SQLException e) {
            Assert.fail(String.format("SQLException occurred executing %s: %s", sqlBasic, e));
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }

        try {
            cursor = db.rawQuery(sqlBasicName, new String[]{});
            Assert.assertEquals(7, cursor.getCount());
            Assert.assertEquals(3, cursor.getColumnCount());
            while (cursor.moveToNext()) {
                Assert.assertTrue(cursor.columnName(0).equals("_id"));
                Assert.assertTrue(cursor.columnName(1).equals("_rev"));
                Assert.assertTrue(cursor.columnName(2).equals("name"));
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
            Assert.assertEquals(1, cursor.getCount());
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
        String name = im.ensureIndexed(fieldNames, indexName);
        Assert.assertTrue(name.equals(indexName));

        Map<String, Object> indexes = im.listIndexes();
        Assert.assertTrue(indexes.containsKey(indexName));

        Map<String, Object> index = (Map<String, Object>) indexes.get(indexName);
        fields = (List<String>) index.get("fields");
        Assert.assertEquals(fieldNames.size() + 2, fields.size());
        for (Object fieldName: fieldNames) {
            String field = (String) fieldName;
            Assert.assertTrue(fields.contains(field));
        }
    }

}