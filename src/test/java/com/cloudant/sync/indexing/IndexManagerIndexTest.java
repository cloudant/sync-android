/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.indexing;

import com.cloudant.sync.datastore.ConflictException;
import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.DocumentBody;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.sqlite4java.SQLiteWrapper;
import com.cloudant.sync.util.SQLDatabaseTestUtils;
import com.cloudant.sync.util.TestUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class IndexManagerIndexTest {

    SQLiteWrapper database = null;
    DatastoreExtended datastore = null;
    IndexManager indexManager = null;
    List<DocumentBody> dbBodies = null;
    private String datastoreManagerPath;

    @Before
    public void setUp() throws IOException, SQLException {
        datastoreManagerPath = TestUtils.createTempTestingDir(this.getClass().getName());
        DatastoreManager datastoreManager = new DatastoreManager(this.datastoreManagerPath);
        datastore = (DatastoreExtended) datastoreManager.openDatastore(getClass().getSimpleName());
        indexManager = new IndexManager(datastore);
        database = (SQLiteWrapper) indexManager.getDatabase();

        createTestBDBodies();
    }

    @After
    public void tearDown() throws Exception {
        TestUtils.deleteDatabaseQuietly(database);
        TestUtils.deleteTempTestingDir(datastoreManagerPath);
    }

    private void createTestBDBodies() throws IOException {
        dbBodies = new ArrayList<DocumentBody>();
        for (int i = 0; i < 7; i++) {
            dbBodies.add(TestUtils.createBDBody("fixture/index" + "_" + i + ".json"));
        }
    }

    @Test
    public void close_sqlDatabaseShouldBeClosed() {
        Assert.assertTrue(indexManager.getDatabase().isOpen());
        indexManager.onDatastoreClosed(null);
        Assert.assertFalse(indexManager.getDatabase().isOpen());
    }

    @Test
    public void getIndex_newIndex_allDataShouldBePersistent() throws IndexExistsException {
        indexManager.ensureIndexed("A", "a");
        Index index = indexManager.getIndex("A");
        Assert.assertEquals("A", index.getName());
        Assert.assertTrue(index.getLastSequence().equals(-1l));
    }

    @Test
    public void getIndex_indexNotExist_returnNull() {
        Index index = indexManager.getIndex("Z");
        Assert.assertNull(index);
    }

    @Test
    public void ensuredIndexed_newIndex_indexShouldBeCreatedAndPersistent() throws IndexExistsException, SQLException {
        {  // String index
            indexManager.ensureIndexed("Album", "album");
            Index album = indexManager.getIndex("Album");
            assertIndexDataFields("Album", "album", IndexType.STRING, -1L, album);
            String tableName = String.format(IndexManager.TABLE_INDEX_NAME_FORMAT, "Album");
            assertTableCreated(tableName);
        }

        {  // Integer index
            indexManager.ensureIndexed("a_123", "abc_123", IndexType.INTEGER);
            Index artist = indexManager.getIndex("a_123");
            assertIndexDataFields("a_123", "abc_123", IndexType.INTEGER, -1L, artist);
            String tableName = String.format(IndexManager.TABLE_INDEX_NAME_FORMAT, "a_123");
            assertTableCreated(tableName);
        }
    }

    private void assertIndexDataFields(String expectedIndexName, String expectedFieldName, IndexType expectedIndexType,
                                       Long expectLastSequence, Index actual) throws SQLException {
        Assert.assertEquals(expectedIndexName, actual.getName());
        Assert.assertEquals(expectedIndexType, actual.getIndexType());
        Assert.assertTrue(actual.getLastSequence().equals(expectLastSequence));
    }

    private void assertTableCreated(String name) throws SQLException {
        Set<String> tables = SQLDatabaseTestUtils.getAllTableNames(database);
        Assert.assertTrue(tables.contains(name));
    }

    @Test(expected = IndexExistsException.class)
    public void ensuredIndexed_duplicatedIndexNamesButDifferentField() throws IndexExistsException {
        indexManager.ensureIndexed("Album", "album");
        indexManager.ensureIndexed("Album", "album2");
    }

    @Test(expected = IndexExistsException.class)
    public void ensuredIndexed_duplicatedIndexNamesButDifferentType() throws IndexExistsException {
        indexManager.ensureIndexed("Album", "album");
        indexManager.ensureIndexed("Album", "album", IndexType.INTEGER);
    }

    @Test(expected = IndexExistsException.class)
    public void ensuredIndexed_duplicatedIndexes() throws IndexExistsException {
        indexManager.ensureIndexed("Album", "album");
        indexManager.ensureIndexed("Album", "album");
    }

    @Test
    public void getAllIndexes_allIndexesShouldBeReturned() throws IndexExistsException {
        Assert.assertEquals(0, indexManager.getAllIndexes().size());
        Index albumIndex =  createAndGetIndex("album", "album", IndexType.STRING);
        Assert.assertEquals(1, indexManager.getAllIndexes().size());

        indexManager.ensureIndexed("artist", "artist", IndexType.INTEGER);
        Index artistIndex = indexManager.getIndex("artist");

        Set<Index> indexes = indexManager.getAllIndexes();
        Assert.assertEquals(2, indexes.size());
        Assert.assertTrue(indexes.contains(albumIndex));
        Assert.assertTrue(indexes.contains(artistIndex));

        for(Index index : indexes) {
            if(index.getName().equals("album")) {
                assertSameIndex(index, albumIndex);
            } else if (index.getName().equals("artist")) {
                assertSameIndex(index, artistIndex);
            }
        }
    }

    private void assertSameIndex(Index l, Index r) {
        Assert.assertEquals(l.getName(), r.getName());
        Assert.assertEquals(l.getIndexType(), r.getIndexType());
        Assert.assertEquals(l.getLastSequence(), r.getLastSequence());
    }

    @Test
    public void deleteIndex_delete_indexShouldBeDeleted() throws IndexExistsException, SQLException {
        Index index = createAndGetIndex("album", "album", IndexType.STRING);
        Assert.assertNotNull(index);
        indexManager.deleteIndex(index.getName());
        Assert.assertEquals(0, indexManager.getAllIndexes().size());
        String indexTable = String.format(IndexManager.TABLE_INDEX_NAME_FORMAT, "album");
        assertTableDeleted(indexTable);
    }

    private void assertTableDeleted(String table) throws SQLException {
        Set<String> tables = SQLDatabaseTestUtils.getAllTableNames(database);
        Assert.assertFalse(tables.contains(table));
    }

    @Test(expected = IllegalArgumentException.class)
    public void deleteIndex_indexNotExist_exception() {
        Assert.assertNull(indexManager.getIndex("Z"));
        indexManager.deleteIndex("Z");
    }

    @Test
    public void ensuredIndexed_documentExistBeforeIndexCreated_existingDocShouldBeIndexed()
            throws IndexExistsException, SQLException {
        DocumentRevision obj0 = datastore.createDocument(dbBodies.get(0));
        DocumentRevision obj1 = datastore.createDocument(dbBodies.get(1));
        DocumentRevision obj2 = datastore.createDocument(dbBodies.get(2));

        Index index = createAndGetIndex("album", "album", IndexType.STRING);

        IndexTestUtils.assertDBObjectNotInIndex(database, index, obj0);
        IndexTestUtils.assertDBObjectInIndex(database, index, "album", obj1);
        IndexTestUtils.assertDBObjectInIndex(database, index, "album", obj2);
    }

    @Test
    public void ensure_custom_indexing_function_used_non_null_indexed()
            throws IndexExistsException, SQLException {
        DocumentRevision obj0 = datastore.createDocument(dbBodies.get(0));
        DocumentRevision obj1 = datastore.createDocument(dbBodies.get(1));
        DocumentRevision obj2 = datastore.createDocument(dbBodies.get(2));

        final class TestIF implements IndexFunction<String> {
            public List<String> indexedValues(String indexName, Map map) {
                return Arrays.asList("fred");
            }
        }

        this.indexManager.ensureIndexed("album", IndexType.STRING, new TestIF());
        Index index = indexManager.getIndex("album");

        for (DocumentRevision obj : new DocumentRevision[]{obj0,obj1,obj2}) {
            IndexTestUtils.assertDBObjectInIndexWithValue(
                    database,
                    index,
                    obj,
                    "fred"
            );
        }
    }

    @Test
    public void ensure_custom_indexing_function_used_null_not_indexed()
            throws IndexExistsException, SQLException {
        DocumentRevision obj0 = datastore.createDocument(dbBodies.get(0));
        DocumentRevision obj1 = datastore.createDocument(dbBodies.get(1));
        DocumentRevision obj2 = datastore.createDocument(dbBodies.get(2));

        final class TestIF implements IndexFunction<String> {
            public List<String> indexedValues(String indexName, Map map) {
                return null;
            }
        }

        this.indexManager.ensureIndexed("album", IndexType.STRING, new TestIF());
        Index index = indexManager.getIndex("album");

        for (DocumentRevision obj : new DocumentRevision[]{obj0,obj1,obj2}) {
            IndexTestUtils.assertDBObjectNotInIndex(
                    database,
                    index,
                    obj
            );
        }
    }

    @Test
    public void createStringIndex_documentInsertedAfterIndexCreated_documentShouldBeIndexed()
            throws IndexExistsException, SQLException {
        Index index =  createAndGetIndex("album", "album", IndexType.STRING);

        DocumentRevision obj0 = datastore.createDocument(dbBodies.get(0));
        DocumentRevision obj1 = datastore.createDocument(dbBodies.get(1));
        DocumentRevision obj2 = datastore.createDocument(dbBodies.get(2));

        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectNotInIndex(database, index, obj0);
        IndexTestUtils.assertDBObjectInIndex(database, index, "album", obj1);
        IndexTestUtils.assertDBObjectInIndex(database, index, "album", obj2);
    }

    @Test
    public void createLongIndex_documentInsertedAfterIndexCreated_documentShouldBeIndexed()
            throws IndexExistsException, SQLException {
        indexManager.ensureIndexed("class", "class", IndexType.INTEGER);
        Index index = indexManager.getIndex("class");

        {
            DocumentRevision obj0 = datastore.createDocument(dbBodies.get(0));
            indexManager.updateAllIndexes();
            IndexTestUtils.assertDBObjectNotInIndex(database, index, obj0);
        }

        {
            DocumentRevision obj1 = datastore.createDocument(dbBodies.get(1));
            indexManager.updateAllIndexes();
            IndexTestUtils.assertDBObjectInIndex(database, index, "class", obj1);
        }
    }

    @Test
    public void createLongIndex_indexValueIsBigLong_documentShouldBeIndexed()
            throws IndexExistsException, SQLException, IOException {
        indexManager.ensureIndexed("class", "class", IndexType.INTEGER);
        Index index = indexManager.getIndex("class");
        byte[] data = FileUtils.readFileToByteArray(new File("fixture/index_really_big_long.json"));
        DocumentRevision obj1 = datastore.createDocument(DocumentBodyFactory.create(data));
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectInIndex(database, index, "class", obj1);
    }

    @Test
    public void createLongIndex_indexValueIsFloat_documentShouldBeIndexed()
            throws IndexExistsException, SQLException, IOException {
        indexManager.ensureIndexed("class", "class", IndexType.INTEGER);
        Index index = indexManager.getIndex("class");
        byte[] data = FileUtils.readFileToByteArray(new File("fixture/index_float.json"));
        DocumentRevision obj1 = datastore.createDocument(DocumentBodyFactory.create(data));
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectInIndex(database, index, "class", obj1);
    }


    @Test
    public void createLongIndex_indexValueConversionNotSupported_documentShouldNotBeIndexed()
            throws IndexExistsException, SQLException, IOException {
        indexManager.ensureIndexed("class", "class", IndexType.INTEGER);
        Index index = indexManager.getIndex("class");
        byte[] data = FileUtils.readFileToByteArray(new File("fixture/index_string.json"));
        DocumentRevision obj1 = datastore.createDocument(DocumentBodyFactory.create(data));
        IndexTestUtils.assertDBObjectNotInIndex(database, index, obj1);
    }

    @Test
    public void indexString_documentWithValidField_indexRowShouldBeAdded()
            throws IndexExistsException, SQLException, IOException {
        Index index = createAndGetIndex("StringIndex", "stringIndex", IndexType.STRING);
        byte[] data = FileUtils.readFileToByteArray(new File("fixture/string_index_valid_field.json"));
        DocumentRevision obj = datastore.createDocument(DocumentBodyFactory.create(data));
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectInIndex(database, index, "stringIndex", obj);
    }

    @Test
    public void indexString_documentWithInvalidField_indexRowShouldNotBeAdded()
            throws IndexExistsException, IOException, SQLException {
        Index index = createAndGetIndex("StringIndex", "stringIndex", IndexType.STRING);
        byte[] data = FileUtils.readFileToByteArray(new File("fixture/string_index_invalid_field.json"));
        DocumentRevision obj = datastore.createDocument(DocumentBodyFactory.create(data));
        IndexTestUtils.assertDBObjectNotInIndex(database, index, obj);
    }

    @Test
    public void indexString_documentUpdated_indexRowShouldBeUpdated() throws Exception {
        Index index = createAndGetIndex("stringIndex", "stringIndex", IndexType.STRING);

        DocumentRevision obj = datastore.createDocument(TestUtils.createBDBody("fixture/string_index_valid_field.json"));
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectInIndex(database, index, "stringIndex", obj);

        DocumentRevision obj2 = datastore.updateDocument(obj.getId(), obj.getRevision(),
                TestUtils.createBDBody("fixture/string_index_valid_field_updated.json"));
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectInIndex(database, index, "stringIndex", obj2);
    }

    @Test
    public void indexString_documentUpdatedFromInvalidToValidValue_indexRowShouldBeUpdated() throws Exception {
        Index index = createAndGetIndex("stringIndex", "stringIndex", IndexType.STRING);

        DocumentRevision obj = datastore.createDocument(TestUtils.createBDBody("fixture/string_index_invalid_field.json"));
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectNotInIndex(database, index, obj);

        DocumentRevision obj2 = datastore.updateDocument(obj.getId(), obj.getRevision(),
                TestUtils.createBDBody("fixture/string_index_valid_field.json"));
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectInIndex(database, index, "stringIndex", obj2);
    }

    private Index createAndGetIndex(String indexName, String filedName, IndexType type) throws IndexExistsException {
        indexManager.ensureIndexed(indexName, filedName, type);
        return indexManager.getIndex(indexName);
    }

    @Test
    public void indexString_documentUpdatedWithInvalidValue_indexValueShouldBeRemoved() throws Exception {
        Index index = createAndGetIndex("stringIndex", "stringIndex", IndexType.STRING);

        byte[] data = FileUtils.readFileToByteArray(new File("fixture/string_index_valid_field.json"));
        DocumentRevision obj = datastore.createDocument(DocumentBodyFactory.create(data));
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectInIndex(database, index, "stringIndex", obj);

        byte[] data2 = FileUtils.readFileToByteArray(new File("fixture/string_index_invalid_field.json"));
        DocumentRevision obj2 = datastore.updateDocument(obj.getId(), obj.getRevision(), DocumentBodyFactory.create(data2));
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectNotInIndex(database, index, obj2);
    }

    @Test
    public void indexString_documentDeleted_indexRowShouldBeRemoved() throws Exception {
        Index index = createAndGetIndex("stringIndex", "stringIndex", IndexType.STRING);

        byte[] data = FileUtils.readFileToByteArray(new File("fixture/string_index_valid_field.json"));
        DocumentRevision obj = datastore.createDocument(DocumentBodyFactory.create(data));
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectInIndex(database, index, "stringIndex", obj);

        datastore.deleteDocument(obj.getId(), obj.getRevision());
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectNotInIndex(database, index, obj);
    }

    @Test
    public void indexInteger_documentCreated_indexRowShouldBeAdded() throws Exception {
        Index index = createAndGetIndex("integerIndex", "integerIndex", IndexType.INTEGER);

        byte[] data = FileUtils.readFileToByteArray(new File("fixture/integer_index_valid_field.json"));
        DocumentRevision obj = datastore.createDocument(DocumentBodyFactory.create(data));
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectInIndex(database, index, "integerIndex", obj);
    }

    @Test
    public void indexInteger_documentCreatedWithInvalidIndexValue_indexRowShouldNotBeAdded() throws Exception {
        Index index = createAndGetIndex("integerIndex", "integerIndex", IndexType.INTEGER);

        byte[] data = FileUtils.readFileToByteArray(new File("fixture/integer_index_invalid_field.json"));
        DocumentRevision obj = datastore.createDocument(DocumentBodyFactory.create(data));
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectNotInIndex(database, index, obj);
    }

    @Test
    public void indexInteger_documentUpdated_indexValueShouldBeUpdate() throws Exception {
        Index index = createAndGetIndex("integerIndex", "integerIndex", IndexType.INTEGER);

        byte[] data = FileUtils.readFileToByteArray(new File("fixture/integer_index_valid_field.json"));
        DocumentRevision obj = datastore.createDocument(DocumentBodyFactory.create(data));
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectInIndex(database, index, "integerIndex", obj);

        byte[] data2 = FileUtils.readFileToByteArray(new File("fixture/integer_index_valid_field_updated.json"));
        DocumentRevision obj2 = datastore.createDocument(DocumentBodyFactory.create(data2));
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectInIndex(database, index, "integerIndex", obj2);
    }

    @Test
    public void indexInteger_documentUpdatedFromInvalidToValidValue_indexValueShouldBeUpdate() throws Exception {
        Index index = createAndGetIndex("integerIndex", "integerIndex", IndexType.INTEGER);

        byte[] data = FileUtils.readFileToByteArray(new File("fixture/integer_index_invalid_field.json"));
        DocumentRevision obj = datastore.createDocument(DocumentBodyFactory.create(data));
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectNotInIndex(database, index, obj);

        byte[] data2 = FileUtils.readFileToByteArray(new File("fixture/integer_index_valid_field.json"));
        DocumentRevision obj2 = datastore.createDocument(DocumentBodyFactory.create(data2));
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectInIndex(database, index, "integerIndex", obj2);
    }

    @Test
    public void indexInteger_documentUpdatedWithInvalidIndexValue_indexRowShouldBeRemoved() throws Exception {
        Index index = createAndGetIndex("integerIndex", "integerIndex", IndexType.INTEGER);

        byte[] data = FileUtils.readFileToByteArray(new File("fixture/integer_index_valid_field.json"));
        DocumentRevision obj = datastore.createDocument(DocumentBodyFactory.create(data));
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectInIndex(database, index, "integerIndex", obj);

        byte[] data2 = FileUtils.readFileToByteArray(new File("fixture/integer_index_invalid_field.json"));
        DocumentRevision obj2 = datastore.createDocument(DocumentBodyFactory.create(data2));
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectNotInIndex(database, index, obj2);
    }

    @Test
    public void indexInteger_documentDeleted_indexRowShouldBeRemoved() throws Exception {
        Index index = createAndGetIndex("integerIndex", "integerIndex", IndexType.INTEGER);

        byte[] data = FileUtils.readFileToByteArray(new File("fixture/integer_index_valid_field.json"));
        DocumentRevision obj = datastore.createDocument(DocumentBodyFactory.create(data));
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectInIndex(database, index, "integerIndex", obj);

        datastore.deleteDocument(obj.getId(), obj.getRevision());
        indexManager.updateAllIndexes();
        IndexTestUtils.assertDBObjectNotInIndex(database, index, obj);
    }

    @Test
    public void multiIndex_documentWithoutTheField_notIndexed()
            throws IndexExistsException, SQLException {
        Index index = createAndGetIndex("Genre", "Genre", IndexType.STRING);
        DocumentRevision rev = datastore.createDocument(dbBodies.get(0));
        indexManager.updateAllIndexes();
        Index index2 = indexManager.getIndex("Genre");
        Assert.assertEquals(Long.valueOf(datastore.getLastSequence()),
                Long.valueOf(index2.getLastSequence()));
        this.assertIndexed(database, index, rev.getId());
    }

    @Test
    public void multiIndex_documentFieldWithListOfTwo_twoIndexValueAdded()
            throws IndexExistsException, SQLException {
        Index index = createAndGetIndex("Genre", "Genre", IndexType.STRING);
        DocumentRevision rev = datastore.createDocument(dbBodies.get(1));
        indexManager.updateAllIndexes();
        Index index2 = indexManager.getIndex("Genre");
        Assert.assertEquals(Long.valueOf(datastore.getLastSequence()),
                Long.valueOf(index2.getLastSequence()));
        this.assertIndexed(database, index, rev.getId(), "Pop", "Rock");
    }

    @Test
    public void multiIndex_documentFieldWithListOfOne_oneIndexValueAdded()
            throws IndexExistsException, SQLException {
        Index index = createAndGetIndex("Genre", "Genre", IndexType.STRING);
        DocumentRevision rev = datastore.createDocument(dbBodies.get(3));
        indexManager.updateAllIndexes();
        Index index2 = indexManager.getIndex("Genre");
        Assert.assertEquals(Long.valueOf(datastore.getLastSequence()),
                Long.valueOf(index2.getLastSequence()));
        this.assertIndexed(database, index, rev.getId(), "Pop");
    }

    @Test
    public void multiIndex_documentFieldWithDuplicatedValues_noDuplicatedValuesAdded()
            throws IndexExistsException, SQLException {
        Index index = createAndGetIndex("Genre", "Genre", IndexType.STRING);
        DocumentRevision rev = datastore.createDocument(dbBodies.get(4));
        // this Document has field: genre: [ "Pop", "Pop" ], and assert
        // only one entry added for "Pop"
        indexManager.updateAllIndexes();
        Index index2 = indexManager.getIndex("Genre");
        Assert.assertEquals(Long.valueOf(datastore.getLastSequence()),
                Long.valueOf(index2.getLastSequence()));
        this.assertIndexed(database, index, rev.getId(), "Pop");
    }

    @Test
    public void index_fieldWithUnsupportedValue_unsupportedValueShouldBeIgnored()
            throws IndexExistsException, SQLException, IOException {

        DocumentBody body = TestUtils.createBDBody("fixture/index_with_unsupported_value.json");
        DocumentRevision rev = datastore.createDocument(body);
        Index index = createAndGetIndex("Genre", "Genre", IndexType.STRING);
        Assert.assertEquals(Long.valueOf(datastore.getLastSequence()),
                Long.valueOf(index.getLastSequence()));
        this.assertIndexed(database, index, rev.getId(), "Pop", "Rock");
    }

    @Test
    public void index_valueWithLeadingTailingSpaces_spacesRemoved()
            throws IndexExistsException, SQLException, IOException {
        DocumentBody body = TestUtils.createBDBody("fixture/index_with_spaces.json");
        DocumentRevision rev = datastore.createDocument(body);
        Index index = createAndGetIndex("Genre", "Genre", IndexType.STRING);
        Assert.assertEquals(Long.valueOf(datastore.getLastSequence()),
                Long.valueOf(index.getLastSequence()));
        this.assertIndexed(database, index, rev.getId(), " Pop", "Rock ", " R & B ");
    }

    @Test
    public void index_fieldWith10KValues_allValuesShouldBeAdded()
            throws IndexExistsException, SQLException, IOException {
        Map m = new HashMap<String, String>();
        List<String> tags = new ArrayList<String>(100000);
        for(int i = 0 ; i < 100000 ; i ++) {
            tags.add("tag" + i);
        }
        m.put("Tag", tags);
        DocumentBody body = DocumentBodyFactory.create(m);
        DocumentRevision rev = datastore.createDocument(body);

        Index index = createAndGetIndex("Tag", "Tag", IndexType.STRING);
        Assert.assertEquals(Long.valueOf(datastore.getLastSequence()),
                Long.valueOf(index.getLastSequence()));
        this.assertIndexed(database, index, rev.getId(), tags.toArray(new String[]{}));
    }

    // test that index updates itself on create/update/delete
    @Test
    public void index_UpdateCrud()
            throws IndexExistsException, SQLException, ConflictException {
        Index index = createAndGetIndex("title", "title", IndexType.STRING);
        // create
        DocumentRevision obj1 = datastore.createDocument(dbBodies.get(1));
        this.assertIndexed(database, index, obj1.getId(), "Politik");
        // update
        Map<String,Object> map = obj1.getBody().asMap();
        map.put("title", "Another Green Day");
        DocumentBody body = DocumentBodyFactory.create(map);
        DocumentRevision obj2 = datastore.updateDocument(obj1.getId(), obj1.getRevision(), body);
        Assert.assertEquals(obj1.getId(), obj2.getId());
        this.assertIndexed(database, index, obj2.getId(), "Another Green Day");
        // delete
        datastore.deleteDocument(obj2.getId(), obj2.getRevision());
        this.assertNotIndexed(database, index, obj2.getId());
    }

    /**
     * A sanity-check that updating the datastore from many threads
     * doesn't cause the index manager to balk.
     */
    @Test
    public void index_UpdateCrudMultiThreaded()
            throws IndexExistsException, SQLException, ConflictException,
                   InterruptedException {
        int n_threads = 5;
        final int n_docs = 100;

        // We'll later search for search == success
        final Map<String,String> matching = ImmutableMap.of("search", "success");
        final Map<String,String> nonmatching = ImmutableMap.of("search", "failure");
        indexManager.ensureIndexed("search", "search", IndexType.STRING);

        final List<String> matching_ids = Collections.synchronizedList(new ArrayList<String>());

        // When run, this thread creates n_docs documents with unique
        // names in the datastore. A subset of these
        // will be matched by our query to the datastore later, which
        // we record in the matching_ids list.
        class PopulateThread extends Thread {

            @Override
            public void run() {
                String docId;
                final String thread_id;
                DocumentBody body;

                thread_id = Thread.currentThread().getName();
                for (int i = 0; i < n_docs; i++) {
                    docId = String.format("%s-%s", thread_id, i);

                    if ((i % 2) == 0) {  // even numbers create matching docs
                        body = DocumentBodyFactory.create(matching);
                        matching_ids.add(docId);
                    } else {
                        body = DocumentBodyFactory.create(nonmatching);
                    }
                    datastore.createDocument(docId, body);
                }
                // we're not on the main thread, so we must close our own connection
                datastore.getSQLDatabase().close();
            }
        }
        List<Thread> threads = new ArrayList<Thread>();

        // Create, start and wait for the threads to complete
        for (int i = 0; i < n_threads; i++) {
            threads.add(new PopulateThread());
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }

        // Check appropriate entries in index
        QueryBuilder q = new QueryBuilder();
        q.index("search").equalTo("success");
        QueryResult result = indexManager.query(q.build());

        List<DocumentRevision> docRevisions = Lists.newArrayList(result);
        List<String> docIds = new ArrayList<String>();
        for (DocumentRevision r : docRevisions) {
            docIds.add(r.getId());
        }

        Assert.assertEquals(matching_ids.size(), docIds.size());
        for (String id : matching_ids) {
            Assert.assertTrue(docIds.contains(id));
        }
    }

    private void assertNotIndexed(SQLDatabase database,
                               Index index,
                               String docId) throws SQLException {
        String table = String.format(IndexManager.TABLE_INDEX_NAME_FORMAT, index.getName());
        Cursor cursor = database.rawQuery("SELECT count(*) FROM " +
                        table + " where docid = ? ",
                new String[]{String.valueOf(docId)}
        );
        Assert.assertTrue(cursor.moveToFirst());
        Assert.assertEquals(Integer.valueOf(0), Integer.valueOf(cursor.getInt(0)));
    }

    private void assertIndexed(SQLDatabase database,
                       Index index,
                       String docId,
                       String ... expectedValues) throws SQLException {
        String table = String.format(IndexManager.TABLE_INDEX_NAME_FORMAT, index.getName());
        assertIndexedValueCount(database, docId, table, expectedValues);
        for(String value : expectedValues) {
            assertIndexedValue(database, docId, table, value);
        }
    }

    private void assertIndexedValue(SQLDatabase database, String docId, String table, String value) throws SQLException {
        Cursor cursor = database.rawQuery("SELECT count(*) FROM " +
                table + " where docid = ? and value = ? ",
                new String[]{String.valueOf(docId), value});
        Assert.assertTrue(cursor.moveToFirst());
        Assert.assertEquals(Integer.valueOf(1), Integer.valueOf(cursor.getInt(0)));
    }

    private void assertIndexedValueCount(SQLDatabase database, String docId, String table, String[] expectedValues) throws SQLException {
        Cursor cursor = database.rawQuery("SELECT count(*) FROM " +
                table + " where docid = ? ",
                new String[]{String.valueOf(docId)});
        Assert.assertTrue(cursor.moveToFirst());
        Assert.assertEquals(Integer.valueOf(expectedValues.length), Integer.valueOf(cursor.getInt(0)));
    }
}
