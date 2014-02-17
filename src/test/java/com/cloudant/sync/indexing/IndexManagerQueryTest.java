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

import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.DocumentBody;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.sqlite.sqlite4java.SQLiteWrapper;
import com.cloudant.sync.util.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.hasItems;

public class IndexManagerQueryTest {

    SQLiteWrapper database = null;
    DatastoreExtended datastore = null;
    IndexManager indexManager = null;
    List<DocumentBody> dbBodies = null;
    List<DocumentRevision> revisions = null;
    private String datastoreManagerPath;

    @Before
    public void setUp() throws IOException, SQLException, IndexExistsException {
        datastoreManagerPath = TestUtils.createTempTestingDir(this.getClass().getName());
        DatastoreManager datastoreManager = new DatastoreManager(this.datastoreManagerPath);
        datastore = (DatastoreExtended) datastoreManager.openDatastore(getClass().getSimpleName());
        indexManager = new IndexManager(datastore);
        database = (SQLiteWrapper) indexManager.getDatabase();

        dbBodies = new ArrayList<DocumentBody>();
        for (int i = 0; i < 8; i++) {
            dbBodies.add(TestUtils.createBDBody("fixture/index" + "_" + i + ".json"));
        }
        prepareDataForQueryTest();
        indexManager.updateAllIndexes();
    }

    @After
    public void tearDown() throws Exception {
        TestUtils.deleteDatabaseQuietly(database);
        TestUtils.deleteTempTestingDir(datastoreManagerPath);
    }

    @Test
    public void query_cjkShouldBeSupported() throws Exception {
        DocumentBody body = TestUtils.createBDBody("fixture/index_chinese_character.json");
        String albumName = (String) body.asMap().get("album");
        String artistName = (String) body.asMap().get("artist");
        datastore.createDocument(body);
        indexManager.updateAllIndexes();
        {
            QueryResult result = indexManager.query(new QueryBuilder().index("Album").equalTo(albumName).build());
            Assert.assertEquals(1, result.size());
        }
        {
            QueryResult result = indexManager.query(new QueryBuilder().index("Artist").equalTo(artistName).build());
            Assert.assertEquals(1, result.size());
        }
    }

    @Test
    public void query_specialCharacter() throws IOException {
        DocumentBody body = TestUtils.createBDBody("fixture/index_special_character.json");
        datastore.createDocument(body);
        indexManager.updateAllIndexes();
        {
            QueryResult result = indexManager.query(new QueryBuilder().index("Album").equalTo("\\\\//").build());
            Assert.assertEquals(1, result.size());
        }

        {
            QueryResult result = indexManager.query(new QueryBuilder().index("Artist").equalTo("Tom's nick name is \"tommy\"").build());
            Assert.assertEquals(1, result.size());
        }
    }

    @Test
    public void query_usingTooManyIndexes() {
        QueryBuilder qb = new QueryBuilder().index("A").equalTo("a")
                .index("B").equalTo("b")
                .index("C").equalTo("c")
                .index("D").equalTo("d")
                .index("E").equalTo("e")
                .index("F").equalTo("f");
        indexManager.query(qb.build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void query_usingIndexDoesNotExist_exception() {
        indexManager.query(new QueryBuilder().index("InvalidIndex").equalTo("someField").build());
    }

    @Test
    public void query_simpleStringIndex() throws IndexExistsException {
        QueryResult result = indexManager.query(new QueryBuilder().index("Album").equalTo("X&Y").build());
        Assert.assertEquals(3, result.size());
    }

    @Test
    public void query_simpleIntegerIndex() throws IndexExistsException {
        QueryResult result = indexManager.query(new QueryBuilder().index("Year").equalTo(2003l).build());
        Assert.assertEquals(1, result.size());
    }

    @Test
    public void query_RangeQueryIntegerIndex() throws IndexExistsException {
        QueryResult result = indexManager.query(
                new QueryBuilder().index("Year").greaterThan(2003l)
                        .index("Year").lessThan(2007l).build());
        Assert.assertEquals(3, result.size());
    }

    @Test
    public void query_RangeQueryWithMinValueIntegerIndex() throws IndexExistsException {
        QueryResult result = indexManager.query(new QueryBuilder().index("Year").greaterThan(2003l).build());
        Assert.assertEquals(4, result.size());
    }

    @Test
    public void query_RangeQueryWithMaxValueIntegerIndex() throws IndexExistsException {
        QueryResult result = indexManager.query(new QueryBuilder().index("Year").lessThan(2003l).build());
        Assert.assertEquals(2, result.size());
    }

    @Test
    public void query_SimpleQueryUsingTwoStringIndex() throws IndexExistsException {
        {
            QueryResult result = indexManager.query(new QueryBuilder()
                    .index("Album").equalTo("X&Y")
                    .index("Artist").equalTo("cold play").build());
            Assert.assertEquals(3, result.size());
        }
        {
            QueryResult result = indexManager.query(new QueryBuilder()
                    .index("Album").equalTo("A rush of blood to my head")
                    .index("Artist").equalTo("cold play").build());
            Assert.assertEquals(2, result.size());
        }
        {
            QueryResult result = indexManager.query(new QueryBuilder()
                    .index("Album").equalTo("Hall Of Fame")
                    .index("Artist").equalTo("cold play").build());
            Assert.assertEquals(0, result.size());
        }
    }

    @Test
    public void query_SimpleQueryUsingOneStringIndexAndOneIntegerIndex()
            throws IndexExistsException {
        QueryResult result = indexManager.query(new QueryBuilder()
                .index("Artist").equalTo("cold play")
                .index("Year").equalTo(2003l).build());
        Assert.assertEquals(1, result.size());
    }

    @Test
    public void query_SimpleQueryUsingStringAndRangeQueryUsingIntegerIndex()
            throws IndexExistsException {
        QueryResult result = indexManager.query(new QueryBuilder()
                .index("Artist").equalTo("cold play")
                .index("Year").greaterThan(2003l).build());
        Assert.assertEquals(2, result.size());
    }

    private void prepareDataForQueryTest() throws IndexExistsException {
        indexManager.ensureIndexed("Album", "album");
        indexManager.ensureIndexed("Artist", "artist");
        indexManager.ensureIndexed("Year", "year", IndexType.INTEGER);

        revisions = new ArrayList<DocumentRevision>();
        for (DocumentBody b : dbBodies) {
            revisions.add(datastore.createDocument(b));
        }

        indexManager.ensureIndexed("A", "a");
        indexManager.ensureIndexed("B", "b");
        indexManager.ensureIndexed("C", "c");
        indexManager.ensureIndexed("D", "d");
        indexManager.ensureIndexed("E", "e");
        indexManager.ensureIndexed("F", "f");
    }

    @Test(expected = IllegalArgumentException.class)
    public void query_user65Indexes_exception() {
        QueryBuilder qb = new QueryBuilder();
        for(int i = 0 ; i < IndexManager.JOINS_LIMIT_PER_QUERY + 1; i ++) {
            qb.index("Index" + i).equalTo(i);
        }
        indexManager.query(qb.build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void query_unsupportedValue() {
        QueryBuilder qb = new QueryBuilder();
        qb.index("Artist").equalTo(100);
        indexManager.query(qb.build());
    }

    @Test
    public void query_multiValueIndex() throws IndexExistsException {
        indexManager.ensureIndexed("Genre", "Genre");
        Index index = indexManager.getIndex("Genre");
        Assert.assertEquals(Long.valueOf(datastore.getLastSequence()),
                Long.valueOf(index.getLastSequence()));

        // To understand the test, check the content of json doc in:
        // fixture/index_*: document index_1/2/3/4/6/7 has "Genre" "Pop",
        // and document index_1/2/5 has "Genre" "Rock".

        {
            QueryBuilder qb = new QueryBuilder();
            qb.index("Genre").equalTo("Pop");
            QueryResult r = indexManager.query(qb.build());
            Assert.assertEquals(6L, r.size());
            Assert.assertThat(r.documentIds(), hasItems(
                    revisions.get(1).getId(),
                    revisions.get(2).getId(),
                    revisions.get(3).getId(),
                    revisions.get(4).getId(),
                    revisions.get(6).getId(),
                    revisions.get(7).getId()));
        }

        {
            QueryBuilder qb = new QueryBuilder();
            qb.index("Genre").equalTo("Rock");
            QueryResult r = indexManager.query(qb.build());
            Assert.assertEquals(3L, r.size());
            Assert.assertThat(r.documentIds(), hasItems(
                    revisions.get(1).getId(),
                    revisions.get(2).getId(),
                    revisions.get(5).getId()));
        }
    }

    @Test
    public void query_OrderBy() throws IndexExistsException {
        QueryResult result = indexManager.query(new QueryBuilder().index("Year").greaterThan(0l).sortBy("Year").build());
        Assert.assertEquals(7, result.size());
        int prevYear = 0;
        for(DocumentRevision doc  : result) {
            int year = (Integer)doc.getBody().asMap().get("year");
            Assert.assertTrue(year >= prevYear);
            prevYear = year;
        }
    }

    @Test
    public void query_OffsetLimit() throws IndexExistsException {
        QueryResult resultAll = indexManager.query(new QueryBuilder().index("Year").greaterThan(0l).sortBy("Year").build());
        QueryResult result = indexManager.query(new QueryBuilder().index("Year").greaterThan(0l).sortBy("Year").offset(2).limit(2).build());
        Assert.assertEquals(2, result.size());
        assert (resultAll.documentIds().toArray()[2].equals(result.documentIds().toArray()[0]));
    }

    @Test
    public void query_UniqueValues() throws IndexExistsException {
        List values = indexManager.uniqueValues("Album");
        Assert.assertEquals(3, values.size());
        Assert.assertThat(values, hasItems(
                "A rush of blood to my head",
                "Hall of fame",
                "X&Y"));
    }



    }
