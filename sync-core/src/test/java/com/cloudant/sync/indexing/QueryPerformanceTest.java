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

import com.cloudant.common.PerformanceTest;
import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.MutableDocumentRevision;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Category(PerformanceTest.class)
public class QueryPerformanceTest {

    SQLDatabase database = null;
    DatastoreExtended datastore = null;
    IndexManager indexManager = null;
    private String datastoreManagerPath;
    List<BasicDocumentRevision> revs = new ArrayList<BasicDocumentRevision>();

    @Before
    public void setUp() throws Exception {
        datastoreManagerPath = TestUtils.createTempTestingDir(this.getClass().getName());
        DatastoreManager datastoreManager = new DatastoreManager(this.datastoreManagerPath);
        datastore = (DatastoreExtended) datastoreManager.openDatastore(getClass().getSimpleName());
        indexManager = new IndexManager(datastore);

        indexManager.ensureIndexed("artist", "artist");
        indexManager.ensureIndexed("genre", "genre");

        prepareDataForQueryTest();
    }

    private void prepareDataForQueryTest() throws Exception {
        List<Map> data = new ArrayList<Map>();
        addMap(data, 10, "tom");
        addMap(data, 100, "jerry");
        addMap(data, 1000, "harry");
        addMap(data, 10000, "sam");
        Collections.shuffle(data);

        for(Map m : data) {
            MutableDocumentRevision rev = new MutableDocumentRevision();
            rev.body = (DocumentBodyFactory.create(m));
            revs.add(datastore.createDocumentFromRevision(rev));
        }
    }

    private void addMap(List<Map> data, int count, String name) {
        for(int i = 0 ; i < count ; i ++) {
            Map m = new HashMap();
            m.put("name", name);
            m.put("age", 30);
            data.add(m);
        }
    }

    @After
    public void tearDown() throws Exception {
        TestUtils.deleteDatabaseQuietly(database);
        TestUtils.deleteTempTestingDir(datastoreManagerPath);
    }

    @Test
    public void test_all() throws IndexExistsException {
        this.test_indexing();

        this.test_query("john", 0);
        this.test_query("tom", 10);
        this.test_query("jerry", 100);
        this.test_query("harry", 1000);
        this.test_query("sam", 10000);
    }

    public void test_indexing() throws IndexExistsException {
        long t0 = System.currentTimeMillis();
        indexManager.ensureIndexed("name", "name");
        long t1 = System.currentTimeMillis();
        System.out.println("Indexing Time: " + String.valueOf(t1 - t0));

    }

    public void test_query(String name, int expectCount) {
        System.out.println("---*---");
        System.out.println("Count: " + expectCount);

        int n = 100;
        long[] query_timings = new long[n];
        long[] walk_timings = new long[n];
        for (int i = 0; i < n; i++) {
            QueryBuilder qb = new QueryBuilder();
            qb.index("name").equalTo(name);
            long t0 = System.currentTimeMillis();
            QueryResult r = indexManager.query(qb.build());
            Assert.assertEquals(expectCount, r.size());
            long t1 = System.currentTimeMillis();
            query_timings[i] = t1 - t0;

            t0 = System.currentTimeMillis();
            this.test_queryResult(r);
            t1 = System.currentTimeMillis();
            walk_timings[i] = t1 - t0;
        }

        long total_q = 0, total_w = 0;
        for (int i = 0; i < n; i++) {
            total_q += query_timings[i];
            total_w += walk_timings[i];
        }
        long average_q = total_q / n;
        long average_w = total_w / n;

        System.out.println("Querying Time (average over " + n + " runs): " + String.valueOf(average_q));

        System.out.println("Walking Time (average over " + n + " runs): " + String.valueOf(average_w));

    }

    public void test_queryResult(QueryResult r) {
        for(BasicDocumentRevision revision : r) {
            // Does nothing
        }
    }

}
