//  Copyright (c) 2016 IBM Cloudant. All rights reserved.
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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.cloudant.sync.datastore.encryption.NullKeyProvider;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLDatabaseQueue;
import com.cloudant.sync.sqlite.SQLQueueCallable;
import com.cloudant.sync.util.SQLDatabaseTestUtils;
import com.cloudant.sync.util.TestUtils;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class QueryResultTest extends AbstractQueryTestBase {

    SQLDatabaseQueue queue;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        fd = new ForwardingDatastore(ds);
        assertThat(fd, is(notNullValue()));
        final String[] metadataTableList = new String[]{QueryConstants.INDEX_METADATA_TABLE_NAME};
        dbq.submit(new SQLQueueCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                SQLDatabaseTestUtils.assertTablesExist(db, metadataTableList);
                return null;
            }
        }).get();

        queue = new SQLDatabaseQueue(factoryPath + "/" + factory.listAllDatastores().get(0) +
            "/db.sync", new NullKeyProvider());

        setUpBasicQueryData();
    }

    /*
     * Perform a simple query then drop the revs table from the database before attempting
     * to get the document ids from the QueryResult.
     */
    @Test(expected = QueryException.class)
    public void testQueryGetDocumentsWithIdsFails() throws Exception,
        ExecutionException {
        List<Object> fields = Collections.<Object>singletonList("pet");
        assertThat(fd.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "cat" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "cat");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);

        queue.submit(new SQLQueueCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                db.execSQL("DROP TABLE IF EXISTS revs");
                return null;
            }
        }).get();

        // Attempt to retrieve the document ids. This should fail with an
        // UncheckedDocumentException because the revs table has been dropped.
        queryResult.documentIds();
    }
}
