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

package com.cloudant.sync.replication;

import com.cloudant.mazha.DocumentRevs;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.DocumentBody;
import com.cloudant.sync.datastore.DocumentRevsList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Test GetRevisionTask.
 */

@RunWith(Parameterized.class)
public class GetRevisionTaskTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {false}, {true},
        });
    }

    @Parameterized.Parameter
    public boolean pullAttachmentsInline;

    @Test
    public void test_get_revision_task()
            throws Exception {
        CouchDB sourceDB = mock(CouchDB.class);

        String docId = "asdjfsdflkjsd";
        String revId = "10-asdfsafsadf";

        List<String> expected = Arrays.asList("1-a", "2-b", "3-a");
        DocumentRevs.Revisions revs = new DocumentRevs.Revisions();
        revs.setIds(expected);
        List<DocumentRevs> documentRevs = new ArrayList<DocumentRevs>();
        DocumentRevs dr = new DocumentRevs();
        dr.setRevisions(revs);
        documentRevs.add(dr);
        ArrayList<String> revIds = new ArrayList<String>();
        revIds.add(revId);
        ArrayList<String> attsSince = new ArrayList<String>();
        // bulk get returns a list of lists...
        List<DocumentRevsList> drll = new ArrayList<DocumentRevsList>();
        drll.add(new DocumentRevsList(documentRevs));

        List<BulkGetRequest> requests = new ArrayList<BulkGetRequest>();
        requests.add(new BulkGetRequest(docId, revIds, attsSince));

        // stubs
        when(sourceDB.bulkGetRevisions(requests, pullAttachmentsInline)).thenReturn(drll);

        // exec
        GetRevisionTask task = new GetRevisionTask(sourceDB, requests, pullAttachmentsInline);
        Iterable<DocumentRevsList> actualDocumentRevs = task.call();

        // verify
        verify(sourceDB).bulkGetRevisions(requests, pullAttachmentsInline);

        Assert.assertEquals(expected, actualDocumentRevs.iterator().next().get(0).getRevisions().getIds());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_exceptions_propagate()
        throws Exception {
        CouchDB sourceDB = mock(CouchDB.class);

        String docId = "asdjfsdflkjsd";
        String revId = "10-asdfsafsadf";
        ArrayList<String> revIds = new ArrayList<String>();
        revIds.add(revId);
        ArrayList<String> attsSince = new ArrayList<String>();

        List<BulkGetRequest> requests = new ArrayList<BulkGetRequest>();
        requests.add(new BulkGetRequest(docId, revIds, attsSince));

        // stubs
        when(sourceDB.bulkGetRevisions(requests, pullAttachmentsInline)).thenThrow(IllegalArgumentException.class);

        //exec
        GetRevisionTask task = new GetRevisionTask(sourceDB, requests, pullAttachmentsInline);
        task.call();
    }

    @Test(expected = NullPointerException.class)
    public void test_null_docId() {
        CouchDB sourceDB = mock(CouchDB.class);
        ArrayList<String> revIds = new ArrayList<String>();
        revIds.add("revId");
        ArrayList<String> attsSince = new ArrayList<String>();
        List<BulkGetRequest> requests = new ArrayList<BulkGetRequest>();
        requests.add(new BulkGetRequest(null, revIds, attsSince));
        new GetRevisionTask(sourceDB, requests, pullAttachmentsInline);
    }

    @Test(expected = NullPointerException.class)
    public void test_null_revId() {
        CouchDB sourceDB = mock(CouchDB.class);
        List<BulkGetRequest> requests = new ArrayList<BulkGetRequest>();
        requests.add(new BulkGetRequest("docId", null, null));
        new GetRevisionTask(sourceDB, requests, pullAttachmentsInline);
    }

    @Test(expected = NullPointerException.class)
    public void test_null_sourceDb() {
        ArrayList<String> revIds = new ArrayList<String>();
        revIds.add("revId");
        ArrayList<String> attsSince = new ArrayList<String>();
        List<BulkGetRequest> requests = new ArrayList<BulkGetRequest>();
        requests.add(new BulkGetRequest("docId", revIds, attsSince));
        new GetRevisionTask(null, requests, pullAttachmentsInline);
    }
}
