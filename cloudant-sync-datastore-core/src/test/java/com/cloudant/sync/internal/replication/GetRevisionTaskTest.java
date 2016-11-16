/*
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.internal.replication;

import com.cloudant.sync.internal.mazha.DocumentRevs;
import com.cloudant.sync.internal.documentstore.DocumentRevsList;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
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

        List<BulkGetRequest> requests = new ArrayList<BulkGetRequest>();
        requests.add(new BulkGetRequest(docId, revIds, attsSince));

        // stubs
        when(sourceDB.getRevisions(docId, revIds, attsSince, pullAttachmentsInline)).thenReturn(documentRevs);

        // exec
        Iterable<DocumentRevsList> actualDocumentRevs = new GetRevisionTaskThreaded(sourceDB,
                requests, pullAttachmentsInline);

        // pulling out of the iterator will ensure the executed tasks are put onto the result queue
        Assert.assertEquals(expected, actualDocumentRevs.iterator().next().get(0).getRevisions()
                .getIds());

        // verify
        verify(sourceDB).getRevisions(docId, revIds, attsSince, pullAttachmentsInline);
    }

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
        when(sourceDB.getRevisions(docId, revIds, attsSince, pullAttachmentsInline)).thenThrow(IllegalArgumentException.class);

        //exec
        try {
            Iterable<DocumentRevsList> actualDocumentRevs = new GetRevisionTaskThreaded(sourceDB, requests, pullAttachmentsInline);

            // pull all the results from the iterator
            for (DocumentRevsList revs : actualDocumentRevs) {
            }
            Assert.fail("Expected exception to be thrown");
        } catch (Exception e){
            // although our stub threw an IllegalArgumentException, we expect next() to have wrapped
            // this up in a RuntimeException.
            Assert.assertTrue(e.getClass().equals(RuntimeException.class));
            Assert.assertTrue(e.getCause().getClass().equals(IllegalArgumentException.class));
        }

    }

    @Test(expected = NullPointerException.class)
    public void test_null_docId() {
        CouchDB sourceDB = mock(CouchDB.class);
        ArrayList<String> revIds = new ArrayList<String>();
        revIds.add("revId");
        ArrayList<String> attsSince = new ArrayList<String>();
        List<BulkGetRequest> requests = new ArrayList<BulkGetRequest>();
        requests.add(new BulkGetRequest(null, revIds, attsSince));
        new GetRevisionTaskThreaded(sourceDB, requests, pullAttachmentsInline);
    }

    @Test(expected = NullPointerException.class)
    public void test_null_revId() {
        CouchDB sourceDB = mock(CouchDB.class);
        List<BulkGetRequest> requests = new ArrayList<BulkGetRequest>();
        requests.add(new BulkGetRequest("docId", null, null));
        new GetRevisionTaskThreaded(sourceDB, requests, pullAttachmentsInline);
    }

    @Test(expected = NullPointerException.class)
    public void test_null_sourceDb() {
        ArrayList<String> revIds = new ArrayList<String>();
        revIds.add("revId");
        ArrayList<String> attsSince = new ArrayList<String>();
        List<BulkGetRequest> requests = new ArrayList<BulkGetRequest>();
        requests.add(new BulkGetRequest("docId", revIds, attsSince));
        new GetRevisionTaskThreaded(null, requests, pullAttachmentsInline);
    }
}
