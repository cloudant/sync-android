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
import com.cloudant.sync.datastore.DocumentRevsList;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RunWith(JUnit4.class)

/**
 * Test GetRevisionTask.
 */
public class GetRevisionTaskTest {

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

        // stubs
        when(sourceDB.getRevisions(docId, revId)).thenReturn(documentRevs);

        // exec
        GetRevisionTask task = new GetRevisionTask(sourceDB, docId, revId);
        DocumentRevsList actualDocumentRevs = task.call();

        // verify
        verify(sourceDB).getRevisions(docId, revId);

        Assert.assertEquals(expected, actualDocumentRevs.get(0).getRevisions().getIds());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_exceptions_propagate()
        throws Exception {
        CouchDB sourceDB = mock(CouchDB.class);

        String docId = "asdjfsdflkjsd";
        String revId = "10-asdfsafsadf";

        // stubs
        when(sourceDB.getRevisions(docId, revId)).thenThrow(IllegalArgumentException.class);

        //exec
        GetRevisionTask task = new GetRevisionTask(sourceDB, docId, revId);
        task.call();
    }

    @Test(expected = NullPointerException.class)
    public void test_null_docId() {
        CouchDB sourceDB = mock(CouchDB.class);
        new GetRevisionTask(sourceDB, null, "revId");
    }

    @Test(expected = NullPointerException.class)
    public void test_null_revId() {
        CouchDB sourceDB = mock(CouchDB.class);
        // The cast is to get rid of a compiler warning
        new GetRevisionTask(sourceDB, "devId", (String[])null);
    }

    @Test(expected = NullPointerException.class)
    public void test_null_sourceDb() {
        new GetRevisionTask(null, "docId", "revId");
    }
}
