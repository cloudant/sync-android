/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
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

package com.cloudant.sync.internal.documentstore;

import static org.hamcrest.CoreMatchers.hasItems;

import com.cloudant.sync.documentstore.Changes;
import com.cloudant.sync.documentstore.DocumentRevision;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DatabaseImplChangesTest extends BasicDatastoreTestBase {

    @Test
    public void changes_noChanges_nothing() throws Exception {
        Changes changes = datastore.changes(0, 100);
        Assert.assertEquals(0, changes.getResults().size());
        Assert.assertEquals(0, changes.getLastSequence());
    }

    @Test
    public void changes_sinceZero_twoDocumentsShouldBeReturned() throws Exception {
        createTwoDocuments();

        Changes changes = datastore.changes(0, 100);
        Assert.assertEquals(2, changes.getResults().size());
    }

    @Test
    public void changes_sinceZeroThenSinceTwo_lastSequenceOfEmptyChangeSetMightNotBeZero() throws Exception {
        createTwoDocuments();

        Changes changes = datastore.changes(0, 100);
        Assert.assertEquals(2, changes.getResults().size());
        Assert.assertEquals(2, changes.getLastSequence());

        // this is the real test, the last sequence of empty changes should NOT be zero!
        Changes changes2 = datastore.changes(2, 100);
        Assert.assertEquals(0, changes2.getResults().size());
        Assert.assertEquals(2, changes2.getLastSequence());
    }

    @Test
    public void changes_sinceMinusOno_twoDocumentsShouldBeReturned() throws Exception {
        createTwoDocuments();
        Changes changes = datastore.changes(-1, 100);
        Assert.assertEquals(2, changes.getResults().size());
        Assert.assertEquals(2, changes.getLastSequence());
    }

    @Test(expected = IllegalArgumentException.class)
    public void changes_limitMinusOne_exception() throws Exception {
        createTwoDocuments();
        datastore.changes(0, -1);
    }

    @Test
    public void changes_sinceOne_oneDocumentsShouldBeReturned() throws Exception{
        createThreeDocuments();

        Changes changes = datastore.changes(2, 100);
        Assert.assertEquals(1, changes.getResults().size());
        Assert.assertEquals(4, changes.getLastSequence());
    }

    @Test
    public void changes_sinceLimitByTwo_correctResultAreReturned() throws Exception {
        DocumentRevision[] docs = createThreeDocuments();
        {
            Changes changes = datastore.changes(0, 2);
            Assert.assertEquals(2, changes.getResults().size());
            Assert.assertEquals(2, changes.getLastSequence());
            Assert.assertThat(resultsToIDs(changes), hasItems(docs[0].getId(), docs[1].getId()));
        }

        {
            Changes changes = datastore.changes(2, 2);
            Assert.assertEquals(1, changes.getResults().size());
            Assert.assertThat(resultsToIDs(changes), hasItems(docs[2].getId()));
            Assert.assertEquals(4, changes.getLastSequence());
        }
    }

    @Test
    public void changes_limitByTenByThereAreOnlyFourChanges_lastSequenceIsFour() throws Exception {
        DocumentRevision[] docs = createThreeDocuments();
        Changes changes = datastore.changes(0, 10);
        Assert.assertEquals(3, changes.getResults().size());
        Assert.assertThat(resultsToIDs(changes), hasItems(docs[0].getId(), docs[1].getId(), docs[2].getId()));
        Assert.assertEquals(4, changes.getLastSequence());
    }
    
    private List<String> resultsToIDs(Changes changes) {
        List<String> changeIDs = new ArrayList<String>();
        for (DocumentRevision rev : changes.getResults()) {
            changeIDs.add(rev.getId());
        }
        return changeIDs;
    }
}
