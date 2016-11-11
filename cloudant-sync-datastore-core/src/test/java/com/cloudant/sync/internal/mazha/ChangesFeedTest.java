/**
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

package com.cloudant.sync.internal.mazha;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.cloudant.common.RequireRunningCouchDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.core.Is.is;

@Category(RequireRunningCouchDB.class)
public class ChangesFeedTest extends CouchClientTestBase {

    @Test
    public void changes_dbChangesMustSuccessfullyReturn() {
        Response res1 = ClientTestUtils.createHelloWorldDoc(client);
        Response res2 = ClientTestUtils.createHelloWorldDoc(client);
        Object lastSeq = "0";

        { // Two docs changed
            ChangesResult changes = client.changes(lastSeq);
            Map<String, List<String>> changedRevIds = findChangedRevisionIds(changes);

            Assert.assertThat(changedRevIds.size(), is(equalTo(2)));
            Assert.assertThat(changedRevIds.keySet(), hasItems(res1.getId(), res2.getId()));
            Assert.assertThat(changedRevIds.get(res1.getId()), hasItem(res1.getRev()));
            Assert.assertThat(changedRevIds.get(res2.getId()), hasItem(res2.getRev()));

            lastSeq = changes.getLastSeq();
        }

        Response res3 = ClientTestUtils.createHelloWorldDoc(client);

        { // One doc changed
            ChangesResult changes = client.changes(lastSeq);
            Map<String, List<String>> changedRevIds = findChangedRevisionIds(changes);

            Assert.assertThat(changedRevIds.size(), is(equalTo(1)));
            Assert.assertThat(changedRevIds.keySet(), hasItem(res3.getId()));
            Assert.assertThat(changedRevIds.get(res3.getId()), hasItem(res3.getRev()));

            lastSeq = changes.getLastSeq();
        }

        { // No changes
            ChangesResult changes = client.changes(lastSeq);
            Assert.assertTrue(changes.size() == 0);
        }
    }

    public Map<String, List<String>> findChangedRevisionIds(ChangesResult changesResult) {
        Map<String, List<String>> res = new HashMap<String, List<String>>();
        for(ChangesResult.Row row: changesResult.getResults()) {
            List<String> revIds = new ArrayList<String>();
            for(ChangesResult.Row.Rev rev : row.getChanges()) {
                revIds.add(rev.getRev());
            }
            res.put(row.getId(), revIds);
        }
        return res;
    }

    @Test(expected = NoResourceException.class)
    public void changes_dbNotExist_exception() {
        client.deleteDb();
        client.changes("1");
    }

    @Test
    public void changes_docWithConflicts_conflictsShouldBeReturned() {
        // Not sure how to changed conflicts yet
    }

    @Test
    public void changes_dbWithConflicts_changesMustSuccessfullyReturn() {
        ClientTestUtils.createHelloWorldDoc(client);
        Object lastSeq = "0";

        {
            ChangesResult changes = client.changes(lastSeq);
            Assert.assertEquals(1, changes.size());
            lastSeq = changes.getLastSeq();
        }

        Response res1 = ClientTestUtils.createHelloWorldDoc(client);
        String[] openRevs = ClientTestUtils.createDocumentWithConflicts(client, res1);
        Response res2 = ClientTestUtils.createHelloWorldDoc(client);

        {
            ChangesResult changes = client.changes(lastSeq);
            Assert.assertEquals(2, changes.size());

            List<String> doc1ChangedRevs = findChangesRevs(changes, res1.getId());
            Assert.assertEquals(3, doc1ChangedRevs.size());
            Assert.assertThat(doc1ChangedRevs, hasItems(openRevs));

            List<String> doc2ChangedRevs = findChangesRevs(changes, res2.getId());
            Assert.assertEquals(1, doc2ChangedRevs.size());
            Assert.assertThat(doc2ChangedRevs, hasItem(res2.getRev()));
        }
    }

    public List<String> findChangesRevs(ChangesResult changes, String id) {
        List<String> changedRevs = new ArrayList<String>();
        for(ChangesResult.Row row : changes.getResults()) {
            if(row.getId().equals(id)) {
                for(ChangesResult.Row.Rev rev: row.getChanges()) {
                    changedRevs.add(rev.getRev());
                }
            }
        }
        return changedRevs;
    }
}
