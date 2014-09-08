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

package com.cloudant.mazha;

import com.cloudant.common.CouchConstants;
import com.cloudant.common.CouchUtils;
import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.mazha.json.JSONHelper;
import com.cloudant.sync.util.TestUtils;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.hasItem;

@Category(RequireRunningCouchDB.class)
public class BulkAPITest extends CouchClientTestBase {

    @Test
    public void bulk_success() {
        Response res1 = ClientTestUtils.createHelloWorldDoc(client);
        Response res2 = ClientTestUtils.createHelloWorldDoc(client);

        Map<String, Object> doc1 = client.getDocument(res1.getId());
        Map<String, Object> doc2 = client.getDocument(res2.getId());

        doc1.put(CouchConstants._rev, CouchUtils.generateNextRevisionId((String) doc1.get(CouchConstants._rev)));
        doc1.put("Here", "Is Cloudant!");

        doc2.put(CouchConstants._rev, CouchUtils.generateNextRevisionId((String) doc2.get(CouchConstants._rev)));
        doc2.put("Here", "Is Beijing!");

        List<Response> responses = client.bulk(doc1, doc2);

        // When include "new_edits=false" option, server will not include entries for any of the successful revisions
        // (since their rev IDs are already known to the sender)
        // http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API#Modify_Multiple_Documents_With_a_Single_Request
        Assert.assertEquals(0, responses.size());

        Map<String, Object> doc1Updated = client.getDocument(res1.getId());
        Assert.assertTrue(((String) doc1Updated.get(CouchConstants._rev)).startsWith("2-"));

        Map<String, Object> doc2Updated = client.getDocument(res2.getId());
        Assert.assertTrue(((String) doc2Updated.get(CouchConstants._rev)).startsWith("2-"));
    }

    @Test
    public void bulk_withRevisionHistory_success() {
        Response res1 = ClientTestUtils.createHelloWorldDoc(client);
        Map<String, Object> doc1 = client.getDocument(res1.getId());

        String revision1 = res1.getRev();
        String revision2 = CouchUtils.generateNextRevisionId(revision1);
        String revision3 = CouchUtils.generateNextRevisionId(revision2);

        Map<String, Object> _revisions = ClientTestUtils.getRevisionHistory(revision3, revision2, revision1);
        doc1.put(CouchConstants._rev, revision3);
        doc1.put(CouchConstants._revisions, _revisions);

        List<Response> responses = client.bulk(doc1);
        Assert.assertEquals(0, responses.size());

        Map<String, Object> allRevs = client.getDocRevisions(res1.getId(), revision3, JSONHelper.STRING_MAP_TYPE_DEF);

        int revisionStart = (Integer) ((Map<String, Object>) allRevs.get(CouchConstants._revisions)).get
                (CouchConstants.start);
        Assert.assertEquals(3, revisionStart);

        List<String> allRevisionHashes = (List<String>) ((Map<String, Object>) allRevs.get(CouchConstants._revisions)
        ).get(CouchConstants.ids);

        Assert.assertEquals(allRevisionHashes.get(0), CouchUtils.getRevisionIdSuffix(revision3));
        Assert.assertEquals(allRevisionHashes.get(1), CouchUtils.getRevisionIdSuffix(revision2));
        Assert.assertEquals(allRevisionHashes.get(2), CouchUtils.getRevisionIdSuffix(revision1));
    }

    List<String> createRevisionList(Response res, int num) {
        List<String> revs = new ArrayList<String>();
        String currentRev = res.getRev();
        revs.add(currentRev);
        for (int i = 0; i < num; i++) {
            currentRev = CouchUtils.generateNextRevisionId(currentRev);
            revs.add(currentRev);
        }
        // it is important the list is in reverse order
        Collections.reverse(revs);
        return revs;
    }

    @Test
    public void bulk_twoDivergedUpdatesForSameDocumentUsedSeparately_conflictsMustBeCreated() {

        Response res = ClientTestUtils.createHelloWorldDoc(client);
        String finalRev = null;
        String expectedConflictRev = null;

        {
            List<String> revs = createRevisionList(res, 2);
            Map<String, Object> _revisions = ClientTestUtils.getRevisionHistory(revs.toArray(new String[]{}));

            Map<String, Object> doc = client.getDocument(res.getId());
            doc.put(CouchConstants._rev, revs.get(0));
            doc.put(CouchConstants._revisions, _revisions);
            doc.put("name", "tom");

            List<Response> responses = client.bulk(doc);
            Assert.assertEquals(0, responses.size());
            finalRev = revs.get(0);
        }

        {
            List<String> revs = createRevisionList(res, 1);
            Map<String, Object> _revisions = ClientTestUtils.getRevisionHistory(revs.toArray(new String[]{}));
            Map<String, Object> doc1 = client.getDocument(res.getId());
            doc1.put(CouchConstants._rev, revs.get(0));
            doc1.put(CouchConstants._revisions, _revisions);
            doc1.put("name", "jerry");

            List<Response> responses = client.bulk(doc1);
            Assert.assertEquals(0, responses.size());
            expectedConflictRev = revs.get(0);
        }

        Map<String, Object> docWithConflicts = client.getDocConflictRevs(res.getId());

        List<String> conflictsRevisions = (List<String>) docWithConflicts.get(CouchConstants._conflicts);
        Assert.assertEquals(1, conflictsRevisions.size());
        Assert.assertThat(conflictsRevisions, hasItem(expectedConflictRev));

        Assert.assertEquals(res.getId(), docWithConflicts.get(CouchConstants._id));
        Assert.assertEquals(finalRev, docWithConflicts.get(CouchConstants._rev));
        Assert.assertEquals("tom", docWithConflicts.get("name"));
    }

    @Test
    public void bulk_threeDivergedUpdatesForSameDocument_conflictsMustBeCreated() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        String[] openRevs = ClientTestUtils.createDocumentWithConflicts(client, res);

        Map<String, Object> docWithConflicts = client.getDocConflictRevs(res.getId());

        List<String> conflictsRevisions = (List<String>) docWithConflicts.get(CouchConstants._conflicts);
        Assert.assertEquals(2, conflictsRevisions.size());
        Assert.assertThat(conflictsRevisions, hasItem(openRevs[1]));
        Assert.assertThat(conflictsRevisions, hasItem(openRevs[2]));
    }

    @Test
    public void bulkSerializedDoc_oneDocWithOneRev_success() throws IOException {
        String jsonData = FileUtils.readFileToString(TestUtils.loadFixture("fixture/bulk_docs_1.json"));
        List<Response> responseList = client.bulkSerializedDocs(jsonData);

        Assert.assertEquals(0, responseList.size());
        Map<String, Object> doc = client.getDocument("1");
        Assert.assertEquals("1", doc.get("_id"));
        Assert.assertEquals("1-1a", doc.get("_rev"));
        Assert.assertEquals("Tom", doc.get("name"));
        Assert.assertEquals(30, doc.get("age"));
    }

    @Test
    public void bulkSerializedDoc_oneDocWithThreeRev_success() throws IOException {
        String jsonData = FileUtils.readFileToString(TestUtils.loadFixture("fixture/bulk_docs_2.json"));
        List<Response> responseList = client.bulkSerializedDocs(jsonData);

        Assert.assertEquals(0, responseList.size());
        Map<String, Object> doc = client.getDocument("2");
        Assert.assertEquals("2", doc.get("_id"));
        Assert.assertEquals("3-2c", doc.get("_rev"));
        Assert.assertEquals("Jerry", doc.get("name"));
        Assert.assertEquals(22, doc.get("age"));
    }

    @Test
    public void bulkSerializedDoc_twoDocs_success() throws IOException {
        String jsonData1 = FileUtils.readFileToString(TestUtils.loadFixture("fixture/bulk_docs_1.json"));
        String jsonData2 = FileUtils.readFileToString(TestUtils.loadFixture("fixture/bulk_docs_2.json"));
        List<Response> responseList = client.bulkSerializedDocs(jsonData1, jsonData2);

        Assert.assertEquals(0, responseList.size());

        {
            Map<String, Object> doc = client.getDocument("1");
            Assert.assertEquals("1", doc.get("_id"));
            Assert.assertEquals("1-1a", doc.get("_rev"));
            Assert.assertEquals("Tom", doc.get("name"));
            Assert.assertEquals(30, doc.get("age"));
        }

        {
            Map<String, Object> doc = client.getDocument("2");
            Assert.assertEquals("2", doc.get("_id"));
            Assert.assertEquals("3-2c", doc.get("_rev"));
            Assert.assertEquals("Jerry", doc.get("name"));
            Assert.assertEquals(22, doc.get("age"));
        }
    }

    @Test
    public void bulkSerializedDoc_twoDocsWithSameId_conflictsCreated() throws IOException {
        String jsonData1 = FileUtils.readFileToString(TestUtils.loadFixture("fixture/bulk_docs_2.json"));
        String jsonData2 = FileUtils.readFileToString(TestUtils.loadFixture("fixture/bulk_docs_3.json"));
        List<Response> responseList = client.bulkSerializedDocs(jsonData1, jsonData2);

        Assert.assertEquals(0, responseList.size());
        Map<String, Object> doc = client.getDocument("2");
        Assert.assertEquals("2", doc.get("_id"));
        Assert.assertEquals("3-2d", doc.get("_rev"));
        Assert.assertEquals("David", doc.get("name"));
        Assert.assertEquals(50, doc.get("age"));

        Map<String, Object> docWithConflicts = client.getDocConflictRevs("2");
        Assert.assertThat((List<String>) docWithConflicts.get(CouchConstants._conflicts), hasItem("3-2c"));
    }

    @Test
    public void bulkSerializedDoc_twoDocsWithSameIdDifferentTrees_conflictsCreated() throws IOException {
        String jsonData1 = FileUtils.readFileToString(TestUtils.loadFixture("fixture/bulk_docs_3.json"));
        String jsonData2 = FileUtils.readFileToString(TestUtils.loadFixture("fixture/bulk_docs_4.json"));
        List<Response> responseList = client.bulkSerializedDocs(jsonData1, jsonData2);

        Assert.assertEquals(0, responseList.size());
        Map<String, Object> doc = client.getDocument("2");
        Assert.assertEquals("2", doc.get("_id"));
        Assert.assertEquals("3-2z", doc.get("_rev"));
        Assert.assertEquals("Craig", doc.get("name"));
        Assert.assertEquals(90, doc.get("age"));

        Map<String, Object> docWithConflicts = client.getDocConflictRevs("2");
        Assert.assertThat((List<String>) docWithConflicts.get(CouchConstants._conflicts), hasItem("3-2d"));
    }

    @Test
    public void bulkSerializedDoc_revDoesNotMathRevHistory_seemsLikeRevIsIgnored() throws IOException {
        String jsonData1 = FileUtils.readFileToString(TestUtils.loadFixture("fixture/bulk_docs_rev_not_match.json"));
        List<Response> responseList = client.bulkSerializedDocs(jsonData1);
        Assert.assertEquals(0, responseList.size());
        Map<String, Object> doc = client.getDocument("2");
        Assert.assertEquals("2", doc.get("_id"));
        Assert.assertEquals("3-2z", doc.get("_rev"));
        Assert.assertEquals("Hanson", doc.get("name"));
        Assert.assertEquals(91, doc.get("age"));
    }

    @Test(expected = CouchException.class)
    public void bulkSerializedDoc_badJson_exception() throws IOException {
            String jsonData1 = FileUtils.readFileToString(TestUtils.loadFixture("fixture/bulk_docs_bad_json.json"));
            client.bulkSerializedDocs(jsonData1);
    }

    @Test
    public void bulkSerializedDoc_badJsonAndGoodJson_exception() throws IOException {
        // All or nothing? Good!
        try {
            String jsonData1 = FileUtils.readFileToString(TestUtils.loadFixture("fixture/bulk_docs_1.json"));
            String jsonData2 = FileUtils.readFileToString(TestUtils.loadFixture("fixture/bulk_docs_bad_json.json"));
            List<Response> responseList = client.bulkSerializedDocs(jsonData1, jsonData2);
        } catch (CouchException e) {
        }

        try {
            Map<String, Object> doc = client.getDocument("1");
        } catch (NoResourceException e) {
        }
    }


    public List<String> findChangesRevs(ChangesResult changes, String id) {
        List<String> changedRevs = new ArrayList<String>();
        for (ChangesResult.Row row : changes.getResults()) {
            if (row.getId().equals(id)) {
                for (ChangesResult.Row.Rev rev : row.getChanges()) {
                    changedRevs.add(rev.getRev());
                }
            }
        }
        return changedRevs;
    }
}
