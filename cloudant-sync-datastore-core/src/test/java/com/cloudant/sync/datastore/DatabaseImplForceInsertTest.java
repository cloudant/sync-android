/**
 * Copyright (C) 2013, 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.datastore;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;

import com.cloudant.sync.datastore.encryption.NullKeyProvider;
import com.cloudant.sync.event.Subscribe;
import com.cloudant.sync.notifications.DocumentModified;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.CouchUtils;
import com.cloudant.sync.util.JSONUtils;
import com.cloudant.sync.util.TestUtils;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

public class DatabaseImplForceInsertTest {

    public static final String OBJECT_ID = "object_id";
    String database_dir ;
    String documentOneFile = "fixture/document_1.json";
    String documentTwoFile = "fixture/document_2.json";

    SQLDatabase database = null;
    DatabaseImpl datastore = null;
    byte[] jsonData = null;
    DocumentBody bodyOne = null;
    DocumentBody bodyTwo = null;

    @Before
    public void setUp() throws Exception {
        database_dir = TestUtils.createTempTestingDir(DatabaseImplForceInsertTest.class.getName());
        datastore = new DatabaseImpl(new File(database_dir, "test"), new NullKeyProvider());

        jsonData = FileUtils.readFileToByteArray(TestUtils.loadFixture(documentOneFile));
        bodyOne = new DocumentBodyImpl(jsonData);

        jsonData = FileUtils.readFileToByteArray(TestUtils.loadFixture(documentTwoFile));
        bodyTwo = new DocumentBodyImpl(jsonData);
    }

    @After
    public void tearDown() throws Exception {
        datastore.close();
        TestUtils.deleteDatabaseQuietly(database);
        TestUtils.deleteTempTestingDir(database_dir);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forceInsert_revHistoryNotInRightOrder_exception() throws Exception {
        DocumentRevision rev = createDbObject();
        datastore.forceInsert(rev, "1-rev", "3-rev", "2-rev", "4-rev");
    }

    @Test(expected = IllegalArgumentException.class)
    public void forceInsert_currentRevisionNotInTheHistory_exception() throws Exception {
        DocumentRevision rev = createDbObject();
        datastore.forceInsert(rev, "1-rev", "2-rev", "3-rev");
    }

    @Test
    public void forceInsert_documentNotInLocalDB_documentShouldBeInserted() throws Exception {
        DocumentRevision rev = createDbObject();
        datastore.forceInsert(rev, "1-rev", "2-rev", "4-rev");
        assertDBObjectIsCorrect(OBJECT_ID, 4, bodyOne);
    }

    private DocumentRevision createDbObject() {
        return createDbObject("4-rev", bodyOne);
    }

    private DocumentRevision createDbObject(String rev, DocumentBody body) {
        DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
        builder.setDocId(OBJECT_ID);
        builder.setRevId(rev);
        builder.setDeleted(false);
        builder.setBody(body);
        return builder.build();
    }

    private DocumentRevision createDbObjectDeleted(String rev) {
        DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
        builder.setDocId(OBJECT_ID);
        builder.setRevId(rev);
        builder.setDeleted(true);
        builder.setBody(new DocumentBodyImpl(JSONUtils.emptyJSONObjectAsBytes()));
        return builder.build();
    }

    @Test
    public void forceInsert_newRevisionsFromRemoteDB_newRevisionShouldBeInserted() throws Exception {
        {
            DocumentRevision rev = createDbObject();
            datastore.forceInsert(rev, "1-rev", "2-rev", "4-rev");
        }

        assertDBObjectIsCorrect(OBJECT_ID, 4, bodyOne);

        {
            DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
            builder.setDocId(OBJECT_ID);
            builder.setRevId("5-rev");
            builder.setDeleted(false);
            builder.setBody(bodyOne);
            DocumentRevision newRev = builder.build();

            datastore.forceInsert(newRev, "1-rev", "2-rev", "4-rev", "5-rev");
        }

        assertDBObjectIsCorrect(OBJECT_ID, 5, bodyOne);
    }

    @Test
    public void forceInsert_longerPathFromRemoteDB_remoteDBWins() throws Exception {

        {
            DocumentRevision rev = createDbObject();
            datastore.forceInsert(rev, "1-rev", "2-rev", "4-rev");

            DocumentRevision insertedObj = datastore.getDocument(OBJECT_ID);
            insertedObj.setBody(bodyTwo);
            DocumentRevision updatedObj = datastore.updateDocumentFromRevision(insertedObj);

            Assert.assertNotNull(updatedObj);

            assertDBObjectIsCorrect(OBJECT_ID, 5, bodyTwo);
        }

        {
            DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
            builder.setDocId(OBJECT_ID);
            builder.setRevId("6-rev");
            builder.setDeleted(false);
            builder.setBody(bodyOne);
            DocumentRevision newRev = builder.build();

            datastore.forceInsert(newRev, "1-rev", "2-rev", "4-rev", "5-rev", "6-rev");
        }

        assertDBObjectIsCorrect(OBJECT_ID, 6, bodyOne);

    }

    @Test
    public void forceInsert_longerPathFromLocalDB_localDBWins() throws Exception {
        {
            DocumentRevision rev = createDbObject();
            datastore.forceInsert(rev, "1-rev", "2-rev", "4-rev");

            DocumentRevision insertedObj = datastore.getDocument(OBJECT_ID);
            insertedObj.setBody(bodyTwo);
            DocumentRevision updateObj = datastore.updateDocumentFromRevision(insertedObj);
            insertedObj.setBody(bodyTwo);
            DocumentRevision updateObj2 = datastore.updateDocumentFromRevision(updateObj);

            Assert.assertNotNull(updateObj2);

            assertDBObjectIsCorrect(OBJECT_ID, 6, bodyTwo);
        }

        {
            DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
            builder.setDocId(OBJECT_ID);
            builder.setRevId("5-rev");
            builder.setDeleted(false);
            builder.setBody(bodyOne);
            DocumentRevision newRev = builder.build();

            datastore.forceInsert(newRev, "1-rev", "2-rev", "4-rev", "5-rev");
        }

        assertDBObjectIsCorrect(OBJECT_ID, 6, bodyTwo);

        DocumentRevision p = datastore.getDocument(OBJECT_ID, "5-rev");
        Assert.assertNotNull(p);
        Assert.assertEquals("5-rev", p.getRevision());
        Assert.assertTrue(Arrays.equals(bodyOne.asBytes(), p.getBody().asBytes()));
    }

    @Test
    public void forceInsert_sameLengthOfPath_remoteRevisionWins() throws Exception {
        {
            DocumentRevision rev = createDbObject();
            datastore.forceInsert(rev, "1-rev", "2-rev", "3-rev", "4-rev");

            DocumentRevision insertedObj = datastore.getDocument(OBJECT_ID);
            insertedObj.setBody(bodyTwo);
            DocumentRevision updateObj = datastore.updateDocumentFromRevision(insertedObj);
            insertedObj.setBody(bodyTwo);
            DocumentRevision updateObj2 = datastore.updateDocumentFromRevision(updateObj);

            Assert.assertNotNull(updateObj2);

            assertDBObjectIsCorrect(OBJECT_ID, 6, bodyTwo);
        }

        String localRevisionId6 = null;
        String remoteRevisionId6 = null;
        {
            DocumentRevisionTree tree = datastore.getAllRevisionsOfDocument(OBJECT_ID);
            DocumentRevision current = tree.getCurrentRevision();
            List<DocumentRevision> all = tree.getPathForNode(current.getSequence());
            // Make sure the latest revision from remote db has bigger String (in terms of String comparison)
            for(DocumentRevision a : all) {
                int g = CouchUtils.generationFromRevId(a.getRevision());
                if(g == 6) {
                    localRevisionId6 = a.getRevision();
                    StringBuilder sb = new StringBuilder(localRevisionId6);
                    sb.setCharAt(2, (char)(sb.charAt(2) + 1));
                    remoteRevisionId6 = sb.toString();
                }
            }

            Assert.assertNotNull(localRevisionId6);
            Assert.assertNotNull(remoteRevisionId6);

            DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
            builder.setDocId(OBJECT_ID);
            builder.setRevId(remoteRevisionId6);
            builder.setDeleted(false);
            builder.setBody(bodyOne);
            DocumentRevision newRev = builder.build();

            datastore.forceInsert(newRev, "1-rev", "2-rev", "3-rev", "4-rev", "5-rev", remoteRevisionId6);
        }

        DocumentRevision obj = datastore.getDocument(OBJECT_ID);
        Assert.assertEquals(remoteRevisionId6, obj.getRevision());
        Assert.assertTrue(Arrays.equals(bodyOne.asBytes(), obj.getBody().asBytes()));
    }

    @Test(expected = DocumentException.class)
    public void forceInsert_sameRevisionTwice() throws Exception {
        DocumentRevision rev = createDbObject("1-rev", bodyOne);
        EventSubscriber eventSubscriber = new EventSubscriber();
        datastore.getEventBus().register(eventSubscriber);
        datastore.forceInsert(rev, "1-rev");
        Assert.assertThat(datastore.getDocumentCount(), is(1));
        Assert.assertThat(eventSubscriber.eventCount, is(1));
        datastore.forceInsert(rev, "1-rev");
    }

    @Test
    public void forceInsert_sameLengthOfPath_localRevisionWins() throws Exception {
        {
            DocumentRevision rev = createDbObject();
            datastore.forceInsert(rev, "1-rev", "2-rev", "3-rev", "4-rev");

            DocumentRevision insertedObj = datastore.getDocument(OBJECT_ID);
            insertedObj.setBody(bodyTwo);
            DocumentRevision updateObj = datastore.updateDocumentFromRevision(insertedObj);
            insertedObj.setBody(bodyTwo);
            DocumentRevision updateObj2 = datastore.updateDocumentFromRevision(updateObj);

            Assert.assertNotNull(updateObj2);

            assertDBObjectIsCorrect(OBJECT_ID, 6, bodyTwo);
        }

        String localRevisionId6 = null;
        String remoteRevisionId6 = null;
        {
            // Make sure the latest revision from remote db has smaller String (in terms of String comparison)
            DocumentRevisionTree tree = datastore.getAllRevisionsOfDocument(OBJECT_ID);
            DocumentRevision current = tree.getCurrentRevision();
            List<DocumentRevision> all = tree.getPathForNode(current.getSequence());
            for(DocumentRevision a : all) {
                int g = CouchUtils.generationFromRevId(a.getRevision());
                if(g == 6) {
                    localRevisionId6 = a.getRevision();
                    StringBuilder sb = new StringBuilder(localRevisionId6);
                    sb.setCharAt(2, (char)(sb.charAt(2) - 1));
                    remoteRevisionId6 = sb.toString();
                }
            }

            Assert.assertNotNull(localRevisionId6);
            Assert.assertNotNull(remoteRevisionId6);

            DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
            builder.setDocId(OBJECT_ID);
            builder.setRevId(remoteRevisionId6);
            builder.setDeleted(false);
            builder.setBody(bodyOne);
            DocumentRevision newRev = builder.build();

            datastore.forceInsert(newRev, "1-rev", "2-rev", "3-rev", "4-rev", "5-rev",
                    remoteRevisionId6);
        }

        DocumentRevision obj = datastore.getDocument(OBJECT_ID);
        Assert.assertEquals(localRevisionId6, obj.getRevision());
        Assert.assertTrue(Arrays.equals(bodyTwo.asBytes(), obj.getBody().asBytes()));
    }

    @Test
    public void forceInsert_conflictsWithDocDeletedInLocalDB_nonDeletionWins() throws Exception {
        List<String> revs = new ArrayList<String>();
        revs.add("1-rev");
        revs.add("2-rev");
        revs.add("4-rev");

        {
            DocumentRevision rev = createDbObject();
            datastore.forceInsert(rev, "1-rev", "2-rev", "4-rev");

            DocumentRevision insertedObj = datastore.getDocument(OBJECT_ID);
            insertedObj.setBody(bodyTwo);
            DocumentRevision updateObj = datastore.updateDocumentFromRevision(insertedObj);
            updateObj.setBody(bodyTwo);
            DocumentRevision updateObj2 = datastore.updateDocumentFromRevision(updateObj);
            Assert.assertNotNull(updateObj2);

            // Delete the document from the local database
            datastore.deleteDocumentFromRevision(updateObj2);
            DocumentRevision deletedObj = datastore.getDocument(OBJECT_ID);
            Assert.assertTrue(deletedObj.isDeleted());
        }

        {
            DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
            builder.setDocId(OBJECT_ID);
            builder.setRevId("5-rev");
            builder.setDeleted(false);
            builder.setBody(bodyOne);
            DocumentRevision newRev = builder.build();

            datastore.forceInsert(newRev, "1-rev", "2-rev", "4-rev", "5-rev");
        }

        assertDBObjectIsCorrect(OBJECT_ID, 5, bodyOne);
    }

    @Test
    public void forceInsert_conflictsWithDocDeletedInRemoteDB_nonDeletionWins() throws Exception {
        {
            DocumentRevision rev = createDbObject();
            datastore.forceInsert(rev, "1-rev", "2-rev", "4-rev");

            DocumentRevision insertedObj = datastore.getDocument(OBJECT_ID);
            insertedObj.setBody(bodyTwo);
            DocumentRevision updateObj = datastore.updateDocumentFromRevision(insertedObj);
            updateObj.setBody(bodyTwo);
            DocumentRevision updateObj2 = datastore.updateDocumentFromRevision(updateObj);
            Assert.assertNotNull(updateObj2);
        }

        {
            DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
            builder.setDocId(OBJECT_ID);
            builder.setRevId("8-rev");
            builder.setDeleted(true);
            builder.setBody(DocumentBodyFactory.EMPTY);
            DocumentRevision newRev = builder.build();

            datastore.forceInsert(newRev, "1-rev", "2-rev", "4-rev",
                    "5-rev", "6-rev", "7-rev", "8-rev");
        }

        assertDBObjectIsCorrect(OBJECT_ID, 6, bodyTwo);
    }

    private void assertDBObjectIsCorrect(String docId, int revGeneration, DocumentBody body) throws Exception {
        DocumentRevision obj = datastore.getDocument(docId);
        Assert.assertNotNull(obj);
        Assert.assertEquals(revGeneration, CouchUtils.generationFromRevId(obj.getRevision()));
        Assert.assertTrue(Arrays.equals(body.asBytes(), obj.getBody().asBytes()));
    }

    @Test
    public void forceInsert_newTreeLengthOfOneFromRemoteDb_newTreeShouldBeInsertedAndNewTreeWins() throws Exception {
        {
            DocumentRevision rev = createDbObject("1-a", bodyOne);
            datastore.forceInsert(rev, "1-a" );
            DocumentRevision insertedObj = datastore.getDocument(OBJECT_ID);
            Assert.assertEquals("1-a", insertedObj.getRevision());
        }

        {
            DocumentRevision rev = createDbObject("1-b", bodyTwo);
            datastore.forceInsert(rev, "1-b");
        }

        DocumentRevisionTree tree = datastore.getAllRevisionsOfDocument(OBJECT_ID);
        Assert.assertThat(tree.leafs(), hasSize(2));
        Assert.assertThat(tree.leafRevisionIds(), hasItems("1-a", "1-b"));
        assertDocumentHasRevAndBody(OBJECT_ID, "1-b", bodyTwo);
    }

    @Test
    public void forceInsert_newTreeLengthOfOneFromRemoteDb_newTreeShouldBeInsertedButOldTreeWins() throws Exception {
        {
            DocumentRevision rev = createDbObject("1-x", bodyOne);
            datastore.forceInsert(rev, "1-x" );
            DocumentRevision insertedObj = datastore.getDocument(OBJECT_ID);
            Assert.assertEquals("1-x", insertedObj.getRevision());
        }

        {
            DocumentRevision rev = createDbObject("1-a", bodyTwo);
            datastore.forceInsert(rev, "1-a");
        }

        DocumentRevisionTree tree = datastore.getAllRevisionsOfDocument(OBJECT_ID);
        Assert.assertThat(tree.leafs(), hasSize(2));
        Assert.assertThat(tree.leafRevisionIds(), hasItems("1-a", "1-x"));
        assertDocumentHasRevAndBody(OBJECT_ID, "1-x", bodyOne);
    }

    @Test
    public void forceInsert_newTreeLengthOfTwoFromRemoteDb_newTreeShouldBeInserted() throws Exception{
        {
            DocumentRevision rev = createDbObject("1-x", bodyOne);
            datastore.forceInsert(rev, "1-x");
            DocumentRevision insertedObj = datastore.getDocument(OBJECT_ID);
            Assert.assertEquals("1-x", insertedObj.getRevision());
        }

        {
            DocumentRevision rev = createDbObject("2-c", bodyTwo);
            datastore.forceInsert(rev, "1-a", "2-c");
        }

        DocumentRevisionTree tree = datastore.getAllRevisionsOfDocument(OBJECT_ID);
        Assert.assertThat(tree.leafs(), hasSize(2));
        Assert.assertThat(tree.leafRevisionIds(), hasItems("1-x", "2-c"));

        DocumentRevision leaf = datastore.getDocument(OBJECT_ID, "2-c");
        Assert.assertThat(tree.getPath(leaf.getSequence()), equalTo(Arrays.asList("2-c", "1-a")));

        assertDocumentHasRevAndBody(OBJECT_ID, "2-c", bodyTwo);
    }

    @Test
    public void forceInsert_pickWinnerOfConflicts_Simple_7x_last() throws Exception {

        // regression test for pickWinnerOfConflicts:
        // under previous behaviour this would fail to mark 2-y as current even though 7-x is
        // deleted, if 7-x is inserted last

        // create a chain of revs 1-x -> 6-x
        // then add 2-y and 7-x

        DocumentRevision rev1 = createDbObject("1-x", bodyOne);
        DocumentRevision rev2 = createDbObject("2-x", bodyOne);
        DocumentRevision rev3 = createDbObject("3-x", bodyOne);
        DocumentRevision rev4 = createDbObject("4-x", bodyOne);
        DocumentRevision rev5 = createDbObject("5-x", bodyOne);
        DocumentRevision rev6 = createDbObject("6-x", bodyOne);

        DocumentRevision rev7 = createDbObjectDeleted("7-x");
        DocumentRevision rev2_alt = createDbObject("2-y", bodyOne);

        datastore.forceInsert(rev1, "1-x");
        datastore.forceInsert(rev2, "1-x", "2-x");
        datastore.forceInsert(rev3, "1-x","2-x","3-x");
        datastore.forceInsert(rev4, "1-x", "2-x", "3-x", "4-x");
        datastore.forceInsert(rev5, "1-x", "2-x", "3-x", "4-x", "5-x");
        datastore.forceInsert(rev6, "1-x", "2-x", "3-x", "4-x", "5-x", "6-x");

        datastore.forceInsert(rev2_alt, "1-x", "2-y");
        datastore.forceInsert(rev7, "6-x", "7-x");

        Assert.assertEquals(datastore.getDocument(OBJECT_ID).getRevision(), "2-y");
    }

    @Test
    public void forceInsert_pickWinnerOfConflicts_Simple_2y_last() throws Exception {

        // this test is the same as the one above but we switch round the last two forceInserts
        // - this ensure we get the same result regardless of insertion order

        DocumentRevision rev1 = createDbObject("1-x", bodyOne);
        DocumentRevision rev2 = createDbObject("2-x", bodyOne);
        DocumentRevision rev3 = createDbObject("3-x", bodyOne);
        DocumentRevision rev4 = createDbObject("4-x", bodyOne);
        DocumentRevision rev5 = createDbObject("5-x", bodyOne);
        DocumentRevision rev6 = createDbObject("6-x", bodyOne);

        DocumentRevision rev7 = createDbObjectDeleted("7-x");
        DocumentRevision rev2_alt = createDbObject("2-y", bodyOne);

        datastore.forceInsert(rev1, "1-x");
        datastore.forceInsert(rev2, "1-x", "2-x");
        datastore.forceInsert(rev3, "1-x","2-x","3-x");
        datastore.forceInsert(rev4, "1-x", "2-x", "3-x", "4-x");
        datastore.forceInsert(rev5, "1-x", "2-x", "3-x", "4-x", "5-x");
        datastore.forceInsert(rev6, "1-x", "2-x", "3-x", "4-x", "5-x", "6-x");

        datastore.forceInsert(rev7, "6-x", "7-x");
        datastore.forceInsert(rev2_alt, "1-x", "2-y");

        Assert.assertEquals(datastore.getDocument(OBJECT_ID).getRevision(), "2-y");
    }

    @Test
    public void forceInsert_pickWinnerOfConflicts_Complex() throws Exception {

        // build a complex tree and delete random leaf nodes
        // check that after each delete the correct rev is marked as current
        // ensures that deleted revs are never marked current and that the next eligible leaf in
        // the tree is always correctly nominated to be current by pickWinnerOfConflicts

        // first build a tree such that there will be leaf nodes in the range of generations between
        // 2 and maxtree+2

        int maxTree = 10; // deepest tree will have revs from 1..maxTree+1 followed by the conflicted leaf nodes
        int nConflicts = 3; // number of conflicted leaf nodes
        char startChar = 'a'; // revids will be x-a, x-b, x-c etc where x is the generation
        String startRev = String.format("1-%c", startChar);
        DocumentRevision root = createDbObject(startRev, bodyOne);
        // make subtree for 1-a, 2-a etc
        makeSubTree(startRev, String.format("%c", startChar), 1, maxTree+2, nConflicts);
        // now make subtrees starting at 2-b, 3-c etc, rooted at 1-a, 2-a etc
        // each subtree is of a constant height ensuring leaf nodes at a variety of generations
        for(int i=0; i<maxTree-1; i++) {
            String rootId = String.format("%d-%c", i+1, startChar);
            makeSubTree(rootId, String.format("%c", startChar+i+1), i+1, i+3, nConflicts);
        }

        // now go through continually deleting random leafs until they have all been deleted

        Random a = new Random();

        // fetch non-deleted leafs until there are none left
        List<DocumentRevision> leafs;
        while((leafs = (datastore.getAllRevisionsOfDocument(OBJECT_ID).leafRevisions(true))).size() != 0) {
            // getDocument() should never return a deleted document:
            // under previous behaviour of pickWinnerOfConflicts, this would fail
            DocumentRevision currentLeaf = datastore.getDocument(OBJECT_ID);
            Assert.assertFalse("Current revision should not have been marked as deleted. Current leaf: " +
                    currentLeaf +
                    ". Current state of all leaf nodes: "+leafs,
                    currentLeaf.isDeleted());

            // root the new deleted doc at the randomly selected leaf node
            int random = a.nextInt(leafs.size());
            DocumentRevision randomLeaf =  leafs.get(random);
            String newRevId = String.format("%d-%s-deleted", randomLeaf.getGeneration() + 1,
                    randomLeaf.getRevision());
            DocumentRevision deleted = createDbObjectDeleted(newRevId);
            datastore.forceInsert(deleted, randomLeaf.getRevision(), newRevId);

            // we use the same comparator as pickWinnerOfConflicts
            // re-fetch leafs after insert
            leafs = (datastore.getAllRevisionsOfDocument(OBJECT_ID).leafRevisions(true));
            Collections.sort(leafs, new Comparator<DocumentRevision>() {
                @Override
                public int compare(DocumentRevision r1, DocumentRevision r2) {
                    int generationCompare = r1.getGeneration() - r2.getGeneration();
                    if (generationCompare != 0) {
                        return -generationCompare;
                    } else {
                        return -r1.getRevision().compareTo(r2.getRevision());
                    }
                }
            });
            currentLeaf = datastore.getDocument(OBJECT_ID);

            if (leafs.size() == 0) {
                break;
            }

            // check that our view of 'current' agrees with what pickWinnerOfConflicts did
            Assert.assertEquals(currentLeaf, leafs.get(0));

            // also check that none of the other leafs are marked current
            for (DocumentRevision leaf : leafs.subList(1, leafs.size())) {
                Assert.assertFalse(
                        "Leaf "+leaf+" should not be marked current. Current state of all leaf nodes: "+leafs,
                        leaf.isCurrent());
            }
        }
        // check that after all of them are deleted, the winning rev is the deepest leaf node with
        // the highest sorted revid
        String expectedRevId = String.format("%d-%d-%c%d-deleted", maxTree + 3, maxTree + 2,
                startChar, nConflicts - 1);
        Assert.assertEquals(
                "RevId should have been highest expected value. Current state of all sorted leaf nodes: "+leafs,
                expectedRevId,
                datastore.getDocument(OBJECT_ID).getRevision());
    }

    private void assertDocumentHasRevAndBody(String id, String rev, DocumentBody body) throws Exception {
        DocumentRevision obj = datastore.getDocument(id);
        Assert.assertEquals(rev, obj.getRevision());
        Assert.assertTrue(Arrays.equals(obj.getBody().asBytes(), body.asBytes()));
    }

    // make a chain of revisions from `start` to `depth` inclusive and then add `conflicts` number
    // of leaf nodes. the interior nodes are named "1-x" etc (using `id`) and the leaf nodes are
    // named "12-11-x0" "12-11-x1" etc
    private void makeSubTree(String root, String id, int start, int depth, int conflicts) throws DocumentException {
        int i;
        String lastRevId = root;
        for (i=start; i<depth-1; i++) {
            String revId = String.format("%d-%s", i+1, id);
            DocumentRevision rev = createDbObject(revId, bodyOne);
            datastore.forceInsert(rev, lastRevId, revId);
            lastRevId = revId;
        }
        // now some leaf nodes of the format "12-11-x0", "12-11-x1" etc
        for (int j=0; j<conflicts; j++) {
            String revId = String.format("%d-%s%d", i+1, id, j);
            DocumentRevision rev = createDbObject(revId, bodyOne);
            datastore.forceInsert(rev, lastRevId, revId);
        }
    }

    public static class EventSubscriber {
        int eventCount = 0;

        @Subscribe
        public void event(DocumentModified documentModified) {
            eventCount++;
        }
    }
}
