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

package com.cloudant.sync.datastore;

import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.CouchUtils;
import com.cloudant.sync.util.TestUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class BasicDatastoreForceInsertTest {

    public static final String OBJECT_ID = "object_id";
    String database_dir ;
    String documentOneFile = "fixture/document_1.json";
    String documentTwoFile = "fixture/document_2.json";

    SQLDatabase database = null;
    BasicDatastore datastore = null;
    byte[] jsonData = null;
    DocumentBody bodyOne = null;
    DocumentBody bodyTwo = null;

    @Before
    public void setUp() throws Exception {
        database_dir = TestUtils.createTempTestingDir(BasicDatastoreForceInsertTest.class.getName());
        datastore = new BasicDatastore(database_dir, "test");
        database = datastore.getSQLDatabase();

        jsonData = FileUtils.readFileToByteArray(new File(documentOneFile));
        bodyOne = new BasicDocumentBody(jsonData);

        jsonData = FileUtils.readFileToByteArray(new File(documentTwoFile));
        bodyTwo = new BasicDocumentBody(jsonData);
    }

    @After
    public void tearDown() throws Exception {
        TestUtils.deleteTempTestingDir(database_dir);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forceInsert_revHistoryNotInRightOrder_exception() {
        DocumentRevision rev = createDbObject();
        datastore.forceInsert(rev, "1-rev", "3-rev", "2-rev", "4-rev");
    }

    @Test(expected = IllegalArgumentException.class)
    public void forceInsert_currentRevisionNotInTheHistory_exception() {
        DocumentRevision rev = createDbObject();
        datastore.forceInsert(rev, "1-rev", "2-rev", "3-rev");
    }

    @Test
    public void forceInsert_documentNotInLocalDB_documentShouldBeInserted() {
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


    @Test
    public void forceInsert_newRevisionsFromRemoteDB_newRevisionShouldBeInserted() {
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
    public void forceInsert_longerPathFromRemoteDB_remoteDBWins() throws ConflictException {

        {
            DocumentRevision rev = createDbObject();
            datastore.forceInsert(rev, "1-rev", "2-rev", "4-rev");

            DocumentRevision insertedObj = datastore.getDocument(OBJECT_ID);
            DocumentRevision updateObj = datastore.updateDocument(insertedObj.getId(), insertedObj.getRevision(), bodyTwo);
            Assert.assertNotNull(updateObj);

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
    public void forceInsert_longerPathFromLocalDB_localDBWins() throws ConflictException {
        {
            DocumentRevision rev = createDbObject();
            datastore.forceInsert(rev, "1-rev", "2-rev", "4-rev");

            DocumentRevision insertedObj = datastore.getDocument(OBJECT_ID);
            DocumentRevision updateObj = datastore.updateDocument(insertedObj.getId(), insertedObj.getRevision(), bodyTwo);
            DocumentRevision updateObj2 = datastore.updateDocument(updateObj.getId(), updateObj.getRevision(), bodyTwo);
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
    public void forceInsert_sameLengthOfPath_remoteRevisionWins() throws ConflictException {
        {
            DocumentRevision rev = createDbObject();
            datastore.forceInsert(rev, "1-rev", "2-rev", "3-rev", "4-rev");

            DocumentRevision insertedObj = datastore.getDocument(OBJECT_ID);
            DocumentRevision updateObj = datastore.updateDocument(insertedObj.getId(), insertedObj.getRevision(), bodyTwo);
            DocumentRevision updateObj2 = datastore.updateDocument(updateObj.getId(), updateObj.getRevision(), bodyTwo);
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

    @Test
    public void forceInsert_sameLengthOfPath_localRevisionWins() throws ConflictException {
        {
            DocumentRevision rev = createDbObject();
            datastore.forceInsert(rev, "1-rev", "2-rev", "3-rev", "4-rev");

            DocumentRevision insertedObj = datastore.getDocument(OBJECT_ID);
            DocumentRevision updateObj = datastore.updateDocument(insertedObj.getId(), insertedObj.getRevision(), bodyTwo);
            DocumentRevision updateObj2 = datastore.updateDocument(updateObj.getId(), updateObj.getRevision(), bodyTwo);
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

            datastore.forceInsert(newRev, "1-rev", "2-rev", "3-rev", "4-rev", "5-rev", remoteRevisionId6);
        }

        DocumentRevision obj = datastore.getDocument(OBJECT_ID);
        Assert.assertEquals(localRevisionId6, obj.getRevision());
        Assert.assertTrue(Arrays.equals(bodyTwo.asBytes(), obj.getBody().asBytes()));
    }

    @Test
    public void forceInsert_conflictsWithDocDeletedInLocalDB_nonDeletionWins() throws ConflictException {
        List<String> revs = new ArrayList<String>();
        revs.add("1-rev");
        revs.add("2-rev");
        revs.add("4-rev");

        {
            DocumentRevision rev = createDbObject();
            datastore.forceInsert(rev, "1-rev", "2-rev", "4-rev");

            DocumentRevision insertedObj = datastore.getDocument(OBJECT_ID);
            DocumentRevision updateObj = datastore.updateDocument(insertedObj.getId(), insertedObj.getRevision(), bodyTwo);
            DocumentRevision updateObj2 = datastore.updateDocument(updateObj.getId(), updateObj.getRevision(), bodyTwo);
            Assert.assertNotNull(updateObj2);

            // Delete the document from the local database
            datastore.deleteDocument(updateObj2.getId(), updateObj2.getRevision());
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
    public void forceInsert_conflictsWithDocDeletedInRemoteDB_nonDeletionWins() throws ConflictException {
        {
            DocumentRevision rev = createDbObject();
            datastore.forceInsert(rev, "1-rev", "2-rev", "4-rev");

            DocumentRevision insertedObj = datastore.getDocument(OBJECT_ID);
            DocumentRevision updateObj = datastore.updateDocument(insertedObj.getId(), insertedObj.getRevision(), bodyTwo);
            DocumentRevision updateObj2 = datastore.updateDocument(updateObj.getId(), updateObj.getRevision(), bodyTwo);
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

    private void assertDBObjectIsCorrect(String docId, int revGeneration, DocumentBody body) {
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
    public void forceInsert_newTreeLengthOfTwoFromRemoteDb_newTreeShouldBeInserted() {
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

    private void assertDocumentHasRevAndBody(String id, String rev, DocumentBody body) {
        DocumentRevision obj = datastore.getDocument(id);
        Assert.assertEquals(rev, obj.getRevision());
        Assert.assertTrue(Arrays.equals(obj.getBody().asBytes(), body.asBytes()));
    }

}
