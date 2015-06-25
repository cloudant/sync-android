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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLQueueCallable;
import com.cloudant.sync.util.CouchUtils;
import com.cloudant.sync.util.DatabaseUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

public class BasicDatastoreCRUDTest extends BasicDatastoreTestBase {

    public ArrayList<DocumentBody> generateDocuments(int count) {
        ArrayList<DocumentBody> result = new ArrayList<DocumentBody>(count);
        for (int i = 0; i < count; i++) {
            HashMap<String, String> map = new HashMap<String,String>();
            map.put(String.format("hello-%d", i), "world");
            DocumentBody documentBody = new BasicDocumentBody(map);
            result.add(documentBody);
        }
        return result;
    }

    @Test(expected = IllegalArgumentException.class)
    public void extensionsFolderDir_null_exception() {
        this.datastore.extensionDataFolder(null);
    }

    @Test
    public void extensionsDir() {
        String expected = this.datastore.datastoreDir + File.separator + "extensions";
        Assert.assertEquals(expected, this.datastore.extensionsDir);
    }

    @Test
    public void extensionsFolderDir_extensionName_correctFolderName() {
        String actual = this.datastore.extensionDataFolder("ext1");
        String expected = this.datastore.extensionsDir + File.separator + "ext1";
        Assert.assertEquals(expected, actual);
    }

    @Test(expected = IllegalStateException.class)
    public void close_getDocumentCount_exception() throws Exception {
        datastore.runOnDbQueue(new SQLQueueCallable<Object>()  {
            @Override
            public Object call(SQLDatabase db) throws Exception {
                Assert.assertTrue(db.isOpen());
                return null;
            }
        }).get();

        this.datastore.close();
        this.datastore.getDocumentCount();
    }

    @Test
    public void createDocument_bodyOnly_success() throws Exception {
        MutableDocumentRevision rev_mut = new MutableDocumentRevision();
        rev_mut.body = bodyOne;
        BasicDocumentRevision rev = datastore.createDocumentFromRevision(rev_mut);
        validateNewlyCreatedDocument(rev);
        Assert.assertNull(rev_mut.docId);
    }

    @Test
    public void createDocument_docIdAndBody_success() throws Exception {
        String docId = CouchUtils.generateDocumentId();
        MutableDocumentRevision newDoc = new MutableDocumentRevision();
        newDoc.docId = docId;
        newDoc.body = bodyOne;
        BasicDocumentRevision rev = datastore.createDocumentFromRevision(newDoc);
        validateNewlyCreatedDocument(rev);
        Assert.assertEquals(docId, rev.getId());
    }

    @Test
    public void createDocument_idInChinese_success() throws Exception {
        String id = "\u738b\u4e1c\u5347";
        MutableDocumentRevision newDoc = new MutableDocumentRevision();
        newDoc.docId = id;
        newDoc.body = bodyOne;
        BasicDocumentRevision rev = datastore.createDocumentFromRevision(newDoc);
        validateNewlyCreatedDocument(rev);
        System.out.println(rev.getId());
        Assert.assertEquals(id, rev.getId());
    }

    @Test(expected = DocumentException.class)
    public void createDocument_existDocId_fail() throws Exception {
        String docId = CouchUtils.generateDocumentId();
        MutableDocumentRevision newDoc = new MutableDocumentRevision();
        newDoc.docId = docId;
        newDoc.body = bodyOne;
        BasicDocumentRevision rev = datastore.createDocumentFromRevision(newDoc);
        validateNewlyCreatedDocument(rev);
        datastore.createDocumentFromRevision(newDoc);
    }

    @Test(expected = DocumentException.class)
    public void createDocument_specialField_fail() throws Exception {
        Map m = createMapWithSpecialField();
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.body = BasicDocumentBody.bodyWith(m);
        datastore.createDocumentFromRevision(rev);
    }

    private Map createMapWithSpecialField() {
        Map m = new HashMap();
        m.put("_a", "A");
        return m;
    }


    @Test
    public void createLocalDocument_docIdAndBody_success() throws Exception {
        String docId = CouchUtils.generateDocumentId();
        LocalDocument localDocument = datastore.insertLocalDocument(docId, bodyOne);
        Assert.assertEquals(docId, localDocument.docId);
    }

    @Test
    public void updateDocument_existingDocument_success() throws Exception {
        MutableDocumentRevision rev_1Mut = new MutableDocumentRevision();
        rev_1Mut.body = bodyOne;
        BasicDocumentRevision rev_1 = datastore.createDocumentFromRevision(rev_1Mut);
        validateNewlyCreatedDocument(rev_1);

        MutableDocumentRevision rev_2Mut = rev_1.mutableCopy();
        rev_2Mut.body = bodyTwo;
        BasicDocumentRevision rev_2 = datastore.updateDocumentFromRevision(rev_2Mut);
        Assert.assertEquals(2, CouchUtils.generationFromRevId(rev_2.getRevision()));
        Assert.assertTrue(rev_2.isCurrent()); // new revision is current revision

        BasicDocumentRevision rev_1_again = datastore.getDocument(rev_1.getId(), rev_1.getRevision());
        Assert.assertTrue(rev_1.isCurrent()); // rev_1 is still marked as "current", and developer need query db to get the latest data, yikes :(
        Assert.assertFalse(rev_1_again.isCurrent());

        MutableDocumentRevision rev_3Mut = rev_2.mutableCopy();
        rev_3Mut.body = bodyOne;
        BasicDocumentRevision rev_3 = datastore.updateDocumentFromRevision(rev_3Mut);
        Assert.assertEquals(3, CouchUtils.generationFromRevId(rev_3.getRevision()));

        MutableDocumentRevision rev_4Mut = rev_3.mutableCopy();
        rev_4Mut.body = bodyTwo;
        BasicDocumentRevision rev_4 = datastore.updateDocumentFromRevision(rev_4Mut);
        Assert.assertEquals(4, CouchUtils.generationFromRevId(rev_4.getRevision()));
    }

    @Test(expected = DocumentException.class)
    public void updateDocument_specialField_exception() throws Exception {
        MutableDocumentRevision rev_1Mut = new MutableDocumentRevision();
        rev_1Mut.body = bodyOne;
        BasicDocumentRevision rev_1 = datastore.createDocumentFromRevision(rev_1Mut);
        validateNewlyCreatedDocument(rev_1);

        Map m = createMapWithSpecialField();
        MutableDocumentRevision rev_2Mut = rev_1.mutableCopy();
        rev_2Mut.body = BasicDocumentBody.bodyWith(m);
        datastore.updateDocumentFromRevision(rev_2Mut);
    }

    @Test(expected = DocumentException.class)
    public void updateDocument_targetDocumentNotCurrentRevision_exception() throws Exception{
        MutableDocumentRevision rev_1Mut = new MutableDocumentRevision();
        rev_1Mut.body = bodyOne;
        BasicDocumentRevision rev_1 = datastore.createDocumentFromRevision(rev_1Mut);
        validateNewlyCreatedDocument(rev_1);
        validateNewlyCreatedDocument(rev_1);

        MutableDocumentRevision rev_2Mut = rev_1.mutableCopy();
        rev_2Mut.body = bodyTwo;
        BasicDocumentRevision rev_2 = datastore.updateDocumentFromRevision(rev_2Mut);
        Assert.assertEquals(2, CouchUtils.generationFromRevId(rev_2.getRevision()));

        rev_1 = datastore.getDocument(rev_1.getId(), rev_1.getRevision());
        Assert.assertFalse(rev_1.isCurrent());
        rev_1Mut = rev_1.mutableCopy();
        rev_1Mut.body = bodyOne;

        datastore.updateDocumentFromRevision(rev_1Mut);
    }

    @Test(expected = ConflictException.class)
    public void deleteDocument_previousRevisionNotLeafNode_exception() throws Exception {
        MutableDocumentRevision rev_1Mut = new MutableDocumentRevision();
        rev_1Mut.body = bodyOne;
        BasicDocumentRevision rev1 = datastore.createDocumentFromRevision(rev_1Mut);
        MutableDocumentRevision rev1_mut = rev1.mutableCopy();
        rev1_mut.body = bodyTwo;
        BasicDocumentRevision rev2 = datastore.updateDocumentFromRevision(rev1_mut);
        Assert.assertNotNull(rev2);
        this.datastore.deleteDocumentFromRevision(rev1);
    }

    @Test
    public void deleteDocument_previousRevisionWasWinner_newRevisionInsertedAsWinner()
            throws Exception {
        MutableDocumentRevision rev_1Mut = new MutableDocumentRevision();
        rev_1Mut.body = bodyOne;
        BasicDocumentRevision rev1 = datastore.createDocumentFromRevision(rev_1Mut);
        
        BasicDocumentRevision deletedRev = this.datastore.deleteDocumentFromRevision(rev1);
        Assert.assertEquals(2, CouchUtils.generationFromRevId(deletedRev.getRevision()));
        Assert.assertTrue(deletedRev.isDeleted());
        Assert.assertTrue(deletedRev.isCurrent());

        DocumentRevisionTree tree = this.datastore.getAllRevisionsOfDocument(rev1.getId());
        BasicDocumentRevision rev2 = (BasicDocumentRevision) tree.getCurrentRevision();
        Assert.assertEquals(2, CouchUtils.generationFromRevId(rev2.getRevision()));
        Assert.assertTrue(rev2.isDeleted());
        Assert.assertTrue(rev2.isCurrent());

    }

    @Test
    public void deleteDocument_previousRevisionWasDeleted_noNewRevisionInserted()
            throws Exception {
        MutableDocumentRevision rev_1Mut = new MutableDocumentRevision();
        rev_1Mut.body = bodyOne;
        BasicDocumentRevision rev1 = datastore.createDocumentFromRevision(rev_1Mut);
        this.datastore.deleteDocumentFromRevision(rev1);
        DocumentRevisionTree tree1 = this.datastore.getAllRevisionsOfDocument(rev1.getId());
        BasicDocumentRevision rev2 = (BasicDocumentRevision) tree1.getCurrentRevision();
        Assert.assertEquals(2, CouchUtils.generationFromRevId(rev2.getRevision()));

        this.datastore.deleteDocumentFromRevision(rev2);
        DocumentRevisionTree tree2 = this.datastore.getAllRevisionsOfDocument(rev1.getId());
        BasicDocumentRevision rev3 = (BasicDocumentRevision) tree2.getCurrentRevision();
        Assert.assertEquals(rev2.getRevision(), rev3.getRevision());
    }

    @Test
    public void deleteDocument_previousRevisionWasNotWinner_newRevisionIsNotWinner()
            throws Exception {

        MutableDocumentRevision rev1aMut = new MutableDocumentRevision();
        rev1aMut.body = bodyOne;
        BasicDocumentRevision rev1a = this.datastore.createDocumentFromRevision(rev1aMut);
        MutableDocumentRevision rev2aMut = rev1a.mutableCopy();
        rev2aMut.body = bodyTwo;
        BasicDocumentRevision rev2a = this.datastore.updateDocumentFromRevision(rev2aMut);
        BasicDocumentRevision rev3b = this.createDetachedDocumentRevision(rev1a.getId(), "3-b", bodyOne);
        this.datastore.forceInsert(rev3b, rev1a.getRevision(), "2-b", "3-b");
        this.datastore.deleteDocumentFromRevision(rev2a);

        DocumentRevisionTree tree = this.datastore.getAllRevisionsOfDocument(rev1a.getId());

        Set<String> leafRevisionIds = tree.leafRevisionIds();
        Assert.assertThat(leafRevisionIds, hasSize(2));
        Assert.assertThat(leafRevisionIds, hasItem("3-b"));
        String newInsertedRevisionId = null;
        for(String revId : leafRevisionIds) {
            if(!revId.equals("3-b")) {
                newInsertedRevisionId = revId;
            }
        }

        Assert.assertEquals(3, CouchUtils.generationFromRevId(newInsertedRevisionId));
        BasicDocumentRevision newInsertedRevision = this.datastore.getDocument(rev1a.getId(), newInsertedRevisionId);
        Assert.assertTrue(newInsertedRevision.isDeleted());
        Assert.assertFalse(newInsertedRevision.isCurrent());
    }

    private BasicDocumentRevision createDetachedDocumentRevision(String docId, String rev, DocumentBody body) {
        DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
        builder.setDocId(docId);
        builder.setRevId(rev);
        builder.setDeleted(false);
        builder.setBody(body);
        return builder.build();
    }

    @Test
    public void getLastSequence_noDocuments_ShouldBeMinusOne() {
        Assert.assertEquals(BasicDatastore.SEQUENCE_NUMBER_START, datastore.getLastSequence());
    }

    @Test
    public void getLastSequence() throws Exception {
        createTwoDocuments();
        Assert.assertEquals(2l, datastore.getLastSequence());
    }

    @Test
    public void getDocumentCount() throws Exception {
        createTwoDocuments();
        Assert.assertEquals(2, datastore.getDocumentCount());
    }

    @Test
    public void getDocument_twoDoc() throws Exception {
        MutableDocumentRevision rev_1Mut = new MutableDocumentRevision();
        rev_1Mut.body = bodyOne;
        BasicDocumentRevision rev_1 = datastore.createDocumentFromRevision(rev_1Mut);
        validateNewlyCreatedDocument(rev_1);

        BasicDocumentRevision revRead_1 = datastore.getDocument(rev_1.getId(), rev_1.getRevision());
        Assert.assertTrue(revRead_1.isCurrent());

        MutableDocumentRevision rev_2Mut = rev_1.mutableCopy();
        rev_2Mut.body = bodyTwo;
        datastore.updateDocumentFromRevision(rev_2Mut);

        BasicDocumentRevision revRead_2 = datastore.getDocument(rev_1.getId(), rev_1.getRevision());
        Assert.assertFalse(revRead_2.isCurrent());

        BasicDocumentRevision revRead_3 = datastore.getDocument(rev_1.getId());
        Assert.assertTrue(revRead_3.isCurrent());
        Assert.assertEquals(2, CouchUtils.generationFromRevId(revRead_3.getRevision()));

        BasicDocumentRevision revRead_3_2 = datastore.getDocument(rev_1.getId());
        Assert.assertTrue(revRead_3_2.isCurrent());
        Assert.assertEquals(2, CouchUtils.generationFromRevId(revRead_3_2.getRevision()));
    }

    @Test
    public void updateLocalDocument_existingDocument_success() throws Exception {
        LocalDocument rev_1 = datastore.insertLocalDocument("docid",bodyOne);

        LocalDocument rev_2 = datastore.insertLocalDocument(rev_1.docId, bodyTwo);
        Assert.assertNotNull(rev_2);

        LocalDocument rev2Read = datastore.getLocalDocument(rev_2.docId);
        Assert.assertNotNull(rev2Read);
        Assert.assertEquals(rev2Read.body.asMap(),bodyTwo.asMap());
    }

    @Test
    public void existsDocument_goodOneAndBadOne() throws Exception {
        MutableDocumentRevision rev_1Mut = new MutableDocumentRevision();
        rev_1Mut.body = bodyOne;
        BasicDocumentRevision rev_1 = datastore.createDocumentFromRevision(rev_1Mut);
        validateNewlyCreatedDocument(rev_1);

        String badRevision = CouchUtils.generateNextRevisionId(rev_1.getRevision());

        Assert.assertTrue(datastore.containsDocument(rev_1.getId(), rev_1.getRevision()));
        Assert.assertFalse(datastore.containsDocument(rev_1.getId(), badRevision));

        Assert.assertTrue(datastore.containsDocument(rev_1.getId()));
        Assert.assertFalse(datastore.containsDocument("-1"));
    }

    @Test
    public void getPublicUUID_correctUUIDShouldBeReturned() throws Exception {
        String publicUUID = datastore.getPublicIdentifier();
        Assert.assertNotNull(publicUUID);

            datastore.runOnDbQueue(new SQLQueueCallable<Object>() {
                @Override
                public Object call(SQLDatabase db) throws Exception {
                    Cursor cursor = null;
                    try {
                        String[] args = new String[]{"publicUUID"};
                        cursor = db.rawQuery(" SELECT count(*) FROM info WHERE key = ? ", args);
                        cursor.moveToFirst();
                        Long count = cursor.getLong(0);
                        Assert.assertEquals(Long.valueOf(1L), count);
                        return null;
                    } finally {
                        DatabaseUtils.closeCursorQuietly(cursor);
                    }
                }
            }).get();

    }

    @Test
    public void getDocumentsWithIds_NA_allSpecifiedDocumentsShouldBeReturnedInCorrectOrder() throws Exception {
        MutableDocumentRevision rev_1Mut = new MutableDocumentRevision();
        rev_1Mut.body = bodyOne;
        BasicDocumentRevision rev_1 = datastore.createDocumentFromRevision(rev_1Mut);
        MutableDocumentRevision rev_1_2Mut = rev_1.mutableCopy();
        rev_1_2Mut.body = bodyTwo;
        BasicDocumentRevision rev_1_2 = datastore.updateDocumentFromRevision(rev_1_2Mut);
        MutableDocumentRevision rev_2Mut = new MutableDocumentRevision();
        rev_2Mut.body = bodyTwo;
        BasicDocumentRevision rev_2 = datastore.createDocumentFromRevision(rev_2Mut);

        List<String> ids = new ArrayList<String>();
        ids.add(rev_1.getId());
        ids.add(rev_2.getId());

        {
            List<BasicDocumentRevision> docs = datastore.getDocumentsWithIds(ids);
            Assert.assertEquals(2, docs.size());
            assertIdAndRevisionAndShallowContent(rev_1_2, docs.get(0));
        }

        List<String> ids2 = new ArrayList<String>();
        ids2.add(rev_2.getId());
        ids2.add(rev_1.getId());

        {
            List<BasicDocumentRevision> docs = datastore.getDocumentsWithIds(ids2);
            Assert.assertEquals(2, docs.size());
            assertIdAndRevisionAndShallowContent(rev_2, docs.get(0));
        }
    }

    private BasicDocumentRevision[] createTwoDocumentsForGetDocumentsWithInternalIdsTest() throws Exception {
        MutableDocumentRevision rev_1Mut = new MutableDocumentRevision();
        rev_1Mut.body = bodyOne;
        BasicDocumentRevision rev_1 = datastore.createDocumentFromRevision(rev_1Mut);
        MutableDocumentRevision rev_1_2Mut = rev_1.mutableCopy();
        rev_1_2Mut.body = bodyTwo;
        BasicDocumentRevision rev_1_2 = datastore.updateDocumentFromRevision(rev_1_2Mut);
        MutableDocumentRevision rev_2Mut = new MutableDocumentRevision();
        rev_2Mut.body = bodyTwo;
        BasicDocumentRevision rev_2 = datastore.createDocumentFromRevision(rev_2Mut);
        return new BasicDocumentRevision[]{ rev_1, rev_2 };
    }

    @Test
    public void getDocumentsWithInternalIds_emptyIdList_emptyListShouldReturn() throws
            Exception {
        createTwoDocumentsForGetDocumentsWithInternalIdsTest();

        List<Long> ids = new ArrayList<Long>();

        {
            List<BasicDocumentRevision> docs = datastore.getDocumentsWithInternalIds(ids);
            Assert.assertEquals(0, docs.size());
        }
    }

    @Test
    public void getDocumentsWithInternalIds_twoIds_allSpecifiedDocumentsShouldBeReturnedInCorrectOrder() throws
            Exception {
        BasicDocumentRevision[] dbObjects = createTwoDocumentsForGetDocumentsWithInternalIdsTest();

        List<Long> ids = new ArrayList<Long>();
        ids.add(dbObjects[1].getInternalNumericId());
        ids.add(dbObjects[0].getInternalNumericId());
        ids.add(101L);

        {
            List<BasicDocumentRevision> docs = datastore.getDocumentsWithInternalIds(ids);
            Assert.assertEquals(2, docs.size());
            Assert.assertEquals(dbObjects[0].getId(), docs.get(0).getId());
            Assert.assertEquals(dbObjects[1].getId(), docs.get(1).getId());
        }
    }


    @Test
    public void getDocumentsWithInternalIds_moreIdsThanSQLiteParameterLimit() throws Exception {

        // Fill a datastore with a large number of docs
        int n_docs = 1200;
        List<Long> internal_ids = new ArrayList<Long>(n_docs);
        for (int i = 0; i < n_docs; i++) {
            Map content = new HashMap();
            content.put("hello", "world");
            DocumentBody body = DocumentBodyFactory.create(content);
            MutableDocumentRevision rev = new MutableDocumentRevision();
            rev.body = body;
            BasicDocumentRevision saved = datastore.createDocumentFromRevision(rev);
            internal_ids.add(saved.getInternalNumericId());
        }

        // Default SQLite parameter limit is 999, and we batch into batches
        // of 500 to be on the safe side, so look at batches around those
        // values
        int[][] tests = new int[][]{
            {0, 998},  // fromIndex, number of documents to retrieve
            {0, 999},
            {0, 1000},
            {50, 1100},
            {450, 499},
            {450, 500},
            {450, 501}
        };
        for (int[] test : tests) {
            int fromIndex = test[0], toIndex = test[0]+test[1];
            List<BasicDocumentRevision> docs = datastore.getDocumentsWithInternalIds(
                internal_ids.subList(fromIndex, toIndex)
            );
            Assert.assertEquals(test[1], docs.size());
        }
    }

    @Test
    public void getDocumentsWithInternalIds_invalidId_emptyListReturned() throws
            Exception {
        createTwoDocumentsForGetDocumentsWithInternalIdsTest();

        List<Long> ids = new ArrayList<Long>();
        ids.add(101L);
        ids.add(102L);

        {
            List<BasicDocumentRevision> docs = datastore.getDocumentsWithInternalIds(ids);
            Assert.assertEquals(0, docs.size());
        }
    }

    private void assertIdAndRevisionAndShallowContent(BasicDocumentRevision expected, BasicDocumentRevision actual) {
        Assert.assertEquals(expected.getId(), actual.getId());
        Assert.assertEquals(expected.getRevision(), actual.getRevision());

        Map<String, Object> expectedMap = expected.getBody().asMap();
        Map<String, Object> actualMap = actual.getBody().asMap();
        for (String key : expectedMap.keySet()) {
            Assert.assertNotNull(actualMap.get(key));
            Assert.assertEquals(expectedMap.get(key), actualMap.get(key));
        }
    }

    @Test
    public void getAllDocuments() throws Exception {

        int objectCount = 100;
        List<DocumentBody> bodies = this.generateDocuments(objectCount);
        List<BasicDocumentRevision> documentRevisions = new ArrayList<BasicDocumentRevision>(objectCount);
        for (int i = 0; i < objectCount; i++) {
            MutableDocumentRevision rev = new MutableDocumentRevision();
            rev.body = bodies.get(i);
            BasicDocumentRevision saved = datastore.createDocumentFromRevision(rev);
            documentRevisions.add(saved);
        }
        ArrayList<BasicDocumentRevision> reversedObjects = new ArrayList<BasicDocumentRevision>(documentRevisions);
        Collections.reverse(reversedObjects);

        // Test count and offsets for descending and ascending
        getAllDocuments_testCountAndOffset(objectCount, documentRevisions, false);
        getAllDocuments_testCountAndOffset(objectCount, reversedObjects, true);
    }

    @Test
    public void getAllDocumentIds() throws Exception {
        Assert.assertTrue(datastore.getAllDocumentIds().isEmpty());
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "document-one";
        rev.body = bodyOne;
        datastore.createDocumentFromRevision(rev);
        rev.docId = "document-two";
        rev.body = bodyTwo;
        datastore.createDocumentFromRevision(rev);
        Assert.assertThat(datastore.getAllDocumentIds(), containsInAnyOrder("document-one",
                                                                            "document-two"));
    }

    @Test
    public void createDbWithSlashAndCreateDocument() throws Exception {
            Datastore datastore = datastoreManager.openDatastore("dbwith/aslash");
            MutableDocumentRevision rev = new MutableDocumentRevision();
            rev.body = bodyOne;
            BasicDocumentRevision doc = datastore.createDocumentFromRevision(rev);
            validateNewlyCreatedDocument(doc);
            datastore.close();
    }

    private void getAllDocuments_testCountAndOffset(int objectCount, List<BasicDocumentRevision> expectedDocumentRevisions, boolean descending) {

        int count;
        int offset = 0;
        List<BasicDocumentRevision> result;

        // Count
        count = 10;
        result = datastore.getAllDocuments(offset, count, descending);
        getAllDocuments_compareResult(expectedDocumentRevisions, result, count, offset);

        count = 47;
        result = datastore.getAllDocuments(offset, count, descending);
        getAllDocuments_compareResult(expectedDocumentRevisions, result, count, offset);

        count = objectCount;
        result = datastore.getAllDocuments(offset, count, descending);
        getAllDocuments_compareResult(expectedDocumentRevisions, result, count, offset);

        count = objectCount * 12;
        result = datastore.getAllDocuments(offset, count, descending);
        getAllDocuments_compareResult(expectedDocumentRevisions, result, objectCount, offset);


        // Offsets
        offset = 10; count = 10;
        result = datastore.getAllDocuments(offset, count, descending);
        getAllDocuments_compareResult(expectedDocumentRevisions, result, count, offset);

        offset = 20; count = 30;
        result = datastore.getAllDocuments(offset, count, descending);
        getAllDocuments_compareResult(expectedDocumentRevisions, result, count, offset);

        offset = objectCount - 3; count = 10;
        result = datastore.getAllDocuments(offset, count, descending);
        getAllDocuments_compareResult(expectedDocumentRevisions, result, 3, offset);

        offset = objectCount + 5; count = 10;
        result = datastore.getAllDocuments(offset, count, descending);
        getAllDocuments_compareResult(expectedDocumentRevisions, result, 0, 0);

        // Error cases
        try {
            offset = 0; count = -10;
            datastore.getAllDocuments(offset, count, descending);
            Assert.fail("IllegalArgumentException not thrown");
        } catch (IllegalArgumentException ex) {
            // All fine
        }
        try {
            offset = -10; count = 10;
            datastore.getAllDocuments(offset, count, descending);
            Assert.fail("IllegalArgumentException not thrown");
        } catch (IllegalArgumentException ex) {
            // All fine
        }
        try {
            offset = 50; count = -10;
            datastore.getAllDocuments(offset, count, descending);
            Assert.fail("IllegalArgumentException not thrown");
        } catch (IllegalArgumentException ex) {
            // All fine
        }
    }

    private void getAllDocuments_compareResult(List<BasicDocumentRevision> expectedDocumentRevisions, List<BasicDocumentRevision> result, int count, int offset) {
        ListIterator<BasicDocumentRevision> iterator;
        iterator = result.listIterator();
        Assert.assertEquals(count, result.size());
        while (iterator.hasNext()) {
            int index = iterator.nextIndex();
            BasicDocumentRevision actual = iterator.next();
            BasicDocumentRevision expected = expectedDocumentRevisions.get(index + offset);

            assertIdAndRevisionAndShallowContent(expected, actual);
        }
    }
}
