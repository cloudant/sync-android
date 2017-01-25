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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

import com.cloudant.sync.documentstore.ConflictException;
import com.cloudant.sync.documentstore.DocumentBody;
import com.cloudant.sync.documentstore.DocumentBodyFactory;
import com.cloudant.sync.documentstore.DocumentNotFoundException;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.documentstore.InvalidDocumentException;
import com.cloudant.sync.documentstore.LocalDocument;
import com.cloudant.sync.internal.documentstore.callables.GetDocumentsWithInternalIdsCallable;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.common.CouchUtils;
import com.cloudant.sync.internal.util.DatabaseUtils;
import com.cloudant.sync.internal.util.JSONUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

public class CrudImplDatabaseTest extends BasicDatastoreTestBase {

    public ArrayList<DocumentBody> generateDocuments(int count) {
        ArrayList<DocumentBody> result = new ArrayList<DocumentBody>(count);
        for (int i = 0; i < count; i++) {
            HashMap<String, String> map = new HashMap<String,String>();
            map.put(String.format("hello-%d", i), "world");
            DocumentBody documentBody = new DocumentBodyImpl(map);
            result.add(documentBody);
        }
        return result;
    }

    @Test(expected = IllegalStateException.class)
    public void close_getDocumentCount_exception() throws Exception {
        datastore.runOnDbQueue(new SQLCallable<Object>()  {
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
        DocumentRevision rev_mut = new DocumentRevision();
        rev_mut.setBody(bodyOne);
        DocumentRevision rev = datastore.create(rev_mut);
        validateNewlyCreatedDocument(rev);
        Assert.assertNull(rev_mut.getId());
    }

    @Test
    public void createDocument_docIdAndBody_success() throws Exception {
        String docId = CouchUtils.generateDocumentId();
        DocumentRevision newDoc = new DocumentRevision(docId);
        newDoc.setBody(bodyOne);
        DocumentRevision rev = datastore.create(newDoc);
        validateNewlyCreatedDocument(rev);
        Assert.assertEquals(docId, rev.getId());
    }

    @Test
    public void createDocument_idInChinese_success() throws Exception {
        String id = "\u738b\u4e1c\u5347";
        DocumentRevision newDoc = new DocumentRevision(id);
        newDoc.setBody(bodyOne);
        DocumentRevision rev = datastore.create(newDoc);
        validateNewlyCreatedDocument(rev);
        System.out.println(rev.getId());
        Assert.assertEquals(id, rev.getId());
    }

    @Test(expected = ConflictException.class)
    public void createDocument_existDocId_fail() throws Exception {
        String docId = CouchUtils.generateDocumentId();
        DocumentRevision newDoc = new DocumentRevision(docId);
        newDoc.setBody(bodyOne);
        DocumentRevision rev = datastore.create(newDoc);
        validateNewlyCreatedDocument(rev);
        datastore.create(newDoc);
    }

    @Test(expected = InvalidDocumentException.class)
    public void createDocument_specialField_fail() throws Exception {
        Map m = createMapWithSpecialField();
        DocumentRevision rev = new DocumentRevision();
        rev.setBody(DocumentBodyImpl.bodyWith(m));
        datastore.create(rev);
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
        DocumentRevision rev_1Mut = new DocumentRevision();
        rev_1Mut.setBody(bodyOne);
        DocumentRevision rev_1 = datastore.create(rev_1Mut);
        validateNewlyCreatedDocument(rev_1);

        DocumentRevision rev_2Mut = rev_1;
        rev_2Mut.setBody(bodyTwo);
        DocumentRevision rev_2 = datastore.update(rev_2Mut);
        Assert.assertEquals(2, CouchUtils.generationFromRevId(rev_2.getRevision()));
        Assert.assertTrue(((DocumentRevision)rev_2).isCurrent()); // new revision is current revision

        DocumentRevision rev_1_again = datastore.read(rev_1.getId(), rev_1.getRevision());
        Assert.assertTrue(((DocumentRevision)rev_1).isCurrent()); // rev_1 is still marked as "current", and developer need query db to get the latest data, yikes :(
        Assert.assertFalse(rev_1_again.isCurrent());

        DocumentRevision rev_3Mut = rev_2;
        rev_3Mut.setBody(bodyOne);
        DocumentRevision rev_3 = datastore.update(rev_3Mut);
        Assert.assertEquals(3, CouchUtils.generationFromRevId(rev_3.getRevision()));

        DocumentRevision rev_4Mut = rev_3;
        rev_4Mut.setBody(bodyTwo);
        DocumentRevision rev_4 = datastore.update(rev_4Mut);
        Assert.assertEquals(4, CouchUtils.generationFromRevId(rev_4.getRevision()));
    }

    @Test(expected = InvalidDocumentException.class)
    public void updateDocument_specialField_exception() throws Exception {
        DocumentRevision rev_1Mut = new DocumentRevision();
        rev_1Mut.setBody(bodyOne);
        DocumentRevision rev_1 = datastore.create(rev_1Mut);
        validateNewlyCreatedDocument(rev_1);

        Map m = createMapWithSpecialField();
        DocumentRevision rev_2Mut = rev_1;
        rev_2Mut.setBody(DocumentBodyImpl.bodyWith(m));
        datastore.update(rev_2Mut);
    }

    @Test(expected = ConflictException.class)
    public void updateDocument_targetDocumentNotCurrentRevision_exception() throws Exception{
        DocumentRevision rev_1Mut = new DocumentRevision();
        rev_1Mut.setBody(bodyOne);
        DocumentRevision rev_1 = datastore.create(rev_1Mut);
        validateNewlyCreatedDocument(rev_1);
        validateNewlyCreatedDocument(rev_1);

        DocumentRevision rev_2Mut = rev_1;
        rev_2Mut.setBody(bodyTwo);
        DocumentRevision rev_2 = datastore.update(rev_2Mut);
        Assert.assertEquals(2, CouchUtils.generationFromRevId(rev_2.getRevision()));

        rev_1 = datastore.read(rev_1.getId(), rev_1.getRevision());
        Assert.assertFalse(((InternalDocumentRevision)rev_1).isCurrent());
        rev_1Mut = rev_1;
        rev_1Mut.setBody(bodyOne);

        datastore.update(rev_1Mut);
    }

    @Test(expected = ConflictException.class)
    public void deleteDocument_previousRevisionNotLeafNode_exception() throws Exception {
        DocumentRevision rev_1Mut = new DocumentRevision();
        rev_1Mut.setBody(bodyOne);
        DocumentRevision rev1 = datastore.create(rev_1Mut);
        DocumentRevision rev1_mut = rev1;
        rev1_mut.setBody(bodyTwo);
        DocumentRevision rev2 = datastore.update(rev1_mut);
        Assert.assertNotNull(rev2);
        this.datastore.delete(rev1);
    }

    @Test
    public void deleteDocument_previousRevisionWasWinner_newRevisionInsertedAsWinner()
            throws Exception {
        DocumentRevision rev_1Mut = new DocumentRevision();
        rev_1Mut.setBody(bodyOne);
        InternalDocumentRevision rev1 = (InternalDocumentRevision)datastore.create(rev_1Mut);

        InternalDocumentRevision deletedRev = (InternalDocumentRevision)this.datastore.delete(rev1);
        Assert.assertEquals(2, CouchUtils.generationFromRevId(deletedRev.getRevision()));
        Assert.assertTrue(deletedRev.isDeleted());
        Assert.assertTrue(deletedRev.isCurrent());
        Assert.assertArrayEquals(JSONUtils.emptyJSONObjectAsBytes(), deletedRev.getBody().asBytes());
        Assert.assertEquals(rev1.getSequence(), deletedRev.getParent());
        Assert.assertTrue(deletedRev.getSequence() > rev1.getSequence());
        Assert.assertEquals(rev1.getInternalNumericId(), deletedRev.getInternalNumericId());
        Assert.assertTrue(deletedRev.isCurrent());

        DocumentRevisionTree tree = this.datastore.getAllRevisionsOfDocument(rev1.getId());
        DocumentRevision rev2 = (DocumentRevision) tree.getCurrentRevision();
        Assert.assertEquals(2, CouchUtils.generationFromRevId(rev2.getRevision()));
        Assert.assertTrue(rev2.isDeleted());
        Assert.assertTrue(rev2.isCurrent());

    }

    @Test
    public void deleteDocument_previousRevisionWasDeleted_noNewRevisionInserted()
            throws Exception {
        DocumentRevision rev_1Mut = new DocumentRevision();
        rev_1Mut.setBody(bodyOne);
        DocumentRevision rev1 = datastore.create(rev_1Mut);
        this.datastore.delete(rev1);
        DocumentRevisionTree tree1 = this.datastore.getAllRevisionsOfDocument(rev1.getId());
        DocumentRevision rev2 = (DocumentRevision) tree1.getCurrentRevision();
        Assert.assertEquals(2, CouchUtils.generationFromRevId(rev2.getRevision()));

        try {
            this.datastore.delete(rev2);
            Assert.fail("Expected DocumentNotFoundException");
        } catch(DocumentNotFoundException dnfe) {
            ;
        }
        DocumentRevisionTree tree2 = this.datastore.getAllRevisionsOfDocument(rev1.getId());
        DocumentRevision rev3 = (DocumentRevision) tree2.getCurrentRevision();
        Assert.assertEquals(rev2.getRevision(), rev3.getRevision());
    }

    @Test
    public void deleteDocument_previousRevisionWasNotWinner_newRevisionIsNotWinner()
            throws Exception {

        DocumentRevision rev1aMut = new DocumentRevision();
        rev1aMut.setBody(bodyOne);
        InternalDocumentRevision rev1a = (InternalDocumentRevision)this.datastore.create(rev1aMut);
        DocumentRevision rev2aMut = rev1a;
        rev2aMut.setBody(bodyTwo);
        InternalDocumentRevision rev2a = (InternalDocumentRevision)this.datastore.update(rev2aMut);
        InternalDocumentRevision rev3b = this.createDetachedDocumentRevision(rev1a.getId(), "3-b", bodyOne);
        this.datastore.forceInsert(rev3b, rev1a.getRevision(), "2-b", "3-b");

        InternalDocumentRevision deletedRev = (InternalDocumentRevision)this.datastore.delete(rev2a);
        Assert.assertArrayEquals(JSONUtils.emptyJSONObjectAsBytes(), deletedRev.getBody().asBytes());
        Assert.assertEquals(rev2a.getSequence(), deletedRev.getParent());
        Assert.assertTrue(deletedRev.getSequence() > rev2a.getSequence());
        Assert.assertEquals(rev2a.getInternalNumericId(), deletedRev.getInternalNumericId());
        Assert.assertFalse(deletedRev.isCurrent());

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
        DocumentRevision newInsertedRevision = this.datastore.read(rev1a.getId(), newInsertedRevisionId);
        Assert.assertTrue(newInsertedRevision.isDeleted());
        Assert.assertFalse(newInsertedRevision.isCurrent());
    }

    private InternalDocumentRevision createDetachedDocumentRevision(String docId, String rev, DocumentBody body) {
        DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
        builder.setDocId(docId);
        builder.setRevId(rev);
        builder.setDeleted(false);
        builder.setBody(body);
        return builder.build();
    }

    @Test
    public void getLastSequence_noDocuments_ShouldBeMinusOne() throws Exception {
        Assert.assertEquals(DatabaseImpl.SEQUENCE_NUMBER_START, datastore.getLastSequence());
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
        DocumentRevision rev_1Mut = new DocumentRevision();
        rev_1Mut.setBody(bodyOne);
        DocumentRevision rev_1 = datastore.create(rev_1Mut);
        validateNewlyCreatedDocument(rev_1);

        DocumentRevision revRead_1 = datastore.read(rev_1.getId(), rev_1.getRevision());
        Assert.assertTrue(revRead_1.isCurrent());

        DocumentRevision rev_2Mut = rev_1;
        rev_2Mut.setBody(bodyTwo);
        datastore.update(rev_2Mut);

        DocumentRevision revRead_2 = datastore.read(rev_1.getId(), rev_1.getRevision());
        Assert.assertFalse(revRead_2.isCurrent());

        DocumentRevision revRead_3 = datastore.read(rev_1.getId());
        Assert.assertTrue(revRead_3.isCurrent());
        Assert.assertEquals(2, CouchUtils.generationFromRevId(revRead_3.getRevision()));

        DocumentRevision revRead_3_2 = datastore.read(rev_1.getId());
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
        DocumentRevision rev_1Mut = new DocumentRevision();
        rev_1Mut.setBody(bodyOne);
        DocumentRevision rev_1 = datastore.create(rev_1Mut);
        validateNewlyCreatedDocument(rev_1);

        String badRevision = CouchUtils.generateNextRevisionId(rev_1.getRevision());

        Assert.assertTrue(datastore.contains(rev_1.getId(), rev_1.getRevision()));
        Assert.assertFalse(datastore.contains(rev_1.getId(), badRevision));

        Assert.assertTrue(datastore.contains(rev_1.getId()));
        Assert.assertFalse(datastore.contains("-1"));
    }

    @Test
    public void getPublicUUID_correctUUIDShouldBeReturned() throws Exception {
        String publicUUID = datastore.getPublicIdentifier();
        Assert.assertNotNull(publicUUID);

            datastore.runOnDbQueue(new SQLCallable<Object>() {
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
        DocumentRevision rev_1Mut = new DocumentRevision();
        rev_1Mut.setBody(bodyOne);
        DocumentRevision rev_1 = datastore.create(rev_1Mut);
        DocumentRevision rev_1_2Mut = rev_1;
        rev_1_2Mut.setBody(bodyTwo);
        DocumentRevision rev_1_2 = datastore.update(rev_1_2Mut);
        DocumentRevision rev_2Mut = new DocumentRevision();
        rev_2Mut.setBody(bodyTwo);
        DocumentRevision rev_2 = datastore.create(rev_2Mut);

        List<String> ids = new ArrayList<String>();
        ids.add(rev_1.getId());
        ids.add(rev_2.getId());

        {
            List<? extends DocumentRevision> docs = datastore.read(ids);
            Assert.assertEquals(2, docs.size());
            assertIdAndRevisionAndShallowContent(rev_1_2, (InternalDocumentRevision)docs.get(0));
        }

        List<String> ids2 = new ArrayList<String>();
        ids2.add(rev_2.getId());
        ids2.add(rev_1.getId());

        {
            List<? extends DocumentRevision> docs = datastore.read(ids2);
            Assert.assertEquals(2, docs.size());
            assertIdAndRevisionAndShallowContent(rev_2, (InternalDocumentRevision)docs.get(0));
        }
    }

    private DocumentRevision[] createTwoDocumentsForGetDocumentsWithInternalIdsTest() throws Exception {
        DocumentRevision rev_1Mut = new DocumentRevision();
        rev_1Mut.setBody(bodyOne);
        DocumentRevision rev_1 = datastore.create(rev_1Mut);
        DocumentRevision rev_1_2Mut = rev_1;
        rev_1_2Mut.setBody(bodyTwo);
        DocumentRevision rev_1_2 = datastore.update(rev_1_2Mut);
        DocumentRevision rev_2Mut = new DocumentRevision();
        rev_2Mut.setBody(bodyTwo);
        DocumentRevision rev_2 = datastore.create(rev_2Mut);
        return new DocumentRevision[]{ rev_1, rev_2 };
    }

    @Test
    public void getDocumentsWithInternalIds_emptyIdList_emptyListShouldReturn() throws
            Exception {
        createTwoDocumentsForGetDocumentsWithInternalIdsTest();

        List<Long> ids = new ArrayList<Long>();

        {
            List<? extends DocumentRevision> docs = datastore.runOnDbQueue(new GetDocumentsWithInternalIdsCallable(ids, null, null)).get();
            Assert.assertEquals(0, docs.size());
        }
    }

    @Test
    public void getDocumentsWithInternalIds_twoIds_allSpecifiedDocumentsShouldBeReturnedInCorrectOrder() throws
            Exception {
        DocumentRevision[] dbObjects = createTwoDocumentsForGetDocumentsWithInternalIdsTest();

        List<Long> ids = new ArrayList<Long>();
        ids.add(((InternalDocumentRevision)dbObjects[1]).getInternalNumericId());
        ids.add(((InternalDocumentRevision)dbObjects[0]).getInternalNumericId());
        ids.add(101L);

        {
            List<? extends DocumentRevision> docs = datastore.runOnDbQueue(new GetDocumentsWithInternalIdsCallable(ids, null, null)).get();
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
            DocumentRevision rev = new DocumentRevision();
            rev.setBody(body);
            DocumentRevision saved = datastore.create(rev);
            internal_ids.add(((InternalDocumentRevision)saved).getInternalNumericId());
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
            List<? extends DocumentRevision> docs = datastore.runOnDbQueue(new
                    GetDocumentsWithInternalIdsCallable(internal_ids.subList(fromIndex, toIndex),
                    null, null)).get();
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
            List<? extends DocumentRevision> docs = datastore.runOnDbQueue(new GetDocumentsWithInternalIdsCallable(ids, null, null)).get();
            Assert.assertEquals(0, docs.size());
        }
    }

    private void assertIdAndRevisionAndShallowContent(DocumentRevision expected, DocumentRevision actual) {
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
        List<DocumentRevision> documentRevisions = new ArrayList<DocumentRevision>(objectCount);
        for (int i = 0; i < objectCount; i++) {
            DocumentRevision rev = new DocumentRevision();
            rev.setBody(bodies.get(i));
            DocumentRevision saved = datastore.create(rev);
            documentRevisions.add(saved);
        }
        ArrayList<DocumentRevision> reversedObjects = new ArrayList<DocumentRevision>(documentRevisions);
        Collections.reverse(reversedObjects);

        // Test count and offsets for descending and ascending
        getAllDocuments_testCountAndOffset(objectCount, documentRevisions, false);
        getAllDocuments_testCountAndOffset(objectCount, reversedObjects, true);
    }

    @Test
    public void getAllDocumentIds() throws Exception {
        Assert.assertTrue(datastore.getIds().isEmpty());
        DocumentRevision rev = new DocumentRevision("document-one");
        rev.setBody(bodyOne);
        datastore.create(rev);
        rev = new DocumentRevision("document-two");
        rev.setBody(bodyTwo);
        datastore.create(rev);
        Assert.assertThat(datastore.getIds(), containsInAnyOrder("document-one",
                                                                            "document-two"));
    }

    @Test
    public void compactConflictedTree() throws Exception {
        String docId = "document-one";

        // create a root node
        DocumentRevision root = new DocumentRevision(docId);
        root.setBody(bodyOne);
        root = datastore.create(root);

        // create some leaf nodes
        for (int i=0; i<10; i++) {
            InternalDocumentRevision leaf = new InternalDocumentRevision(docId, "2-xyz"+i, bodyOne, null);
            datastore.forceInsert(leaf, root.getRevision(), leaf.getRevision());
        }

        // check root and leafs have bodies before compaction
        Assert.assertTrue("root body must not be empty before compaction", root.getBody().asMap().size() > 0);
        for (DocumentRevision leaf : datastore.getAllRevisionsOfDocument(docId).leafRevisions()) {
            Assert.assertTrue("leaf body must not be empty before compaction", leaf.getBody().asMap().size() > 0);
        }

        datastore.compact();

        // re-fetch and check root and leafs: root should not have a body but leafs should
        root = datastore.read(root.getId(), root.getRevision());
        Assert.assertEquals("root body must be empty after compaction", 0, root.getBody().asMap().size());
        for (DocumentRevision leaf : datastore.getAllRevisionsOfDocument(docId).leafRevisions()) {
            Assert.assertTrue("leaf body must not be empty after compaction", leaf.getBody().asMap().size() > 0);
        }

    }

    private void getAllDocuments_testCountAndOffset(int objectCount, List<DocumentRevision> expectedDocumentRevisions, boolean descending) throws DocumentStoreException {

        int count;
        int offset = 0;
        List<DocumentRevision> result;

        // Count
        count = 10;
        result = datastore.read(offset, count, descending);
        getAllDocuments_compareResult(expectedDocumentRevisions, result, count, offset);

        count = 47;
        result = datastore.read(offset, count, descending);
        getAllDocuments_compareResult(expectedDocumentRevisions, result, count, offset);

        count = objectCount;
        result = datastore.read(offset, count, descending);
        getAllDocuments_compareResult(expectedDocumentRevisions, result, count, offset);

        count = objectCount * 12;
        result = datastore.read(offset, count, descending);
        getAllDocuments_compareResult(expectedDocumentRevisions, result, objectCount, offset);


        // Offsets
        offset = 10; count = 10;
        result = datastore.read(offset, count, descending);
        getAllDocuments_compareResult(expectedDocumentRevisions, result, count, offset);

        offset = 20; count = 30;
        result = datastore.read(offset, count, descending);
        getAllDocuments_compareResult(expectedDocumentRevisions, result, count, offset);

        offset = objectCount - 3; count = 10;
        result = datastore.read(offset, count, descending);
        getAllDocuments_compareResult(expectedDocumentRevisions, result, 3, offset);

        offset = objectCount + 5; count = 10;
        result = datastore.read(offset, count, descending);
        getAllDocuments_compareResult(expectedDocumentRevisions, result, 0, 0);

        // Error cases
        try {
            offset = 0; count = -10;
            datastore.read(offset, count, descending);
            Assert.fail("IllegalArgumentException not thrown");
        } catch (IllegalArgumentException ex) {
            // All fine
        }
        try {
            offset = -10; count = 10;
            datastore.read(offset, count, descending);
            Assert.fail("IllegalArgumentException not thrown");
        } catch (IllegalArgumentException ex) {
            // All fine
        }
        try {
            offset = 50; count = -10;
            datastore.read(offset, count, descending);
            Assert.fail("IllegalArgumentException not thrown");
        } catch (IllegalArgumentException ex) {
            // All fine
        }
    }

    private void getAllDocuments_compareResult(List<DocumentRevision> expectedDocumentRevisions, List<DocumentRevision> result, int count, int offset) {
        ListIterator<DocumentRevision> iterator;
        iterator = result.listIterator();
        Assert.assertEquals(count, result.size());
        while (iterator.hasNext()) {
            int index = iterator.nextIndex();
            DocumentRevision actual = iterator.next();
            DocumentRevision expected = expectedDocumentRevisions.get(index + offset);

            assertIdAndRevisionAndShallowContent(expected, actual);
        }
    }
}
