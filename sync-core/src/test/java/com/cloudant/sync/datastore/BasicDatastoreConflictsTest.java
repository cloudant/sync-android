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

import com.cloudant.sync.util.CouchUtils;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.hasSize;

public class BasicDatastoreConflictsTest extends BasicDatastoreTestBase {

    @Test
    public void getConflictedDocumentIds_oneConflictWithTwoConflictedLeafs() {
        DocumentRevision rev = this.createDocumentRevision("Tom");
        DocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "1-rev", "Jerry");
        this.datastore.forceInsert(newRev, "1-rev");
        Iterator<String> iterator = this.datastore.getConflictedDocumentIds();
        List<String> conflictedDocId = Lists.newArrayList(iterator);
        Assert.assertThat(conflictedDocId, hasSize(1));
        Assert.assertThat(conflictedDocId, hasItems(rev.getId()));
    }


    @Test
    public void getConflictedDocumentIds_conflictWithThreeConflictedLeafs() {
        DocumentRevision rev = this.createDocumentRevision("Tom");
        DocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "4-a", "Jerry");
        this.datastore.forceInsert(newRev, "1-a", "2-a", "3-a", "4-a");
        DocumentRevision newRev2 = this.createDetachedDocumentRevision(rev.getId(), "3-b", "Harry");
        this.datastore.forceInsert(newRev2, "1-b", "2-b", "3-b");

        Iterator<String> iterator = this.datastore.getConflictedDocumentIds();
        List<String> conflictedDocId = Lists.newArrayList(iterator);
        Assert.assertThat(conflictedDocId, hasSize(1));
        Assert.assertThat(conflictedDocId, hasItems(rev.getId()));
    }

    @Test
    public void getConflictedDocumentIds_oneDeletedLeafAndOneLiveLeaf_conflicts() throws ConflictException {
        DocumentRevision rev = this.createDocumentRevision("Tom");
        this.datastore.deleteDocument(rev.getId(), rev.getRevision());
        DocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "4-a", "Jerry");
        this.datastore.forceInsert(newRev, "1-a", "2-a", "3-a", "4-a");

        Iterator<String> iterator = this.datastore.getConflictedDocumentIds();
        List<String> conflictedDocId = Lists.newArrayList(iterator);
        Assert.assertThat(conflictedDocId, hasSize(0));
    }

    @Test
    public void getConflictedDocumentIds_zeroConflicts() {
        testWithConflictCount(0);
    }

    @Test
    public void getConflictedDocumentIds_twoConflict() {
        testWithConflictCount(2);
    }

    @Test
    public void getConflictedDocumentIds_10Conflicts() {
        testWithConflictCount(10);
    }

    @Test
    public void getConflictedDocumentIds_1000Conflicts() {
        testWithConflictCount(1000);
    }

    @Test
    public void resolveConflictsForDocument_twoConflictAndException_nothing()
            throws ConflictException {
        String docId = this.createConflictedDocument();
        long expectedSequence = this.datastore.getLastSequence();
        this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
            @Override
            public DocumentRevision resolve(String docId, List<DocumentRevision> conflicts) {
                throw new IllegalStateException("Mocked error");
            }
        });
        long actualSequence = this.datastore.getLastSequence();
        Assert.assertEquals(expectedSequence, actualSequence);
    }

    @Test
    public void resolveConflictsForDocument_twoConflictAndReturnNull_nothing()
            throws ConflictException {
        String docId = this.createConflictedDocument();
        long expectedSequence = this.datastore.getLastSequence();
        this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
            @Override
            public DocumentRevision resolve(String docId, List<DocumentRevision> conflicts) {
                return null;
            }
        });
        long actualSequence = this.datastore.getLastSequence();
        Assert.assertEquals(expectedSequence, actualSequence);
    }

    @Test
    public void resolveConflictsForDocument_twoConflictAndNewWinner_newWinnerInserted()
            throws ConflictException {
        String docId = this.createConflictedDocument();
        DocumentRevisionTree oldTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertTrue(oldTree.hasConflicts());
        long expectedSequence = this.datastore.getLastSequence() + 2;

        this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
            @Override
            public DocumentRevision resolve(String docId, List<DocumentRevision> conflicts) {
                Assert.assertEquals(2, conflicts.size());
                return createDetachedDocumentRevision(docId, "1-ignored", "Carl");
            }
        });
        long actualSequence = this.datastore.getLastSequence();
        Assert.assertEquals(expectedSequence, actualSequence);

        DocumentRevisionTree newTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertFalse(newTree.hasConflicts());
        DocumentRevision newWinner = newTree.getCurrentRevision();
        Assert.assertEquals("Carl", newWinner.asMap().get("name"));
    }

    @Test
    public void resolveConflictsForDocument_threeConflictAndNewWinner_newWinnerInserted()
            throws ConflictException {
        String docId = this.createConflictedDocumentWithThreeLeafs();
        DocumentRevisionTree oldTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertTrue(oldTree.hasConflicts());
        long expectedSequence = this.datastore.getLastSequence() + 3;

        this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
            @Override
            public DocumentRevision resolve(String docId, List<DocumentRevision> conflicts) {
                Assert.assertEquals(3, conflicts.size());
                return createDetachedDocumentRevision(docId, "1-ignored", "Aaron");
            }
        });
        long actualSequence = this.datastore.getLastSequence();
        Assert.assertEquals(expectedSequence, actualSequence);

        DocumentRevisionTree newTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertFalse(newTree.hasConflicts());
        DocumentRevision newWinner = newTree.getCurrentRevision();
        Assert.assertEquals("Aaron", newWinner.asMap().get("name"));
    }

    @Test
    public void resolveConflictsForDocument_threeConflictAndNewWinnerAsDeleted_documentDeleted()
            throws ConflictException {
        String docId = this.createConflictedDocumentWithThreeLeafs();
        DocumentRevisionTree oldTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertTrue(oldTree.hasConflicts());
        long expectedSequence = this.datastore.getLastSequence() + 3;

        this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
            @Override
            public DocumentRevision resolve(String docId, List<DocumentRevision> conflicts) {
                Assert.assertEquals(3, conflicts.size());
                BasicDocumentRevision revision =
                        (BasicDocumentRevision)createDetachedDocumentRevision(docId, "1-ignored", "Aaron");
                revision.setDeleted(true);
                return revision;
            }
        });
        long actualSequence = this.datastore.getLastSequence();
        Assert.assertEquals(expectedSequence, actualSequence);

        DocumentRevisionTree newTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertFalse(newTree.hasConflicts());
        DocumentRevision newWinner = newTree.getCurrentRevision();
        Assert.assertTrue(newWinner.isDeleted());
    }

    @Test
    public void resolveConflictsForDocument_timestampBasedResolver_revisionWithLatestTimestampWins()
            throws ConflictException {
        long ts = System.currentTimeMillis();
        DocumentBody body1 = this.createDocumentBody("Tom", ts);
        DocumentRevision rev = this.datastore.createDocument(body1);
        DocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "4-a", "Jerry", ts + 1);
        this.datastore.forceInsert(newRev, "1-a", "2-a", "3-a", "4-a");
        DocumentRevision newRev2 = this.createDetachedDocumentRevision(rev.getId(), "2-b", "Carl", ts + 2);
        this.datastore.forceInsert(newRev2, "1-b", "2-b");

        DocumentRevision oldWinner = this.datastore.getDocument(rev.getId());
        Assert.assertEquals("4-a", oldWinner.getRevision());
        Assert.assertEquals("Jerry", oldWinner.getBody().asMap().get("name"));

        this.datastore.resolveConflictsForDocument(rev.getId(), new TimestampBasedConflictsResolver());

        DocumentRevision newWinner = this.datastore.getDocument(rev.getId());
        Assert.assertEquals("Carl", newWinner.asMap().get("name"));
        int generation = CouchUtils.generationFromRevId(newWinner.getRevision());
        Assert.assertEquals(Integer.valueOf(5), Integer.valueOf(generation));
    }

    private void testWithConflictCount(int conflictCount) {
        List<String> expectedConflicts = createConflictDocuments(conflictCount);
        Iterator<String> iterator = this.datastore.getConflictedDocumentIds();
        List<String> actualConflicts = Lists.newArrayList(iterator);
        Assert.assertThat(actualConflicts, hasSize(expectedConflicts.size()));
        for(String id : expectedConflicts) {
            Assert.assertThat(actualConflicts, hasItem(id));
        }
    }

    private List<String> createConflictDocuments(int conflictCount) {
        List<String> expectedConflicts = new ArrayList<String>();
        for(int i = 0 ; i <  conflictCount; i ++) {
            String id = createConflictedDocument();
            expectedConflicts.add(id);
        }
        return expectedConflicts;
    }

    private String createConflictedDocument() {
        DocumentRevision rev = this.createDocumentRevision("Tom");
        DocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "2-a", "Jerry");
        this.datastore.forceInsert(newRev, "1-a", "2-a");
        return rev.getId();
    }

    private String createConflictedDocumentWithThreeLeafs() {
        DocumentRevision rev = this.createDocumentRevision("Tom");
        DocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "2-a", "Jerry");
        this.datastore.forceInsert(newRev, "1-a", "2-a");
        DocumentRevision newRev2 = this.createDetachedDocumentRevision(rev.getId(), "4-b", "Carl");
        this.datastore.forceInsert(newRev2, "1-b", "2-b", "3-b", "4-b");
        return rev.getId();
    }


    private DocumentRevision createDocumentRevision(String name) {
        DocumentBody body = createDocumentBody(name);
        return this.datastore.createDocument(body);
    }

    private DocumentRevision createDetachedDocumentRevision(String docId, String rev, String name) {
        return createDetachedDocumentRevision(docId, rev, name, System.currentTimeMillis());
    }

    private DocumentRevision createDetachedDocumentRevision(String docId, String rev, String name, long ts) {
        DocumentBody body = this.createDocumentBody(name, ts);
        DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
        builder.setDocId(docId);
        builder.setRevId(rev);
        builder.setDeleted(false);
        builder.setBody(body);
        return builder.build();
    }

    private DocumentBody createDocumentBody(String name, long ts) {
        Map m = new HashMap<String, Object>();
        m.put("name", name);
        m.put("timestamp", ts);
        return DocumentBodyFactory.create(m);
    }

    private DocumentBody createDocumentBody(String name) {
        return createDocumentBody(name, System.currentTimeMillis());
    }
}
