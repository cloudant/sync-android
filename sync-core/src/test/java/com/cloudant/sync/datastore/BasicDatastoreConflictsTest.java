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

import java.io.ByteArrayInputStream;
import java.io.IOException;
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
    public void getConflictedDocumentIds_oneConflictWithTwoConflictedLeafs() throws IOException {
        BasicDocumentRevision rev = this.createDocumentRevision("Tom");
        BasicDocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "1-rev", "Jerry");
        this.datastore.forceInsert(newRev, "1-rev");
        Iterator<String> iterator = this.datastore.getConflictedDocumentIds();
        List<String> conflictedDocId = Lists.newArrayList(iterator);
        Assert.assertThat(conflictedDocId, hasSize(1));
        Assert.assertThat(conflictedDocId, hasItems(rev.getId()));
    }


    @Test
    public void getConflictedDocumentIds_conflictWithThreeConflictedLeafs() throws IOException {
        BasicDocumentRevision rev = this.createDocumentRevision("Tom");
        BasicDocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "4-a", "Jerry");
        this.datastore.forceInsert(newRev, "1-a", "2-a", "3-a", "4-a");
        BasicDocumentRevision newRev2 = this.createDetachedDocumentRevision(rev.getId(), "3-b", "Harry");
        this.datastore.forceInsert(newRev2, "1-b", "2-b", "3-b");

        Iterator<String> iterator = this.datastore.getConflictedDocumentIds();
        List<String> conflictedDocId = Lists.newArrayList(iterator);
        Assert.assertThat(conflictedDocId, hasSize(1));
        Assert.assertThat(conflictedDocId, hasItems(rev.getId()));
    }

    @Test
    public void getConflictedDocumentIds_oneDeletedLeafAndOneLiveLeaf_conflicts() throws ConflictException, IOException {
        BasicDocumentRevision rev = this.createDocumentRevision("Tom");
        this.datastore.deleteDocumentFromRevision(rev);
        BasicDocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "4-a", "Jerry");
        this.datastore.forceInsert(newRev, "1-a", "2-a", "3-a", "4-a");

        Iterator<String> iterator = this.datastore.getConflictedDocumentIds();
        List<String> conflictedDocId = Lists.newArrayList(iterator);
        Assert.assertThat(conflictedDocId, hasSize(0));
    }

    @Test
    public void getConflictedDocumentIds_zeroConflicts() throws IOException {
        testWithConflictCount(0);
    }

    @Test
    public void getConflictedDocumentIds_twoConflict() throws IOException {
        testWithConflictCount(2);
    }

    @Test
    public void getConflictedDocumentIds_10Conflicts() throws IOException {
        testWithConflictCount(10);
    }

    @Test
    public void getConflictedDocumentIds_1000Conflicts() throws IOException {
        testWithConflictCount(1000);
    }

    @Test
    public void resolveConflictsForDocument_twoConflictAndException_nothing()
            throws ConflictException, IOException {
        String docId = this.createConflictedDocument();
        long expectedSequence = this.datastore.getLastSequence();
        this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
            @Override
            public BasicDocumentRevision resolve(String docId, List<BasicDocumentRevision> conflicts) {
                throw new IllegalStateException("Mocked error");
            }
        });
        long actualSequence = this.datastore.getLastSequence();
        Assert.assertEquals(expectedSequence, actualSequence);
    }

    @Test
    public void resolveConflictsForDocument_twoConflictAndReturnNull_nothing()
            throws ConflictException, IOException {
        String docId = this.createConflictedDocument();
        long expectedSequence = this.datastore.getLastSequence();
        this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
            @Override
            public BasicDocumentRevision resolve(String docId, List<BasicDocumentRevision> conflicts) {
                return null;
            }
        });
        long actualSequence = this.datastore.getLastSequence();
        Assert.assertEquals(expectedSequence, actualSequence);
    }

    @Test
    public void resolveConflictsForDocument_twoConflictAndNewWinner_newWinnerInserted()
            throws ConflictException, IOException {
        String docId = this.createConflictedDocument();
        DocumentRevisionTree oldTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertTrue(oldTree.hasConflicts());
        // new sequence will be increased by 1 due to deleting one document
        long expectedSequence = this.datastore.getLastSequence() + 1;

        this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
            @Override
            public BasicDocumentRevision resolve(String docId, List<BasicDocumentRevision> conflicts) {
                Assert.assertEquals(2, conflicts.size());
                for(BasicDocumentRevision rev : conflicts) {
                    if (rev.asMap().get("name").equals("Tom")) {
                        return rev;
                    }
                }
                return null;
            }
        });
        long actualSequence = this.datastore.getLastSequence();
        Assert.assertEquals(expectedSequence, actualSequence);

        DocumentRevisionTree newTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertFalse(newTree.hasConflicts());
        BasicDocumentRevision newWinner = newTree.getCurrentRevision();
        Assert.assertEquals("Tom", newWinner.asMap().get("name"));
    }

    // attachments on the non-current document, check they get copied over when we select it
    @Test
    public void resolveConflictsForDocument_twoConflictAndNewWinner_newWinnerInsertedWithAttachments()
            throws ConflictException, IOException {
        String docId = this.createConflictedDocumentWithAttachments();
        DocumentRevisionTree oldTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertTrue(oldTree.hasConflicts());
        // new sequence will be increased by 1 due to deleting one document
        long expectedSequence = this.datastore.getLastSequence() + 1;

        // check the winner is the one without attachments
        Assert.assertEquals("Jerry", this.datastore.getDocument(docId).asMap().get("name"));
        Assert.assertTrue(this.datastore.getDocument(docId).getAttachments().isEmpty());

        this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
            @Override
            public BasicDocumentRevision resolve(String docId, List<BasicDocumentRevision> conflicts) {
                Assert.assertEquals(2, conflicts.size());
                for(BasicDocumentRevision rev : conflicts) {
                    if (rev.asMap().get("name").equals("Tom")) {
                        return rev;
                    }
                }
                return null;
            }
        });
        long actualSequence = this.datastore.getLastSequence();
        Assert.assertEquals(expectedSequence, actualSequence);

        DocumentRevisionTree newTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertFalse(newTree.hasConflicts());
        BasicDocumentRevision newWinner = newTree.getCurrentRevision();
        Assert.assertEquals("Tom", newWinner.asMap().get("name"));
        Assert.assertNotNull(newWinner.getAttachments().get("att1"));
    }

    // attachments on the current document, check they don't get carried over
    @Test
    public void resolveConflictsForDocument_twoConflictAndNewWinner_newWinnerInsertedWithAttachments2()
            throws ConflictException, IOException {
        String docId = this.createConflictedDocumentWithAttachmentsWinning();
        DocumentRevisionTree oldTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertTrue(oldTree.hasConflicts());
        // new sequence will be increased by 1 due to deleting one document
        long expectedSequence = this.datastore.getLastSequence() + 1;

        // check the winner is the one with attachments
        Assert.assertEquals("Jerry With Attachments", this.datastore.getDocument(docId).asMap().get("name"));
        Assert.assertEquals(1, this.datastore.getDocument(docId).getAttachments().size());

        this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
            @Override
            public BasicDocumentRevision resolve(String docId, List<BasicDocumentRevision> conflicts) {
                Assert.assertEquals(2, conflicts.size());
                for(BasicDocumentRevision rev : conflicts) {
                    if (rev.asMap().get("name").equals("Tom")) {
                        return rev;
                    }
                }
                return null;
            }
        });
        long actualSequence = this.datastore.getLastSequence();
        Assert.assertEquals(expectedSequence, actualSequence);

        DocumentRevisionTree newTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertFalse(newTree.hasConflicts());
        BasicDocumentRevision newWinner = newTree.getCurrentRevision();
        Assert.assertEquals("Tom", newWinner.asMap().get("name"));
        Assert.assertTrue(newWinner.getAttachments().isEmpty());
    }

    @Test
    public void resolveConflictsForDocument_twoConflictAndNewWinner_newWinnerInsertedMutableWithAttachments()
            throws ConflictException, IOException {
        String docId = this.createConflictedDocument();
        DocumentRevisionTree oldTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertTrue(oldTree.hasConflicts());
        long expectedSequence = this.datastore.getLastSequence() + 2;

        this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
            @Override
            public DocumentRevision resolve(String docId, List<BasicDocumentRevision> conflicts) {
                Assert.assertEquals(2, conflicts.size());
                MutableDocumentRevision rev = conflicts.get(0).mutableCopy();
                rev.body = DocumentBodyFactory.create("{\"name\": \"mutable\"}".getBytes());
                rev.attachments.put("att1", new UnsavedStreamAttachment(new ByteArrayInputStream("hello".getBytes()), "att1", "text/plain"));
                return rev;
            }
        });
        long actualSequence = this.datastore.getLastSequence();
        Assert.assertEquals(expectedSequence, actualSequence);

        DocumentRevisionTree newTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertFalse(newTree.hasConflicts());
        BasicDocumentRevision newWinner = newTree.getCurrentRevision();
        Assert.assertEquals("mutable", newWinner.asMap().get("name"));
        Assert.assertNotNull(newWinner.getAttachments().get("att1"));
    }

    // test to ensure correct failure mode when user returns a new ('unrooted') mutable document
    @Test
    public void resolveConflictsForDocument_twoConflictAndNewWinner_newWinnerInsertedNewMutableFails()
            throws ConflictException, IOException {
        String docId = this.createConflictedDocument();
        DocumentRevisionTree oldTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertTrue(oldTree.hasConflicts());

        BasicDocumentRevision oldWinner = oldTree.getCurrentRevision();
        Assert.assertEquals("Jerry", oldWinner.asMap().get("name"));
        Assert.assertFalse(oldWinner.isDeleted());

        // not updated
        long expectedSequence = this.datastore.getLastSequence();

        try {
            // should throw IllegalArgumentException because sourceRevId is null
            this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
                @Override
                public DocumentRevision resolve(String docId, List<BasicDocumentRevision> conflicts) {
                    Assert.assertEquals(2, conflicts.size());
                    MutableDocumentRevision rev = new MutableDocumentRevision();
                    rev.body = DocumentBodyFactory.create("{\"name\": \"mutable\"}".getBytes());
                    rev.attachments.put("att1", new UnsavedStreamAttachment(new ByteArrayInputStream("hello".getBytes()), "att1", "text/plain"));
                    return rev;
                }
            });
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            ;
        }
        long actualSequence = this.datastore.getLastSequence();
        Assert.assertEquals(expectedSequence, actualSequence);

        DocumentRevisionTree newTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertTrue(newTree.hasConflicts());
        BasicDocumentRevision newWinner = newTree.getCurrentRevision();
        Assert.assertEquals("Jerry", newWinner.asMap().get("name"));
        Assert.assertFalse(newWinner.isDeleted());
    }

    @Test
    public void resolveConflictsForDocument_threeConflictAndNewWinner_newWinnerInserted()
            throws ConflictException, IOException {
        String docId = this.createConflictedDocumentWithThreeLeafs();
        DocumentRevisionTree oldTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertTrue(oldTree.hasConflicts());
        // new sequence will be increased by 2 due to deleting 2 documents
        long expectedSequence = this.datastore.getLastSequence() + 2;

        this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
            @Override
            public BasicDocumentRevision resolve(String docId, List<BasicDocumentRevision> conflicts) {
                Assert.assertEquals(3, conflicts.size());
                for(BasicDocumentRevision rev : conflicts) {
                    if (rev.asMap().get("name").equals("Tom")) {
                        return rev;
                    }
                }
                return null;
            }
        });
        long actualSequence = this.datastore.getLastSequence();
        Assert.assertEquals(expectedSequence, actualSequence);

        DocumentRevisionTree newTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertFalse(newTree.hasConflicts());
        BasicDocumentRevision newWinner = newTree.getCurrentRevision();
        Assert.assertEquals("Tom", newWinner.asMap().get("name"));
    }

    @Test
    public void resolveConflictsForDocument_threeConflictAndNewWinnerAsDeleted_documentDeleted()
            throws ConflictException, IOException {
        String docId = this.createConflictedDocumentWithThreeLeafsOneDeleted();
        DocumentRevisionTree oldTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertTrue(oldTree.hasConflicts());
        // new sequence will be increased by 2 due to deleting 2 documents
        long expectedSequence = this.datastore.getLastSequence() + 2;

        this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
            @Override
            public BasicDocumentRevision resolve(String docId, List<BasicDocumentRevision> conflicts) {
                Assert.assertEquals(3, conflicts.size());
                for(BasicDocumentRevision rev : conflicts) {
                    if (rev.getSequence() == 2) { // this was "name: Tom" but was deleted
                        return rev;
                    }
                }
                return null;
            }
        });
        long actualSequence = this.datastore.getLastSequence();
        Assert.assertEquals(expectedSequence, actualSequence);

        DocumentRevisionTree newTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertFalse(newTree.hasConflicts());

        BasicDocumentRevision newWinner = newTree.getCurrentRevision();
        Assert.assertTrue(newWinner.isDeleted());
    }

    @Test
    public void resolveConflictsForDocument_timestampBasedResolver_revisionWithLatestTimestampWins()
            throws ConflictException, IOException {
        long ts = System.currentTimeMillis();
        DocumentBody body1 = this.createDocumentBody("Tom", ts);
        MutableDocumentRevision revMut = new MutableDocumentRevision();
        revMut.body = body1;
        BasicDocumentRevision rev = this.datastore.createDocumentFromRevision(revMut);
        BasicDocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "4-a", "Jerry", ts + 1);
        this.datastore.forceInsert(newRev, "1-a", "2-a", "3-a", "4-a");
        BasicDocumentRevision newRev2 = this.createDetachedDocumentRevision(rev.getId(), "2-b", "Carl", ts + 2);
        this.datastore.forceInsert(newRev2, "1-b", "2-b");

        BasicDocumentRevision oldWinner = this.datastore.getDocument(rev.getId());
        Assert.assertEquals("4-a", oldWinner.getRevision());
        Assert.assertEquals("Jerry", oldWinner.getBody().asMap().get("name"));

        this.datastore.resolveConflictsForDocument(rev.getId(), new TimestampBasedConflictsResolver());

        BasicDocumentRevision newWinner = this.datastore.getDocument(rev.getId());
        Assert.assertEquals("Carl", newWinner.asMap().get("name"));
        int generation = CouchUtils.generationFromRevId(newWinner.getRevision());
        // last (by timestamp) to be inserted was Carl, 2-b
        Assert.assertEquals(Integer.valueOf(2), Integer.valueOf(generation));
    }

    private void testWithConflictCount(int conflictCount) throws IOException {
        List<String> expectedConflicts = createConflictDocuments(conflictCount);
        Iterator<String> iterator = this.datastore.getConflictedDocumentIds();
        List<String> actualConflicts = Lists.newArrayList(iterator);
        Assert.assertThat(actualConflicts, hasSize(expectedConflicts.size()));
        for(String id : expectedConflicts) {
            Assert.assertThat(actualConflicts, hasItem(id));
        }
    }

    private List<String> createConflictDocuments(int conflictCount) throws IOException{
        List<String> expectedConflicts = new ArrayList<String>();
        for(int i = 0 ; i <  conflictCount; i ++) {
            String id = createConflictedDocument();
            expectedConflicts.add(id);
        }
        return expectedConflicts;
    }

    private String createConflictedDocument() throws IOException {
        BasicDocumentRevision rev = this.createDocumentRevision("Tom");
        BasicDocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "2-a", "Jerry");
        this.datastore.forceInsert(newRev, "1-a", "2-a");
        return rev.getId();
    }

    private String createConflictedDocumentWithAttachments() throws IOException {
        BasicDocumentRevision rev = this.createDocumentRevisionWithAttachment("Tom");
        BasicDocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "2-a", "Jerry");
        this.datastore.forceInsert(newRev, "1-a", "2-a");
        return rev.getId();
    }

    // attachments on the 'winning' side
    private String createConflictedDocumentWithAttachmentsWinning() throws IOException, ConflictException {
        BasicDocumentRevision rev = this.createDocumentRevision("Tom");
        BasicDocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "2-a", "Jerry");
        this.datastore.forceInsert(newRev, "1-a", "2-a");
        BasicDocumentRevision current = this.datastore.getDocument(rev.getId());
        BasicDocumentRevision rev2 = this.updateDocumentRevisionWithAttachment(rev.getId(), current.getRevision(), "Jerry With Attachments");
        return rev.getId();
    }


    private String createConflictedDocumentWithThreeLeafs() throws IOException {
        BasicDocumentRevision rev = this.createDocumentRevision("Tom");
        BasicDocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "2-a", "Jerry");
        this.datastore.forceInsert(newRev, "1-a", "2-a");
        BasicDocumentRevision newRev2 = this.createDetachedDocumentRevision(rev.getId(), "4-b", "Carl");
        this.datastore.forceInsert(newRev2, "1-b", "2-b", "3-b", "4-b");
        return rev.getId();
    }

    private String createConflictedDocumentWithThreeLeafsOneDeleted() throws ConflictException, IOException{
        BasicDocumentRevision rev = this.createDocumentRevision("Tom");
        this.datastore.deleteDocumentFromRevision(rev);
        BasicDocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "2-a", "Jerry");
        this.datastore.forceInsert(newRev, "1-a", "2-a");
        BasicDocumentRevision newRev2 = this.createDetachedDocumentRevision(rev.getId(), "4-b", "Carl");
        this.datastore.forceInsert(newRev2, "1-b", "2-b", "3-b", "4-b");
        return rev.getId();
    }


    private BasicDocumentRevision createDocumentRevision(String name) throws IOException {
        DocumentBody body = createDocumentBody(name);
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.body = body;
        return this.datastore.createDocumentFromRevision(rev);
    }

    private BasicDocumentRevision createDocumentRevisionWithAttachment(String name) throws IOException {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.body = createDocumentBody(name);
        rev.attachments.put("att1", new UnsavedStreamAttachment(new ByteArrayInputStream("hello".getBytes()), "att1", "text/plain"));
        return this.datastore.createDocumentFromRevision(rev);
    }

    private BasicDocumentRevision updateDocumentRevisionWithAttachment(String docId, String revId, String name) throws ConflictException, IOException {
        MutableDocumentRevision rev = new MutableDocumentRevision(revId);
        rev.docId = docId;
        rev.body = createDocumentBody(name);
        rev.attachments.put("att1", new UnsavedStreamAttachment(new ByteArrayInputStream("hello".getBytes()), "att1", "text/plain"));
        return this.datastore.updateDocumentFromRevision(rev);
    }

    private BasicDocumentRevision createDetachedDocumentRevision(String docId, String rev, String name) {
        return createDetachedDocumentRevision(docId, rev, name, System.currentTimeMillis());
    }

    private BasicDocumentRevision createDetachedDocumentRevision(String docId, String rev, String name, long ts) {
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
