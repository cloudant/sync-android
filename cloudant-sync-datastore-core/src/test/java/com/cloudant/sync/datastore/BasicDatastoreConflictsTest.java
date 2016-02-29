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

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.hasSize;

import com.cloudant.sync.util.CouchUtils;
import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BasicDatastoreConflictsTest extends BasicDatastoreTestBase {

    @Test
    public void getConflictedDocumentIds_oneConflictWithTwoConflictedLeafs() throws Exception {
        DocumentRevision rev = this.createDocumentRevision("Tom");
        DocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "1-rev", "Jerry");
        this.datastore.forceInsert(newRev, "1-rev");
        Iterator<String> iterator = this.datastore.getConflictedDocumentIds();
        List<String> conflictedDocId = Lists.newArrayList(iterator);
        Assert.assertThat(conflictedDocId, hasSize(1));
        Assert.assertThat(conflictedDocId, hasItems(rev.getId()));
    }


    @Test
    public void getConflictedDocumentIds_conflictWithThreeConflictedLeafs() throws Exception {
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
    public void getConflictedDocumentIds_oneDeletedLeafAndOneLiveLeaf_conflicts() throws  Exception {
        DocumentRevision rev = this.createDocumentRevision("Tom");
        this.datastore.deleteDocumentFromRevision(rev);
        DocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "4-a", "Jerry");
        this.datastore.forceInsert(newRev, "1-a", "2-a", "3-a", "4-a");

        Iterator<String> iterator = this.datastore.getConflictedDocumentIds();
        List<String> conflictedDocId = Lists.newArrayList(iterator);
        Assert.assertThat(conflictedDocId, hasSize(0));
    }

    @Test
    public void getConflictedDocumentIds_zeroConflicts() throws Exception {
        testWithConflictCount(0);
    }

    @Test
    public void getConflictedDocumentIds_twoConflict() throws Exception {
        testWithConflictCount(2);
    }

    @Test
    public void getConflictedDocumentIds_10Conflicts() throws Exception {
        testWithConflictCount(10);
    }

    @Test
    public void getConflictedDocumentIds_1000Conflicts() throws Exception {
        testWithConflictCount(1000);
    }

    @Test
    public void resolveConflictsForDocument_twoConflictAndException_nothing()
            throws Exception {
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
    public void resolveConflictThenResolveSecondConflict() throws Exception {
        String docId = this.createConflictedDocument();
        long expectedSequence = this.datastore.getLastSequence();
        this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
            @Override
            public DocumentRevision resolve(String docId, List<DocumentRevision> conflicts) {
                DocumentRevision newRev = conflicts.get(0);
                Map<String,Object>body = newRev.getBody().asMap();
                for(int i=1;i<conflicts.size();i++){
                    body.putAll(conflicts.get(i).getBody().asMap());
                }
                newRev.setBody(DocumentBodyFactory.create(body));
                return newRev;
            }

        });
        DocumentRevision newRev = this.createDetachedDocumentRevision(docId, "4-a", "Jerry");
        this.datastore.forceInsert(newRev, "3-a", "4-a");

        final List<DocumentRevision> conflictsList = new ArrayList<DocumentRevision>();
        ConflictResolver resolver = new ConflictResolver() {
            @Override
            public DocumentRevision resolve(String docId, List<DocumentRevision> conflicts) {
                conflictsList.addAll(conflicts);
                return null;
            }
        };

        this.datastore.resolveConflictsForDocument(docId, resolver);
        Assert.assertEquals(2,conflictsList.size());

    }

    @Test
    public void resolveConflictsForDocument_twoConflictAndReturnNull_nothing()
            throws Exception {
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
            throws Exception {
        String docId = this.createConflictedDocument();
        DocumentRevisionTree oldTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertTrue(oldTree.hasConflicts());
        // new sequence will be increased by 1 due to deleting one document
        long expectedSequence = this.datastore.getLastSequence() + 1;

        this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
            @Override
            public DocumentRevision resolve(String docId, List<DocumentRevision> conflicts) {
                Assert.assertEquals(2, conflicts.size());
                for(DocumentRevision rev : conflicts) {
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
        DocumentRevision newWinner = newTree.getCurrentRevision();
        Assert.assertEquals("Tom", newWinner.asMap().get("name"));
    }

    // attachments on the non-current document, check they get copied over when we select it
    @Test
    public void resolveConflictsForDocument_twoConflictAndNewWinner_newWinnerInsertedWithAttachments()
            throws Exception {
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
            public DocumentRevision resolve(String docId, List<DocumentRevision> conflicts) {
                Assert.assertEquals(2, conflicts.size());
                for(DocumentRevision rev : conflicts) {
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
        DocumentRevision newWinner = newTree.getCurrentRevision();
        Assert.assertEquals("Tom", newWinner.asMap().get("name"));
        Assert.assertNotNull(newWinner.getAttachments().get("att1"));
    }

    // attachments on the current document, check they don't get carried over
    @Test
    public void resolveConflictsForDocument_twoConflictAndNewWinner_newWinnerInsertedWithAttachments2()
            throws Exception {
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
            public DocumentRevision resolve(String docId, List<DocumentRevision> conflicts) {
                Assert.assertEquals(2, conflicts.size());
                for(DocumentRevision rev : conflicts) {
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
        DocumentRevision newWinner = newTree.getCurrentRevision();
        Assert.assertEquals("Tom", newWinner.asMap().get("name"));
        Assert.assertTrue(newWinner.getAttachments().isEmpty());
    }

    @Test
    public void resolveConflictsForDocument_twoConflictAndNewWinner_newWinnerInsertedMutableWithAttachments()
            throws Exception {
        String docId = this.createConflictedDocument();
        DocumentRevisionTree oldTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertTrue(oldTree.hasConflicts());
        long expectedSequence = this.datastore.getLastSequence() + 2;

        this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
            @Override
            public DocumentRevision resolve(String docId, List<DocumentRevision> conflicts) {
                Assert.assertEquals(2, conflicts.size());
                DocumentRevision rev = conflicts.get(0);
                rev.setBody(DocumentBodyFactory.create("{\"name\": \"mutable\"}".getBytes()));
                rev.getAttachments().put("att1", new UnsavedStreamAttachment(new ByteArrayInputStream("hello".getBytes()), "att1", "text/plain"));
                return rev;
            }
        });
        long actualSequence = this.datastore.getLastSequence();
        Assert.assertEquals(expectedSequence, actualSequence);

        DocumentRevisionTree newTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertFalse(newTree.hasConflicts());
        DocumentRevision newWinner = newTree.getCurrentRevision();
        Assert.assertEquals("mutable", newWinner.asMap().get("name"));
        Assert.assertNotNull(newWinner.getAttachments().get("att1"));
    }

    // test to ensure correct failure mode when user returns a new ('unrooted') mutable document
    @Test
    public void resolveConflictsForDocument_twoConflictAndNewWinner_newWinnerInsertedNewMutableFails()
            throws  Exception {
        String docId = this.createConflictedDocument();
        DocumentRevisionTree oldTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertTrue(oldTree.hasConflicts());

        DocumentRevision oldWinner = oldTree.getCurrentRevision();
        Assert.assertEquals("Jerry", oldWinner.asMap().get("name"));
        Assert.assertFalse(oldWinner.isDeleted());

        // not updated
        long expectedSequence = this.datastore.getLastSequence();

        try {
            // should throw IllegalArgumentException because sourceRevId is null
            this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
                @Override
                public DocumentRevision resolve(String docId, List<DocumentRevision> conflicts) {
                    Assert.assertEquals(2, conflicts.size());
                    DocumentRevision rev = new DocumentRevision();
                    rev.setBody(DocumentBodyFactory.create("{\"name\": \"mutable\"}".getBytes()));
                    rev.getAttachments().put("att1", new UnsavedStreamAttachment(new ByteArrayInputStream("hello".getBytes()), "att1", "text/plain"));
                    return rev;
                }
            });
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            // check we got the right message
            Assert.assertTrue(iae.getMessage().contains("must have a revision id"));
        }
        long actualSequence = this.datastore.getLastSequence();
        Assert.assertEquals(expectedSequence, actualSequence);

        DocumentRevisionTree newTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertTrue(newTree.hasConflicts());
        DocumentRevision newWinner = newTree.getCurrentRevision();
        Assert.assertEquals("Jerry", newWinner.asMap().get("name"));
        Assert.assertFalse(newWinner.isDeleted());
    }

    @Test
    public void resolveConflictsForDocument_threeConflictAndNewWinner_newWinnerInserted()
            throws Exception {
        String docId = this.createConflictedDocumentWithThreeLeafs();
        DocumentRevisionTree oldTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertTrue(oldTree.hasConflicts());
        // new sequence will be increased by 2 due to deleting 2 documents
        long expectedSequence = this.datastore.getLastSequence() + 2;

        this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
            @Override
            public DocumentRevision resolve(String docId, List<DocumentRevision> conflicts) {
                Assert.assertEquals(3, conflicts.size());
                for(DocumentRevision rev : conflicts) {
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
        DocumentRevision newWinner = newTree.getCurrentRevision();
        Assert.assertEquals("Tom", newWinner.asMap().get("name"));
    }

    @Test
    public void resolveConflictsForDocument_ensureNoDeletedRevsAreMarkedConflicted() throws  Exception {
        //this test ensures that the documents given to the document resolver are not deleted
        // since deleted docs are not in conflict
        String docId = this.createConflictedDocumentWithThreeLeafsOneDeleted();
        DocumentRevisionTree oldTree = this.datastore.getAllRevisionsOfDocument(docId);
        Assert.assertTrue(oldTree.hasConflicts());
        // new sequence will be increased by 2 due to deleting 2 documents
        long expectedSequence = this.datastore.getLastSequence() + 2;

        this.datastore.resolveConflictsForDocument(docId, new ConflictResolver() {
            @Override
            public DocumentRevision resolve(String docId, List<DocumentRevision> conflicts) {
                Assert.assertEquals(2, conflicts.size());
                for(DocumentRevision rev : conflicts) {
                    Assert.assertFalse(rev.isDeleted());
                }
                return null; //make it in conflict still
            }
        });
    }

    @Test
    public void resolveConflictsForDocument_timestampBasedResolver_revisionWithLatestTimestampWins()
            throws Exception {
        long ts = System.currentTimeMillis();
        DocumentBody body1 = this.createDocumentBody("Tom", ts);
        DocumentRevision revMut = new DocumentRevision();
        revMut.setBody(body1);
        DocumentRevision rev = this.datastore.createDocumentFromRevision(revMut);
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
        // last (by timestamp) to be inserted was Carl, 2-b
        Assert.assertEquals(Integer.valueOf(2), Integer.valueOf(generation));
    }

    @Test
    public void resurrectWinningDocument() throws Exception {
        // create n conflicted docs, delete winner, then create another
        DocumentRevision rev = this.createDocumentRevision("Tom");
        int count = 10;
        // create child docs 2-a, 2-c,...
        for (int i=0; i<count; i++) {
            DocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), (String.format("2-%c",'a'+i)), "Jerry");
            this.datastore.forceInsert(newRev, rev.getRevision(), newRev.getRevision());
            DocumentRevision retrieved = this.datastore.getDocument(newRev.getId(), newRev.getRevision());
        }
        DocumentRevision winner = this.datastore.getDocument(rev.getId());
        DocumentRevision deleted = this.datastore.deleteDocumentFromRevision(winner);
        DocumentRevision newRev = new DocumentRevision(rev.getId());
        newRev.setBody(DocumentBodyFactory.create("{\"data\": \"I am the resurrection\"}".getBytes()));
        DocumentRevision resurrected = this.datastore.createDocumentFromRevision(newRev);
        // 1 -> 2{a,b,c,...} -> 3 (deleted) -> 4
        Assert.assertEquals(4, ((DocumentRevision)resurrected).getGeneration());
        // check that 'resurrected' doc's parent is the deleted one
        Assert.assertEquals(((DocumentRevision)deleted).getSequence(), ((DocumentRevision)resurrected).getParent());
    }

    @Test(expected = DocumentException.class)
    public void resurrectWinningDocumentFails() throws Exception {
        // as above, but a non-winning rev is marked deleted
        DocumentRevision rev = this.createDocumentRevision("Tom");
        int count = 10;
        // create child docs 2-a, 2-c,...
        for (int i=0; i<count; i++) {
            DocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), (String.format("2-%c",'a'+i)), "Jerry");
            this.datastore.forceInsert(newRev, rev.getRevision(), newRev.getRevision());
            DocumentRevision retrieved = this.datastore.getDocument(newRev.getId(), newRev.getRevision());
        }
        // first guaranteed to be non-winner since winner is last to sort lexigraphically
        DocumentRevision nonWinner = this.datastore.getAllRevisionsOfDocument(rev.getId()).leafRevisions().get(0);
        DocumentRevision deletedNonWinner = this.datastore.deleteDocumentFromRevision(nonWinner);

        DocumentRevision newRev = new DocumentRevision(rev.getId());
        newRev.setBody(DocumentBodyFactory.create("{\"data\": \"I am the resurrection\"}".getBytes()));
        DocumentRevision resurrected = this.datastore.createDocumentFromRevision(newRev);
    }

    private void testWithConflictCount(int conflictCount) throws Exception {
        List<String> expectedConflicts = createConflictDocuments(conflictCount);
        Iterator<String> iterator = this.datastore.getConflictedDocumentIds();
        List<String> actualConflicts = Lists.newArrayList(iterator);
        Assert.assertThat(actualConflicts, hasSize(expectedConflicts.size()));
        for(String id : expectedConflicts) {
            Assert.assertThat(actualConflicts, hasItem(id));
        }
    }

    private List<String> createConflictDocuments(int conflictCount) throws Exception{
        List<String> expectedConflicts = new ArrayList<String>();
        for(int i = 0 ; i <  conflictCount; i ++) {
            String id = createConflictedDocument();
            expectedConflicts.add(id);
        }
        return expectedConflicts;
    }

    private String createConflictedDocument() throws Exception {
        DocumentRevision rev = this.createDocumentRevision("Tom");
        DocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "2-a", "Jerry");
        this.datastore.forceInsert(newRev, "1-a", "2-a");
        return rev.getId();
    }

    private String createConflictedDocumentWithAttachments() throws Exception {
        DocumentRevision rev = this.createDocumentRevisionWithAttachment("Tom");
        DocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "2-a", "Jerry");
        this.datastore.forceInsert(newRev, "1-a", "2-a");
        return rev.getId();
    }

    // attachments on the 'winning' side
    private String createConflictedDocumentWithAttachmentsWinning() throws Exception {
        DocumentRevision rev = this.createDocumentRevision("Tom");
        DocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "2-a", "Jerry");
        this.datastore.forceInsert(newRev, "1-a", "2-a");
        DocumentRevision current = this.datastore.getDocument(rev.getId());
        DocumentRevision rev2 = this.updateDocumentRevisionWithAttachment(rev.getId(), current.getRevision(), "Jerry With Attachments");
        return rev.getId();
    }


    private String createConflictedDocumentWithThreeLeafs() throws Exception {
        DocumentRevision rev = this.createDocumentRevision("Tom");
        DocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "2-a", "Jerry");
        this.datastore.forceInsert(newRev, "1-a", "2-a");
        DocumentRevision newRev2 = this.createDetachedDocumentRevision(rev.getId(), "4-b", "Carl");
        this.datastore.forceInsert(newRev2, "1-b", "2-b", "3-b", "4-b");
        return rev.getId();
    }

    private String createConflictedDocumentWithThreeLeafsOneDeleted() throws Exception {
        DocumentRevision rev = this.createDocumentRevision("Tom");
        this.datastore.deleteDocumentFromRevision(rev);
        DocumentRevision newRev = this.createDetachedDocumentRevision(rev.getId(), "2-a", "Jerry");
        this.datastore.forceInsert(newRev, "1-a", "2-a");
        DocumentRevision newRev2 = this.createDetachedDocumentRevision(rev.getId(), "4-b", "Carl");
        this.datastore.forceInsert(newRev2, "1-b", "2-b", "3-b", "4-b");
        return rev.getId();
    }


    private DocumentRevision createDocumentRevision(String name) throws Exception {
        DocumentBody body = createDocumentBody(name);
        DocumentRevision rev = new DocumentRevision();
        rev.setBody(body);
        return this.datastore.createDocumentFromRevision(rev);
    }

    private DocumentRevision createDocumentRevisionWithAttachment(String name) throws Exception {
        DocumentRevision rev = new DocumentRevision();
        rev.setBody(createDocumentBody(name));
        rev.getAttachments().put("att1", new UnsavedStreamAttachment(new ByteArrayInputStream("hello".getBytes()), "att1", "text/plain"));
        return this.datastore.createDocumentFromRevision(rev);
    }

    private DocumentRevision updateDocumentRevisionWithAttachment(String docId, String revId, String name) throws Exception {
        DocumentRevision rev = new DocumentRevision(docId, revId);
        rev.setBody(createDocumentBody(name));
        rev.getAttachments().put("att1", new UnsavedStreamAttachment(new ByteArrayInputStream("hello".getBytes()), "att1", "text/plain"));
        return this.datastore.updateDocumentFromRevision(rev);
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
