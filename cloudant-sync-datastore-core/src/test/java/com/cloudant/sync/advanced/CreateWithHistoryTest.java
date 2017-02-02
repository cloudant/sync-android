package com.cloudant.sync.advanced;

import static com.cloudant.sync.advanced.CreateWithHistoryTest.TestDoc.REV1;
import static com.cloudant.sync.advanced.CreateWithHistoryTest.TestDoc.REV2;
import static com.cloudant.sync.advanced.CreateWithHistoryTest.TestDoc.REV3;

import com.cloudant.common.AdvancedAPITest;
import com.cloudant.sync.documentstore.Attachment;
import com.cloudant.sync.documentstore.ConflictResolver;
import com.cloudant.sync.documentstore.Database;
import com.cloudant.sync.documentstore.DocumentBody;
import com.cloudant.sync.documentstore.DocumentBodyFactory;
import com.cloudant.sync.documentstore.DocumentException;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.documentstore.UnsavedFileAttachment;
import com.cloudant.sync.documentstore.UnsavedStreamAttachment;
import com.cloudant.sync.internal.common.CouchUtils;
import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.internal.documentstore.DocumentRevisionTree;
import com.cloudant.sync.internal.documentstore.InternalDocumentRevision;
import com.cloudant.sync.util.TestUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test creating document revisions with history. We use an example revision 3-ghi with a history
 * 1-abc
 * 2-def
 * 3-ghi
 */
public class CreateWithHistoryTest extends AdvancedAPITest {

    enum TestDoc {

        REV1(1, "abc"),
        REV2(2, "def"),
        REV3(3, "ghi");

        static final String ID = "testdoc";
        final DocumentRevision revision;
        final int generation;
        final DocumentBody body;

        TestDoc(int generation, String hash) {
            this.generation = generation;
            this.revision = new DocumentRevision(TestDoc.ID, generation + "-" + hash);
            this.body = DocumentBodyFactory.create(Collections.singletonMap
                    ("number_" + generation, "property_" + hash));
            revision.setBody(body);
        }

        private DocumentRevision makeUnversioned() {
            DocumentRevision rev = new DocumentRevision(ID);
            rev.setBody(body);
            return rev;
        }
    }

    private Database database;

    @Before
    public void initDatabase() {
        database = documentStore.database();
    }

    /**
     * Utility to create some document revisions with bodies matching the TestDoc pattern
     *
     * @param generations number of generations to create between 1 and 3
     * @return
     * @throws Exception
     */
    private List<DocumentRevision> createRevisions(int generations) throws Exception {
        return createRevisions(generations, false);
    }

    /**
     * Utility to create some document revisions with bodies matching the TestDoc pattern
     *
     * @param generations number of generations to create between 1 and 3
     * @return
     * @throws Exception
     */
    private List<DocumentRevision> createRevisions(int generations, boolean withAttachments)
            throws Exception {
        if (generations > 0 && generations <= 3) {
            List<DocumentRevision> created = new ArrayList<DocumentRevision>();
            for (int i = 0; i < generations; i++) {
                DocumentRevision rev;
                switch (i) {
                    case 0:
                        // First time we need to create
                        rev = TestDoc.values()[i].makeUnversioned();
                        if (withAttachments) {
                            rev.setAttachments(addAttachment(null, i + 1));
                        }
                        rev = database.create(rev);
                        break;
                    default:
                        // Use the previous rev to update
                        DocumentRevision previous = created.get(i - 1);
                        rev = new DocumentRevision(previous.getId(), previous.getRevision());
                        rev.setBody(TestDoc.values()[i].body); // Set the new body
                        if (withAttachments) {
                            rev.setAttachments(addAttachment(previous.getAttachments(), i + 1));
                        }
                        rev = database.update(rev);
                        break;
                }
                created.add(rev);
            }
            return created;
        } else {
            throw new IllegalArgumentException("This test only supports creating up to 3 " +
                    "generations");
        }
    }

    /**
     * Utility to add attachments to an existing map, or create a new map
     *
     * @param existing
     * @param attachmentNumber
     * @return
     */
    private Map<String, Attachment> addAttachment(Map<String, Attachment> existing, int
            attachmentNumber) {
        if (attachmentNumber < 0 || attachmentNumber > 2) {
            throw new IllegalArgumentException("Only attachment number 1 or 2 are valid");
        }
        String attName = "attachment_" + attachmentNumber + ".txt";

        File a = TestUtils.loadFixture("fixture/" + attName);

        Attachment attachment = new UnsavedFileAttachment(a, "text/plain");
        if (existing == null) {
            return Collections.singletonMap(attName, attachment);
        } else {
            existing.put(attName, attachment);
            return existing;
        }
    }

    /**
     * Converts a TestDoc enum set into a collection of revisions
     *
     * @param testDocs set of TestDoc revisions in ascending order
     * @return
     */
    private List<DocumentRevision> toRevisionCollection(EnumSet<TestDoc> testDocs) {
        List<DocumentRevision> revisions = new ArrayList<DocumentRevision>(testDocs.size());
        for (TestDoc testDoc : testDocs) {
            revisions.add(testDoc.revision);
        }
        return revisions;
    }

    /**
     * Creates a _revisons.ids history list from an ascending order list of revisions
     *
     * @param revisions
     * @return
     */
    private List<String> toHistory(Collection<DocumentRevision> revisions) {
        List<String> history = new ArrayList<String>();
        for (DocumentRevision rev : revisions) {
            history.add(CouchUtils.getRevisionIdSuffix(rev.getRevision()));
        }
        Collections.reverse(history);
        return history;
    }

    /**
     * Assert that the test doc revisions are present
     *
     * @throws Exception
     */
    private void assertRevisionAndHistoryPresent(Collection<DocumentRevision> testDocRevisions)
            throws
            Exception {
        // Assert that the revision exists
        Assert.assertTrue("The document ID should be present in the document store", database
                .contains(TestDoc.ID));
        // Assert that the specified revisions are present
        for (DocumentRevision testDoc : testDocRevisions) {
            Assert.assertTrue("The revision should be present in the document store",
                    database.contains(TestDoc.ID, testDoc.getRevision()));
        }
    }

    /**
     * Asserts that the only revisions present for a document are the ones expected.
     * Note this method uses internal APIs.
     *
     * @param expectedRevs
     */
    private void assertNoUnexpectedRevs(Collection<DocumentRevision>... expectedRevs) {
        // What we expect
        Set<String> expected = new HashSet<String>();
        for (Collection<DocumentRevision> revs : expectedRevs) {
            for (DocumentRevision rev : revs) {
                expected.add(rev.getRevision());
            }
        }

        // What we can find
        Set<String> actual = new HashSet<String>();
        DocumentRevisionTree tree = ((DatabaseImpl) database).getAllRevisionsOfDocument(TestDoc.ID);
        for (InternalDocumentRevision rev : tree.leafRevisions()) {
            actual.addAll(tree.getPath(rev.getSequence()));
        }
        Assert.assertEquals("There should only be the expected revs", expected, actual);
    }

    /**
     * Method to assert the document revision content is what we expect
     *
     * @throws Exception
     */
    private void assertRevisionContent(Collection<DocumentRevision> expectedRevisions, boolean...
            expectStubs) throws
            Exception {
        int i = 0;
        for (DocumentRevision expectedRevision : expectedRevisions) {
            DocumentRevision readRevision = database.read(TestDoc.ID, expectedRevision
                    .getRevision());
            // Assert the document is present at the specified revision
            Assert.assertNotNull("The read revision should not be null", readRevision);
            // Assert the body matches what is expected
            DocumentBody expectedBody;
            if (expectStubs[i]) {
                expectedBody = DocumentBodyFactory.EMPTY;
            } else {
                expectedBody = expectedRevision.getBody();
            }
            Assert.assertEquals("The body should be as expected", expectedBody.asMap(), readRevision
                    .getBody().asMap());
            i++;
        }
    }

    /**
     * Asserts that the specified revisions are conflicting.
     *
     * @param revs
     * @throws Exception
     */
    private void assertConflictBetween(String... revs) throws Exception {
        Collection<String> conflictedIDs = new HashSet<String>();
        for (String conflicted : database.getConflictedIds()) {
            conflictedIDs.add(conflicted);
        }
        Assert.assertEquals("The testdoc should be conflicted (and the only conflicted ID)",
                Collections.singleton(TestDoc.ID), conflictedIDs);
        // Assert that the conflicted revisions are the ones specified
        AssertingConflictResolver resolver = new AssertingConflictResolver();
        database.resolveConflicts(TestDoc.ID, resolver);
        Set<String> expectedConflictingRevisionIDs = new HashSet<String>();
        expectedConflictingRevisionIDs.addAll(Arrays.asList(revs));
        Assert.assertEquals("The expected leaves should be conflicting",
                expectedConflictingRevisionIDs, resolver.conflictedRevisionIDs);
    }

    /**
     * Insert a new document revision with history.
     * Assert that the REV3 is present and the ancestor revs 1 and 2 are stubs.
     *
     * @throws Exception
     */
    @Test
    public void testCreateWithHistory() throws Exception {
        Collection<DocumentRevision> revisions = toRevisionCollection(EnumSet.allOf(TestDoc.class));

        // Create the revision with history
        advancedDatabase.createWithHistory(REV3.revision, REV3.generation, toHistory(revisions));

        // Assert
        assertRevisionAndHistoryPresent(revisions);
        assertRevisionContent(revisions, true, true, false); // Revs 1 and 2 are stubs
        assertNoUnexpectedRevs(revisions);
    }

    /**
     * Insert the document revision when part of the tree already exists (i.e. rev 1 exists).
     * Assert that rev3 is created, rev2 is inserted as a stub and rev1 is still complete.
     *
     * @throws Exception
     */
    @Test
    public void testExistingPartialAncestorTree() throws Exception {
        // Create a partial existing tree for the document
        List<DocumentRevision> ancestors = createRevisions(1);

        ancestors.add(REV2.revision);
        ancestors.add(REV3.revision);

        // Now create the revision with history
        advancedDatabase.createWithHistory(REV3.revision, REV3.generation, toHistory(ancestors));

        // Assert
        assertRevisionAndHistoryPresent(ancestors);
        assertRevisionContent(ancestors, false, true, false); // Only 2 should be a stub
        assertNoUnexpectedRevs(ancestors);
    }

    /**
     * Insert a document revision when all its ancestors exist (i.e. revs 1 and 2 exist).
     * Assert that rev3 is created and that revs 1 and 2 are still complete.
     *
     * @throws Exception
     */
    @Test
    public void testExistingCompleteAncestorTree() throws Exception {
        // Create an existing tree for the document, not including the rev we intend to insert
        List<DocumentRevision> ancestors = createRevisions(2);
        ancestors.add(REV3.revision);

        // Now create the revision with history
        advancedDatabase.createWithHistory(REV3.revision, REV3.generation, toHistory(ancestors));

        // Assert
        assertRevisionAndHistoryPresent(ancestors);
        assertRevisionContent(ancestors, false, false, false); // None should be stubs
        assertNoUnexpectedRevs(ancestors);
    }

    /**
     * Test inserting rev 3 with history 1 and 2 revs parallel to an existing tree of 3 different
     * revs. The trees are parallel from the first generation.
     * Assert that all 6 revisions are present after the createWithHistory and that a conflict
     * exists for the two latest generation docs.
     *
     * @throws Exception
     */
    @Test
    public void testRootConflict() throws Exception {
        List<DocumentRevision> parallelTree = createRevisions(3);
        List<DocumentRevision> testDocTree = toRevisionCollection(EnumSet.allOf(TestDoc.class));
        // Insert our root conflict
        advancedDatabase.createWithHistory(REV3.revision, REV3.generation, toHistory(testDocTree));

        // Assert both trees are good
        assertRevisionAndHistoryPresent(testDocTree);
        assertRevisionContent(testDocTree, true, true, false); // Revs 1 and 2 are stubs

        assertRevisionAndHistoryPresent(parallelTree);
        assertRevisionContent(parallelTree, false, false, false); // None are stubs

        assertNoUnexpectedRevs(testDocTree, parallelTree);

        assertConflictBetween(REV3.revision.getRevision(), parallelTree.get(2).getRevision());
    }

    /**
     * Similar to {@link #testRootConflict()} but now the revs 1 and 2 are identical for both trees,
     * they only diverge at the 3rd revision.
     * Assert that all 4 revisions are present after the createWithHistory and that a conflict
     * exists for the two latest generation docs.
     *
     * @throws Exception
     */
    @Test
    public void testLeafConflict() throws Exception {
        List<DocumentRevision> parallelTree = createRevisions(3);

        List<DocumentRevision> sharedHistory = Collections.list(Collections.enumeration
                (parallelTree.subList(0, 2))); //note sublist second index is *exclusive*
        sharedHistory.add(REV3.revision);

        advancedDatabase.createWithHistory(REV3.revision, REV3.generation, toHistory
                (sharedHistory));

        // Assert both trees are good
        assertRevisionAndHistoryPresent(Collections.singleton(REV3.revision));
        assertRevisionContent(Collections.singleton(REV3.revision), false); // Not a stub

        assertRevisionAndHistoryPresent(parallelTree);
        assertRevisionContent(parallelTree, false, false, false); // None are stubs

        assertNoUnexpectedRevs(Collections.singleton(REV3.revision), parallelTree);

        assertConflictBetween(REV3.revision.getRevision(), parallelTree.get(2).getRevision());
    }

    /**
     * Test that inserting a rev 3 with attachments and history works.
     * Assert that the revision is created along with stubs for revs 1 and 2 and that the
     * attachments are present.
     *
     * @throws Exception
     */
    @Test
    public void testAttachments() throws Exception {
        // Create a special rev3 we can add
        DocumentRevision rev3WithAttachments = new DocumentRevision(TestDoc.ID, REV3.revision
                .getRevision());
        rev3WithAttachments.setBody(REV3.body);
        // Add attachments
        File a1 = TestUtils.loadFixture("fixture/attachment_1.txt");
        File a2 = TestUtils.loadFixture("fixture/attachment_2.txt");
        Map<String, Attachment> attachments = new HashMap<String, Attachment>();
        Attachment att1 = new UnsavedFileAttachment(a1, "text/plain");
        Attachment att2 = new UnsavedStreamAttachment(new FileInputStream(a2), "text/plain");
        attachments.put("attachment_1.txt", att1);
        attachments.put("attachment_2.txt", att2);
        rev3WithAttachments.setAttachments(attachments);

        List<DocumentRevision> tree = toRevisionCollection(EnumSet.of(REV1, REV2));
        tree.add(rev3WithAttachments);

        // Create the revision with history
        advancedDatabase.createWithHistory(rev3WithAttachments, REV3.generation, toHistory(tree));

        // Assert
        assertRevisionAndHistoryPresent(tree);
        assertRevisionContent(tree, true, true, false); // Revs 1 and 2 are stubs
        assertNoUnexpectedRevs(tree);

        // Assert the attachment content
        DocumentRevision readRevision = database.read(TestDoc.ID);
        Assert.assertEquals("The latest revision should match the expected.", REV3
                .revision.getRevision(), readRevision.getRevision());
        Map<String, Attachment> readAttachments = readRevision.getAttachments();
        Assert.assertEquals("There should be two attachments", 2, readAttachments.size());
        Attachment a = readAttachments.get("attachment_1.txt");
        Assert.assertNotNull("Attachment 1 should be present", a);
        Assert.assertTrue("Attachment 1 content should be correct", TestUtils.streamsEqual(new
                FileInputStream(a1), a.getInputStream()));
        a = readAttachments.get("attachment_2.txt");
        Assert.assertNotNull("Attachment 2 should be present", a);
        Assert.assertTrue("Attachment 2 content should be correct", TestUtils.streamsEqual(new
                FileInputStream(a2), a.getInputStream()));
    }

    /**
     * Test that createWithHistory with an empty attachment map deletes previous attachments.
     * Assert that the revision is created and the previous revisions are in its history.
     * Assert that the latest (created) revision 3 has no attachments.
     *
     * @throws Exception
     */
    @Test
    public void testDeletingAttachments() throws Exception {
        // Create two generations of revisions with attachments
        Collection<DocumentRevision> tree = createRevisions(2, true);
        // Add the latest revision to the tree so we can create a history for it, note the default
        // rev3 has an empty attachment map, so we don't need to explicitly delete them.
        tree.add(REV3.revision);

        // Create the revision with history
        advancedDatabase.createWithHistory(REV3.revision, REV3.generation, toHistory(tree));

        // Assert
        assertRevisionAndHistoryPresent(tree);
        assertRevisionContent(tree, false, false, false); // no stubs
        assertNoUnexpectedRevs(tree);
        // Assert no attachment content
        DocumentRevision readRevision = database.read(TestDoc.ID);
        Assert.assertEquals("The latest revision should match the expected.", REV3.revision
                .getRevision(), readRevision.getRevision());
        Assert.assertTrue("There should be no attachments.", readRevision.getAttachments()
                .isEmpty());
    }

    /**
     * Test that it is possible to add an attachment to a revision when using createWithHistory.
     * Create rev1 with a single attachment then createWithHistory a rev3 with an extra attachment.
     * Assert that the rev 3 revision is created with a rev 2 stub.
     * Assert that the attachments are as expected on the latest revision.
     * Assert that the new attachment is not visible on the older revision.
     *
     * @throws Exception
     */
    @Test
    public void testAddAttachmentToExisting() throws Exception {
        // Create one revisions with an attachment
        Collection<DocumentRevision> tree = createRevisions(1, true);

        DocumentRevision rev3WithExtraAttachment = new DocumentRevision(TestDoc.ID, REV3.revision
                .getRevision());
        rev3WithExtraAttachment.setBody(REV3.body);


        // Add extra attachment
        DocumentRevision rev1 = database.read(TestDoc.ID);
        Map<String, Attachment> attachments = rev1.getAttachments();
        Assert.assertEquals("There should be 1 attachment.", 1, attachments.size());
        File a2 = TestUtils.loadFixture("fixture/attachment_2.txt");
        Attachment att2 = new UnsavedFileAttachment(a2, "text/plain");
        attachments.put("attachment_2.txt", att2);
        Assert.assertEquals("There should be 2 attachments.", 2, attachments.size());
        rev3WithExtraAttachment.setAttachments(attachments);

        // // Add revs 2 and 3 to make a history
        tree.addAll(toRevisionCollection(EnumSet.of(REV2, REV3)));

        // Create the revision with history
        advancedDatabase.createWithHistory(rev3WithExtraAttachment, REV3.generation, toHistory
                (tree));

        // Assert
        assertRevisionAndHistoryPresent(tree);
        assertRevisionContent(tree, false, true, false); // rev 2 is a stub
        assertNoUnexpectedRevs(tree);
        // Assert the attachment content
        DocumentRevision readRevision = database.read(TestDoc.ID);
        Assert.assertEquals("The latest revision should match the expected.", REV3.revision
                .getRevision(), readRevision.getRevision());
        Map<String, Attachment> readAttachments = readRevision.getAttachments();
        Assert.assertEquals("There should be two attachments", 2, readAttachments.size());
        Attachment a = readAttachments.get("attachment_1.txt");
        Assert.assertNotNull("Attachment 1 should be present", a);
        File a1 = TestUtils.loadFixture("fixture/attachment_1.txt");
        Assert.assertTrue("Attachment 1 content should be correct", TestUtils.streamsEqual(new
                FileInputStream(a1), a.getInputStream()));
        a = readAttachments.get("attachment_2.txt");
        Assert.assertNotNull("Attachment 2 should be present", a);
        Assert.assertTrue("Attachment 2 content should be correct", TestUtils.streamsEqual(new
                FileInputStream(a2), a.getInputStream()));


        // Assert that the new attachment is not present on the stub revision or original revision
        Assert.assertEquals("There should be one attachment on the original revision.", 1,
                database.read(TestDoc.ID, rev1.getRevision()).getAttachments().size());
        // The stub revision is only a stub so doesn't show the attachment info.
        Assert.assertEquals("There should be no attachments on the stub revision.", 0, database
                .read(TestDoc.ID, REV2.revision.getRevision()).getAttachments().size());
        Attachment extra = ((DatabaseImpl) database).getAttachment(TestDoc.ID, rev1.getRevision
                (), "attachment_2.txt");
        Assert.assertNull("The new attachment should not be present on the original revision.",
                extra);
    }

    /**
     * Test that calling createWithHistory will successfully delete one of multiple attachments.
     * Assert that the revision is created and the previous revisions are in its history.
     * Assert that the latest (created) revision 3 has the expected attachment.
     *
     * @throws Exception
     */
    @Test
    public void testDeletingAttachmentFromExisting() throws Exception {
        // Create two generations of revisions with attachments
        Collection<DocumentRevision> tree = createRevisions(2, true);

        // Create a rev3
        DocumentRevision rev3WithDeletedAttachment = new DocumentRevision(TestDoc.ID, REV3.revision
                .getRevision());
        rev3WithDeletedAttachment.setBody(REV3.body);

        // Add only 1 of the attachments from rev2, so when we insert the other attachment will be
        // deleted.
        DocumentRevision rev2 = database.read(TestDoc.ID);
        Map<String, Attachment> attachments = rev2.getAttachments();
        Assert.assertEquals("There should be 2 attachments.", 2, attachments.size());
        attachments.remove("attachment_1.txt");
        Assert.assertEquals("There should be 1 attachment.", 1, attachments.size());
        // Set the single attachment on the revision we want to insert
        rev3WithDeletedAttachment.setAttachments(attachments);

        // Add the latest revision ID to the tree so we can create a history for it.
        tree.add(REV3.revision);

        // Create the revision with history
        advancedDatabase.createWithHistory(rev3WithDeletedAttachment, REV3.generation, toHistory
                (tree));

        // Assert
        assertRevisionAndHistoryPresent(tree);
        assertRevisionContent(tree, false, false, false); // no stubs
        assertNoUnexpectedRevs(tree);
        // Assert one attachment was deleted and the other is present
        DocumentRevision readRevision = database.read(TestDoc.ID);
        Assert.assertEquals("The latest revision should match the expected.", REV3.revision
                .getRevision(), readRevision.getRevision());
        Map<String, Attachment> readAttachments = readRevision.getAttachments();
        Assert.assertEquals("There should be one attachment.", 1, readRevision.getAttachments()
                .size());
        File a2 = TestUtils.loadFixture("fixture/attachment_2.txt");
        Attachment a = readAttachments.get("attachment_2.txt");
        Assert.assertNotNull("Attachment 2 should be present", a);
        Assert.assertTrue("Attachment 2 content should be correct", TestUtils.streamsEqual(new
                FileInputStream(a2), a.getInputStream()));
    }

    /**
     * Test that if we keep the same attachments when using createWithHistory nothing unexpected
     * happens. We should be able to read the attachments without re-adding any attachment data.
     * Assert that the expected revisions are present and that the latest revision is as expected.
     * Assert that the attachments are present and with the correct content.
     *
     * @throws Exception
     */
    @Test
    public void testNoChangeOfExistingAttachments() throws Exception {
        // Create two generations of revisions with attachments
        Collection<DocumentRevision> tree = createRevisions(2, true);

        // Create a special rev3 we can add
        DocumentRevision rev3WithAttachments = new DocumentRevision(TestDoc.ID, REV3.revision
                .getRevision());
        rev3WithAttachments.setBody(REV3.body);
        // Add attachments from existing
        rev3WithAttachments.setAttachments(database.read(TestDoc.ID).getAttachments());

        // Add the rev to the tree so we can generate a history
        tree.add(rev3WithAttachments);

        // Create the revision with history
        advancedDatabase.createWithHistory(rev3WithAttachments, REV3.generation, toHistory(tree));

        // Assert
        assertRevisionAndHistoryPresent(tree);
        assertRevisionContent(tree, false, false, false); // No stubs
        assertNoUnexpectedRevs(tree);

        // Assert the attachment content
        File a1 = TestUtils.loadFixture("fixture/attachment_1.txt");
        File a2 = TestUtils.loadFixture("fixture/attachment_2.txt");
        DocumentRevision readRevision = database.read(TestDoc.ID);
        Assert.assertEquals("The latest revision should match the expected.", REV3
                .revision.getRevision(), readRevision.getRevision());
        Map<String, Attachment> readAttachments = readRevision.getAttachments();
        Assert.assertEquals("There should be two attachments", 2, readAttachments.size());
        Attachment a = readAttachments.get("attachment_1.txt");
        Assert.assertNotNull("Attachment 1 should be present", a);
        Assert.assertTrue("Attachment 1 content should be correct", TestUtils.streamsEqual(new
                FileInputStream(a1), a.getInputStream()));
        a = readAttachments.get("attachment_2.txt");
        Assert.assertNotNull("Attachment 2 should be present", a);
        Assert.assertTrue("Attachment 2 content should be correct", TestUtils.streamsEqual(new
                FileInputStream(a2), a.getInputStream()));
    }

    /**
     * Test that calling createWithHistory twice with the same revision causes a DocumentException
     * to be thrown.
     *
     * @throws Exception
     */
    @Test(expected = DocumentException.class)
    public void testDuplicate() throws Exception {
        Collection<DocumentRevision> revisions = toRevisionCollection(EnumSet.allOf(TestDoc.class));

        // Create the revision with history
        advancedDatabase.createWithHistory(REV3.revision, REV3.generation, toHistory(revisions));

        // try to create the same again
        advancedDatabase.createWithHistory(REV3.revision, REV3.generation, toHistory(revisions));
        // This should fail because the revision already exists
        Assert.fail("A second attempt to insert the same revision should fail.");
    }

    /**
     * Test that calling createWithHistory with the current revision not in the history list causes
     * an IllegalArgumentException to be thrown.
     *
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateWithHistoryCurrentNotInHistoryError() throws Exception {

        // Create a rev 3 without rev 3 being in the history
        advancedDatabase.createWithHistory(REV3.revision, REV3.generation, toHistory
                (toRevisionCollection(EnumSet.of(REV1, REV2))));
    }

    /**
     * Test that passing a null revision causes an IllegalArgumentException
     *
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNullRevision() throws Exception {
        advancedDatabase.createWithHistory(null, REV3.generation, toHistory
                (toRevisionCollection(EnumSet.of(REV3))));
    }

    /**
     * Test that using a start generation of zero causes an IllegalArgumentException
     *
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGenerationZero() throws Exception {
        advancedDatabase.createWithHistory(REV3.revision, 0, toHistory
                (toRevisionCollection(EnumSet.of(REV3))));
    }

    /**
     * Test that using a negative generation causes an IllegalArgumentException
     *
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGenerationNegative() throws Exception {
        advancedDatabase.createWithHistory(REV3.revision, -1, toHistory
                (toRevisionCollection(EnumSet.of(REV3))));
    }

    /**
     * Test that using a null history causes an IllegalArgumentException
     *
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNullHistory() throws Exception {
        advancedDatabase.createWithHistory(REV3.revision, REV3.generation, null);
    }

    /**
     * Test that using an empty history list causes an IllegalArgumentException
     *
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testEmptyHistory() throws Exception {
        List<String> history = Collections.emptyList();
        advancedDatabase.createWithHistory(REV3.revision, REV3.generation, history);
    }

    /**
     * Test that calling createWithHistory with only a partial history (i.e. not back to a first
     * generation does not cause any problems.
     * Assert that the inserted revision and a previous generation stub are present and that there
     * is no first generation rev.
     *
     * @throws Exception
     */
    @Test
    public void testCreateWithIncompleteHistory() throws Exception {
        // Create a rev 3 without rev 1 being in the history
        List<DocumentRevision> tree = toRevisionCollection(EnumSet.of(REV2, REV3));

        advancedDatabase.createWithHistory(REV3.revision, REV3.generation, toHistory(tree));

        assertRevisionAndHistoryPresent(tree);
        assertRevisionContent(tree, true, false); // Rev2 is a stub, rev3 is complete
        assertNoUnexpectedRevs(tree);
    }

    /**
     * Test that calling createWithHistory with no ancestors at all does not cause a problem.
     * Assert that the inserted revision is present and that there are no unexpected other revs.
     *
     * @throws Exception
     */
    @Test
    public void testCreateWithIncompleteHistoryNoParentError() throws Exception {
        Collection<DocumentRevision> rev3Only = Collections.singleton(REV3.revision);

        // Create a rev 3 without any further history
        advancedDatabase.createWithHistory(REV3.revision, REV3.generation, toHistory(rev3Only));

        assertRevisionAndHistoryPresent(rev3Only);
        assertRevisionContent(rev3Only, false);
        assertNoUnexpectedRevs(rev3Only);
    }

    /**
     * Test that inserting a revision with a deleted ancestor does not cause any problems.
     * Assert that the new revision is present, the existing history is unchanged and the latest
     * revision is not deleted and matches the inserted revision.
     *
     * @throws Exception
     */
    @Test
    public void testDeletedAncestor() throws Exception {
        // Create a revision, and delete it
        List<DocumentRevision> created = createRevisions(1);
        DocumentRevision rev1 = created.get(0);
        DocumentRevision deletedRev2 = database.delete(rev1);

        Assert.assertTrue("The revision should be deleted", deletedRev2.isDeleted());

        List<DocumentRevision> tree = Arrays.asList(rev1, deletedRev2, REV3.revision);

        advancedDatabase.createWithHistory(REV3.revision, REV3.generation, toHistory(tree));

        // Assert
        assertRevisionAndHistoryPresent(tree);
        // Assert the content, note that the deleted revision 2 will have an empty body like a stub
        assertRevisionContent(tree, false, true, false); // Revs 1 and 3 have bodies
        // Assert that the inserted revision is the winner (i.e. the document is undeleted)
        DocumentRevision latest = database.read(TestDoc.ID);
        Assert.assertEquals("The latest revision should match the expected.", REV3.revision
                .getRevision(), latest.getRevision());
        Assert.assertFalse("The latest revision of the document should not be deleted.", latest
                .isDeleted());
    }

    /**
     * Test that inserting a deleted revision with history does not cause any problems.
     * Assert that the new revision is present, the stub history exists and the latest
     * revision is deleted.
     *
     * @throws Exception
     */
    @Test
    public void testCurrentDeleted() throws Exception {
        // Create the deleted revision with history
        DocumentRevision deleted3 = new DocumentRevision(TestDoc.ID, REV3.revision
                .getRevision());
        deleted3.setDeleted();

        List<DocumentRevision> tree = toRevisionCollection(EnumSet.of(REV1, REV2));
        tree.add(deleted3);

        advancedDatabase.createWithHistory(deleted3, REV3.generation, toHistory(tree));

        // Assert
        assertRevisionAndHistoryPresent(tree);
        assertRevisionContent(tree, true, true, true); // All stubs because 3 is deleted
        DocumentRevision r = database.read(TestDoc.ID);
        Assert.assertEquals("The latest revision should match the expected.", REV3.revision
                .getRevision(), r.getRevision());
        Assert.assertTrue("The revision should be deleted", r.isDeleted());
    }

    private static final class AssertingConflictResolver implements ConflictResolver {

        Set<String> conflictedRevisionIDs = new HashSet<String>();

        @Override
        public DocumentRevision resolve(String docId, List<? extends DocumentRevision> conflicts) {
            for (DocumentRevision rev : conflicts) {
                conflictedRevisionIDs.add(rev.getRevision());
            }
            return conflicts.get(0);
        }
    }
}
