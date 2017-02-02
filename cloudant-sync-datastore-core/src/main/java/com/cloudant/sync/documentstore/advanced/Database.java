package com.cloudant.sync.documentstore.advanced;

import com.cloudant.sync.documentstore.DocumentException;
import com.cloudant.sync.documentstore.DocumentRevision;

import java.util.List;
import java.util.Map;

/**
 * <P>
 * ⚠ Database API methods for advanced use cases.
 * </P>
 * <P>
 * ⚠ Interacting with these methods is not required for the typical use cases. Use with extreme
 * caution.
 * </P>
 */
public interface Database {

    /**
     * <P>
     * ⚠ Creates a new revision in the database with the specified revision history.
     * </P>
     * <P>
     * This is equivalent to inserting a revision with {@code new_edits: false}, functionality that
     * is typically only required by replicators.
     * </P>
     * <P>
     * Note that the revisionsStart and revisionsIDs parameters describe the revision history of the
     * document revision being inserted. This history is equivalent to getting the document from
     * CouchDB with the {@code revs=true} parameter.
     * An example document retrieved with this _revisions history would be:
     * </P>
     * <pre>
     * {@code
     * {"_id”:”exampledoc1”,
     * "_rev":"4-51aa94e4b0ef37271082033bba52b850",
     * "_revisions":
     *     {"start":4,
     *      "ids": [“51aa94e4b0ef37271082033bba52b850",
     *              "f9fb951ca8dadec1459450156b2205cf",
     *              "617a372bba833d7acf3ccf2e7dece15a",
     *              "967a00dff5e02add41819138abb3284d"]
     *     },
     *     ... // other document properties
     * }
     * }
     * </pre>
     * <P>
     * Example usage to insert this example document:
     * </P>
     * <pre>
     * {@code
     * DocumentRevision documentRevision = new DocumentRevision("exampledoc1",
     *     "4-51aa94e4b0ef37271082033bba52b850");
     * // set document content etc
     * documentRevision.setBody(...)
     *
     * // set attachments if necessary, see note on attachments below.
     *
     * documentStore.advanced()
     *    .createWithHistory(documentRevision, 4, Arrays.asList(“51aa94e4b0ef37271082033bba52b850",
     *              "f9fb951ca8dadec1459450156b2205cf",
     *              "617a372bba833d7acf3ccf2e7dece15a",
     *              "967a00dff5e02add41819138abb3284d"))
     * }
     * </pre>
     * <P>
     * If the document is deleted then the inserted revision also needs to be made deleted as in
     * this example:
     * </P>
     * <pre>
     * {@code
     * DocumentRevision documentRevision = new DocumentRevision("exampledoc1",
     *     "4-51aa94e4b0ef37271082033bba52b850");
     * // set document as deleted
     * documentRevision.setDeleted()
     *
     * documentStore.advanced()
     *    .createWithHistory(documentRevision, 4, Arrays.asList(“51aa94e4b0ef37271082033bba52b850",
     *              "f9fb951ca8dadec1459450156b2205cf",
     *              "617a372bba833d7acf3ccf2e7dece15a",
     *              "967a00dff5e02add41819138abb3284d"))
     * }
     * </pre>
     * <P>
     * <B>Attachments</B>
     * </P>
     * <P>
     * Note it is the caller's responsibility to correctly set the attachments on the revision to be
     * created. The existing attachments should be compared to the attachments metadata present in
     * the JSON of the document revision to be created and an appropriate call made to
     * {@link DocumentRevision#setAttachments(Map)} to transfer that metadata and any new attachment
     * files to the {@code DocumentRevision} object. If a comparison of the JSON attachment metadata
     * to the existing attachments indicates that no new attachments are being added then it is
     * possible to simply set the existing attachment map like this:
     * </P>
     * <pre>
     * {@code
     * // Get the existing attachments from the current revision
     * Map<String, Attachment> atts = database.read("exampledoc1").getAttachments();
     * // Set the attachment metadata on the DocumentRevision object that will be passed to
     * // createWithHistory.
     * documentRevision.setAttachments(atts);
     * }
     * </pre>
     * <P>
     * If new attachments need to be added, then the normal CRUD API for creating or updating
     * attachments should be used to create the attachments on the revision before calling
     * createWithHistory, for example, using
     * {@link com.cloudant.sync.documentstore.UnsavedFileAttachment}.
     * </P>
     * <P>
     * If no attachments are set then attachments on ancestor revisions in the existing tree will
     * be deleted on the newly created revision, as it is equivalent to performing an update with an
     * empty attachment map. It is essential to check for existing attachments and set them
     * appropriately on the DocumentRevision if you want to preserve attachments when calling
     * createWithHistory.
     * </P>
     *
     * @param documentRevision the revision of the document to create in the database
     * @param revisionsStart   the generation ID of the revision being inserted, for
     *                         example obtained from the {@code start} property of
     *                         the {@code _revisions} property object of a document
     *                         obtained with {@code revs=true}
     * @param revisionsIDs     the sequence of revision IDs (excluding the generational prefix)
     *                         starting with the revision being inserted and progressing to the root
     *                         of the document, for example a list of the JSON array content of the
     *                         {@code ids} property of the {@code _revisions} property object of a
     *                         document obtained with {@code revs=true}
     * @throws DocumentException if there was an error inserting the revision or its attachments
     *                           into the database
     */
    void createWithHistory(DocumentRevision documentRevision, int revisionsStart, List<String>
            revisionsIDs) throws DocumentException;
}
