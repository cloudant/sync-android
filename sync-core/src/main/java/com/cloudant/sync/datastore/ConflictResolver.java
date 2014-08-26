package com.cloudant.sync.datastore;

import java.util.List;

/**
 * Interface to implement conflicts resolving algorithm.
 */
public interface ConflictResolver {

    /**
     * <p>
     * Return resolved {@code DocumentRevision}, the returned DocumentRevision
     * will be added to the Document tree as the child of the current winner,
     * and all the other revisions passed in be deleted.
     * </p>
     * <p>
     * Notice if the returned {@code DocumentRevision} is marked as deleted,
     * the document will be practically deleted since this deleted revision
     * will be the new winner revision.
     * </p>
     * <p>
     * If returned DocumentRevision is null, nothing is changed in Database.
     * </p>
     *
     * @param docId id of the Document with conflicts
     * @param conflicts list of conflicted DocumentRevision, including
     *                  current winner
     * @return resolved DocumentRevision
     *
     * @see com.cloudant.sync.datastore.Datastore#resolveConflictsForDocument(String, ConflictResolver)
     */
    DocumentRevision resolve(String docId, List<BasicDocumentRevision> conflicts);
}