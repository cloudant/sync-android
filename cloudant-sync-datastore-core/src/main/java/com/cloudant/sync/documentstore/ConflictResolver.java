/**
 * Copyright © 2015 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.documentstore;

import java.util.List;

/**
 * Interface to implement conflicts resolving algorithm.
 *
 * @api_public
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
     * @see Database#resolveConflictsForDocument(String, ConflictResolver)
     */
    DocumentRevision resolve(String docId, List<DocumentRevision> conflicts);
}