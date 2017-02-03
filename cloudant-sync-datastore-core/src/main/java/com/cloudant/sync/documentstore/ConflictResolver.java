/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
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
 * The {@code ConflictResolver} interface should be implemented by classes resolve
 * conflicts via {@link Database#resolveConflicts(String, ConflictResolver)}.
 */
public interface ConflictResolver {

    /**
     * <p>
     * Return the resolved {@link DocumentRevision}.
     * </p>
     * <p>
     * The returned {@link DocumentRevision}
     * will be added to the Document tree as the child of the current winner,
     * and all the other revisions in {@code conflicts} will be deleted.
     * </p>
     * <p>
     * NB: if the returned {@link DocumentRevision} is marked as deleted,
     * the document will effectively be deleted because all leaf revisions including the new winner
     * revision will be marked as deleted.
     * </p>
     * <p>
     * If the returned DocumentRevision is null, nothing is changed in the Database.
     * </p>
     *
     * @param docId ID of the {@link DocumentRevision} with conflicts
     * @param conflicts list of conflicted {@link DocumentRevision}s, including
     *                  current winner
     * @return the resolved {@link DocumentRevision}.
     *
     * @see Database#resolveConflicts(String, ConflictResolver)
     */
    DocumentRevision resolve(String docId, List<? extends DocumentRevision> conflicts);
}
