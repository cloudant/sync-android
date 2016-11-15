/*
 * Copyright © 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.internal.documentstore.callables;

import com.cloudant.sync.internal.documentstore.InternalDocumentRevision;
import com.cloudant.sync.internal.documentstore.DocumentRevisionTree;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;

/**
 * Delete and mark non-current all Document Revisions except the one with the Revision ID
 * {@code revIdKeep}
 *
 * @api_private
 */
public class ResolveConflictsForDocumentCallable implements SQLCallable<Void> {

    private DocumentRevisionTree docTree;
    private String revIdKeep;


    public ResolveConflictsForDocumentCallable(DocumentRevisionTree docTree, String revIdKeep) {
        this.docTree = docTree;
        this.revIdKeep = revIdKeep;
    }

    @Override
    public Void call(SQLDatabase db) throws Exception {

        for (InternalDocumentRevision revision : docTree.leafRevisions()) {
            if (revision.getRevision().equals(revIdKeep)) {
                // this is the one we want to keep, set it to current
                new SetCurrentCallable(revision.getSequence(), true).call(db);
            } else {
                if (revision.isDeleted()) {
                    // if it is deleted, just make it non-current
                    new SetCurrentCallable(revision.getSequence(), false).call(db);
                } else {
                    // if it's not deleted, deleted and make it non-current
                    InternalDocumentRevision deleted = new DeleteDocumentCallable(
                            revision.getId(), revision.getRevision()).call(db);
                    new SetCurrentCallable(deleted.getSequence(), false).call(db);
                }
            }
        }

        return null;

    }
}
