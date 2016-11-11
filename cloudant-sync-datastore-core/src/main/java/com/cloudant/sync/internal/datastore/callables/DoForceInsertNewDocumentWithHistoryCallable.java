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

package com.cloudant.sync.internal.datastore.callables;

import com.cloudant.sync.documentstore.AttachmentException;
import com.cloudant.sync.internal.datastore.DatabaseImpl;
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;

import java.util.List;
import java.util.logging.Logger;

/**
 * Force insert a Revision where the Document does not exist in the local database (ie no Revisions
 * exist). Since there is no Revision tree (yet), build the initial tree by creating stub Revisions
 * as described by `revHistory` and make `rev` the leaf node of this linear "tree".
 *
 * @api_private
 */
public class DoForceInsertNewDocumentWithHistoryCallable implements SQLCallable<Long> {

    private static final Logger logger = Logger.getLogger(DatabaseImpl.class.getCanonicalName());

    private DocumentRevision rev;
    private List<String> revHistory;

    /**
     * @param rev        DocumentRevision to insert
     * @param revHistory revision history to insert, it includes all revisions (include the
     *                   revision of the DocumentRevision
     *                   as well) sorted in ascending order.
     */
    public DoForceInsertNewDocumentWithHistoryCallable(DocumentRevision rev, List<String>
            revHistory) {
        this.rev = rev;
        this.revHistory = revHistory;
    }

    @Override
    public Long call(SQLDatabase db) throws AttachmentException, DocumentStoreException {
        logger.entering("DocumentRevision",
                "doForceInsertNewDocumentWithHistory()",
                new Object[]{rev, revHistory});

        long docNumericID = new InsertDocumentIDCallable(rev.getId()).call(db);
        long parentSequence = 0L;
        for (int i = 0; i < revHistory.size() - 1; i++) {
            // Insert stub node
            parentSequence = DatabaseImpl.insertStubRevisionAdaptor(docNumericID, revHistory.get(i),
                    parentSequence).call(db);
        }
        // Insert the leaf node (don't copy attachments)
        InsertRevisionCallable callable = new InsertRevisionCallable();
        callable.docNumericId = docNumericID;
        callable.revId = revHistory.get(revHistory.size() - 1);
        callable.parentSequence = parentSequence;
        callable.deleted = rev.isDeleted();
        callable.current = true;
        callable.data = rev.getBody().asBytes();
        callable.available = true;
        long sequence = callable.call(db);
        return sequence;
    }
}
