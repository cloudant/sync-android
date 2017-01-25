/*
 * Copyright (C) 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.datastore.callables;

import com.cloudant.sync.datastore.AttachmentStreamFactory;
import com.cloudant.sync.datastore.DatabaseImpl;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.DatabaseUtils;

import java.util.List;

/**
 * Get a List of Document Revisions by Document ID
 *
 * @api_private
 */
public class GetDocumentsWithIdsCallable implements SQLCallable<List<DocumentRevision>> {

    private List<String> docIds;

    private String attachmentsDir;
    private AttachmentStreamFactory attachmentStreamFactory;

    public GetDocumentsWithIdsCallable(List<String> docIds, String attachmentsDir,
                                       AttachmentStreamFactory attachmentStreamFactory) {
        this.docIds = docIds;
        this.attachmentsDir = attachmentsDir;
        this.attachmentStreamFactory = attachmentStreamFactory;
    }

    @Override
    public List<DocumentRevision> call(SQLDatabase db) throws Exception {
        String sql = String.format("SELECT " + DatabaseImpl.FULL_DOCUMENT_COLS + " FROM revs, docs" +
                " WHERE docid IN ( %1$s ) AND current = 1 AND docs.doc_id = revs.doc_id " +
                " ORDER BY docs.doc_id ", DatabaseUtils.makePlaceholders(docIds.size
                ()));
        String[] args = docIds.toArray(new String[docIds.size()]);
        List<DocumentRevision> docs = DatabaseImpl.getRevisionsFromRawQuery(db, sql, args, attachmentsDir, attachmentStreamFactory);
        // Sort in memory since seems not able to sort them using SQL
        return DatabaseImpl.sortDocumentsAccordingToIdList(docIds, docs);
    }
}
