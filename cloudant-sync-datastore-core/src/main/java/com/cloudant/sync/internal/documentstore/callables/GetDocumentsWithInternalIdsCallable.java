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

import com.cloudant.sync.internal.documentstore.AttachmentStreamFactory;
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.documentstore.DocumentException;
import com.cloudant.sync.internal.documentstore.InternalDocumentRevision;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.util.CollectionUtils;
import com.cloudant.sync.internal.util.DatabaseUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Get a list of the winning (current) Revisions matching a list of internal (numeric) Document IDs
 *
 * @api_private
 */
public class GetDocumentsWithInternalIdsCallable implements SQLCallable<List<InternalDocumentRevision>> {

    private List<Long> docIds;
    private String attachmentsDir;
    private AttachmentStreamFactory attachmentStreamFactory;

    /**
     * @param docIds                  List of internal (numeric) Document IDs
     * @param attachmentsDir          Location of attachments
     * @param attachmentStreamFactory Factory to manage access to attachment streams
     */
    public GetDocumentsWithInternalIdsCallable(List<Long> docIds, String attachmentsDir,
                                               AttachmentStreamFactory attachmentStreamFactory) {
        this.docIds = docIds;
        this.attachmentsDir = attachmentsDir;
        this.attachmentStreamFactory = attachmentStreamFactory;
    }

    public List<InternalDocumentRevision> call(SQLDatabase db) throws DocumentStoreException,
            DocumentException {

        if (docIds.size() == 0) {
            return Collections.emptyList();
        }

        final String GET_DOCUMENTS_BY_INTERNAL_IDS = "SELECT " + DatabaseImpl.FULL_DOCUMENT_COLS
                + " FROM revs, docs WHERE revs.doc_id IN ( %s ) AND current = 1 AND docs.doc_id =" +
                " revs.doc_id";

        // Split into batches because SQLite has a limit on the number
        // of placeholders we can use in a single query. 999 is the default
        // value, but it can be lower. It's hard to find this out from Java,
        // so we use a value much lower.
        List<InternalDocumentRevision> result = new ArrayList<InternalDocumentRevision>(docIds.size());

        List<List<Long>> batches = CollectionUtils.partition(docIds,
                DatabaseImpl.SQLITE_QUERY_PLACEHOLDERS_LIMIT);
        for (List<Long> batch : batches) {
            String sql = String.format(
                    GET_DOCUMENTS_BY_INTERNAL_IDS,
                    DatabaseUtils.makePlaceholders(batch.size())
            );
            String[] args = new String[batch.size()];
            for (int i = 0; i < batch.size(); i++) {
                args[i] = Long.toString(batch.get(i));
            }
            result.addAll(DatabaseImpl.getRevisionsFromRawQuery(db, sql, args, attachmentsDir,
                    attachmentStreamFactory));
        }

        // Contract is to sort by sequence number, which we need to do
        // outside the sqlDb as we're batching requests.
        Collections.sort(result, new InternalDocumentRevisionComparator());

        return result;
    }

    private static class InternalDocumentRevisionComparator implements Comparator<InternalDocumentRevision>, Serializable {

        private static final long serialVersionUID = -2052529058740335141L;

        @Override
        public int compare(InternalDocumentRevision documentRevision, InternalDocumentRevision
                documentRevision2) {
            long a = documentRevision.getSequence();
            long b = documentRevision2.getSequence();
            return (int) (a - b);
        }
    }
}
