/*
 * Copyright © 2016, 2017 IBM Corp. All rights reserved.
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

import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.internal.documentstore.AttachmentStreamFactory;
import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.internal.documentstore.InternalDocumentRevision;
import com.cloudant.sync.internal.documentstore.helpers.GetRevisionsFromRawQuery;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.util.DatabaseUtils;
import com.cloudant.sync.internal.util.Misc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Get a List of Document Revisions by Document ID
 */
public class GetDocumentsWithIdsCallable implements SQLCallable<List<DocumentRevision>> {

    private static final Logger logger = Logger.getLogger(GetDocumentsWithIdsCallable.class.getCanonicalName());

    private List<String> docIds;

    private String attachmentsDir;
    private AttachmentStreamFactory attachmentStreamFactory;

    public GetDocumentsWithIdsCallable(List<String> docIds, String attachmentsDir,
                                       AttachmentStreamFactory attachmentStreamFactory) {

        Misc.checkArgument(!docIds.isEmpty(), "docIds list cannot be empty.");
        this.docIds = docIds;
        this.attachmentsDir = attachmentsDir;
        this.attachmentStreamFactory = attachmentStreamFactory;
    }

    @Override
    public List<DocumentRevision> call(SQLDatabase db) throws Exception {
        String sql = String.format("SELECT " + CallableSQLConstants.FULL_DOCUMENT_COLS + " FROM revs, docs" +
                " WHERE docid IN ( %1$s ) AND current = 1 AND docs.doc_id = revs.doc_id " +
                " ORDER BY docs.doc_id ", DatabaseUtils.makePlaceholders(docIds.size
                ()));
        String[] args = docIds.toArray(new String[docIds.size()]);
        List<InternalDocumentRevision> docs = GetRevisionsFromRawQuery.get(db, sql, args,
                attachmentsDir, attachmentStreamFactory);
        // Sort in memory since seems not able to sort them using SQL
        return sortDocumentsAccordingToIdList(docIds, docs);
    }

    private static List<DocumentRevision> sortDocumentsAccordingToIdList(List<String> docIds,
                                                                        List<InternalDocumentRevision> docs) {
        Map<String, InternalDocumentRevision> idToDocs = putDocsIntoMap(docs);
        List<DocumentRevision> results = new ArrayList<DocumentRevision>();
        for (String id : docIds) {
            if (idToDocs.containsKey(id)) {
                results.add(idToDocs.remove(id));
            } else {
                logger.fine("No document found for id: " + id);
            }
        }
        assert idToDocs.size() == 0;
        return results;
    }

    private static Map<String, InternalDocumentRevision> putDocsIntoMap(List<InternalDocumentRevision> docs) {
        Map<String, InternalDocumentRevision> map = new HashMap<String, InternalDocumentRevision>();
        for (InternalDocumentRevision doc : docs) {
            // ID should be unique cross all docs
            assert !map.containsKey(doc.getId());
            map.put(doc.getId(), doc);
        }
        return map;
    }


}
