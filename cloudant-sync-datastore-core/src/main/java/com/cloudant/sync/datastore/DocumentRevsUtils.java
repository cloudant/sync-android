/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.datastore;

import com.cloudant.mazha.DocumentRevs;
import com.cloudant.sync.util.CouchUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DocumentRevsUtils {

    private static final Logger logger = Logger.getLogger(DocumentRevsUtils.class.getCanonicalName());

    /**
     * Create the list of the revision ids in ascending order.
     *
     * The DocumentRevs is for a single tree. There should be one DocumentRevs for each open revision.
     * @param documentRevs a deserialised JSON document including the _revisions structure. See
     * <a href="http://docs.couchdb.org/en/latest/api/document/common.html#getting-a-list-of-revisions">
     * Getting a List of Revisions</a> for more information.
     * @return list of revision ids in ascending order
     */
    public static List<String> createRevisionIdHistory(DocumentRevs documentRevs) {
        validateDocumentRevs(documentRevs);

        String latestRevision = documentRevs.getRev();
        int generation = CouchUtils.generationFromRevId(latestRevision);
        assert generation == documentRevs.getRevisions().getStart();

        List<String> revisionHistory = new ArrayList<String>();
        for (String revision : documentRevs.getRevisions().getIds()) {
            revisionHistory.add(generation + "-" + revision);
            generation--;
        }
        Collections.reverse(revisionHistory);
        logger.log(Level.FINER,"Revisions history: "+revisionHistory);
        return revisionHistory;
    }

    public static DocumentRevision createDocument(DocumentRevs documentRevs) {
        validateDocumentRevs(documentRevs);

        DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
        builder.setDocId(documentRevs.getId());
        builder.setRevId(documentRevs.getRev());
        builder.setDeleted(documentRevs.getDeleted());
        if(documentRevs.getDeleted()) {
            builder.setDeleted(documentRevs.getDeleted());
            builder.setBody(DocumentBodyFactory.EMPTY);
        } else {
            builder.setBody(DocumentBodyFactory.create(documentRevs.getOthers()));
        }
        return builder.build();
    }

    private static void validateDocumentRevs(DocumentRevs documentRevs) {
        Preconditions.checkNotNull(documentRevs, "DocumentRevs must not be null.");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(documentRevs.getId()),
                "DocumentRevs.id must not be empty.");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(documentRevs.getRev()),
                "DocumentRevs.rev must not be empty.");
    }
}
