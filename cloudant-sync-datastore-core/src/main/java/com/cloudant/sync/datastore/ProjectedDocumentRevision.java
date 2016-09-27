//  Copyright (c) 2015 Cloudant. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.cloudant.sync.datastore;

import java.util.List;
import java.util.logging.Logger;

/**
 *  A document revision that has been projected. This means that some fields may be missing when
 *  compared to the original copy of this revision.
 *
 *  Use {@link #toFullRevision()} to obtain a "full" revision with all fields present. This is a pre-requisite
 *  for saving a {@code ProjectedDocumentRevision}.
 *
 * @api_public
 */
public class ProjectedDocumentRevision extends DocumentRevision {

    private static final Logger logger = Logger.getLogger(ProjectedDocumentRevision.class.getCanonicalName());

    Database database;

    ProjectedDocumentRevision(String docId,
                              String revId,
                              boolean deleted,
                              List<? extends Attachment> attachments,
                              DocumentBody body,
                              Database database) {
        super(docId, revId,body);

        super.setDeleted(deleted);
        super.setAttachmentsInternal(attachments);
        this.database = database;
        this.fullRevision = false;
    }

    @Override
    public DocumentRevision toFullRevision() throws DocumentNotFoundException {
        return this.database.getDocument(this.id,this.revision);
    }


}
