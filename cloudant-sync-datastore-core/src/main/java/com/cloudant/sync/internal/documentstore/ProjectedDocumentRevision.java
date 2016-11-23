/*
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

package com.cloudant.sync.internal.documentstore;


import com.cloudant.sync.documentstore.Attachment;
import com.cloudant.sync.documentstore.Database;
import com.cloudant.sync.documentstore.DocumentBody;
import com.cloudant.sync.documentstore.DocumentException;
import com.cloudant.sync.documentstore.DocumentNotFoundException;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.documentstore.DocumentStoreException;

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
public class ProjectedDocumentRevision extends InternalDocumentRevision {

    private static final Logger logger = Logger.getLogger(ProjectedDocumentRevision.class.getCanonicalName());

    Database database;

    public ProjectedDocumentRevision(String docId,
                              String revId,
                              boolean deleted,
                              List<? extends Attachment> attachments,
                              DocumentBody body,
                              Database database) {
        super(docId, revId, body, null);

        super.setDeleted(deleted);
        super.setAttachmentsInternal(attachments);
        this.database = database;
    }

    @Override
    public boolean isFullRevision(){
        return false;
    }

    @Override
    public DocumentRevision toFullRevision() throws DocumentNotFoundException, DocumentStoreException {
        return this.database.getDocument(this.id,this.revision);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ProjectedDocumentRevision that = (ProjectedDocumentRevision) o;

        return database != null ? database.equals(that.database) : that.database == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (database != null ? database.hashCode() : 0);
        return result;
    }
}
