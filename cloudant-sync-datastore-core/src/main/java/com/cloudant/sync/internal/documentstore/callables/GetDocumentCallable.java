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

import com.cloudant.sync.documentstore.Attachment;
import com.cloudant.sync.documentstore.AttachmentException;
import com.cloudant.sync.internal.documentstore.AttachmentStreamFactory;
import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.documentstore.DocumentNotFoundException;
import com.cloudant.sync.internal.documentstore.InternalDocumentRevision;
import com.cloudant.sync.internal.documentstore.helpers.GetFullRevisionFromCurrentCursor;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Get the Document for a given Document ID and Revision ID
 */
public class GetDocumentCallable implements SQLCallable<InternalDocumentRevision> {

    String id;
    String rev;

    String attachmentsDir;
    AttachmentStreamFactory attachmentStreamFactory;

    /**
     * @param id                      The Document ID to get the Document for
     * @param rev                     The Revision ID to get the Document for
     * @param attachmentsDir          Location of attachments
     * @param attachmentStreamFactory Factory to manage access to attachment streams
     */
    public GetDocumentCallable(String id, String rev, String attachmentsDir,
                               AttachmentStreamFactory attachmentStreamFactory) {
        this.id = id;
        this.rev = rev;
        this.attachmentsDir = attachmentsDir;
        this.attachmentStreamFactory = attachmentStreamFactory;
    }

    private static final Logger logger = Logger.getLogger(DatabaseImpl.class.getCanonicalName());


    public InternalDocumentRevision call(SQLDatabase db) throws DocumentNotFoundException, AttachmentException, DocumentStoreException {

        Cursor cursor = null;
        try {
            String[] args = (rev == null) ? new String[]{id} : new String[]{id, rev};
            String sql = (rev == null) ? CallableSQLConstants.GET_DOCUMENT_CURRENT_REVISION :
                    CallableSQLConstants.GET_DOCUMENT_GIVEN_REVISION;
            cursor = db.rawQuery(sql, args);
            if (cursor.moveToFirst()) {
                long sequence = cursor.getLong(3);
                Map<String, ? extends Attachment> atts = new AttachmentsForRevisionCallable(
                        this.attachmentsDir, this.attachmentStreamFactory, sequence).call(db);
                return GetFullRevisionFromCurrentCursor.get(cursor, atts);
            } else {
                throw new DocumentNotFoundException(id, rev);
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error getting document with id: " + id + "and rev " + rev, e);
            throw new DocumentStoreException(String.format("Could not find document with id %s at " +
                    "revision %s", id, rev), e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

}
