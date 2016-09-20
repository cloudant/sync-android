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

import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.AttachmentException;
import com.cloudant.sync.datastore.AttachmentManager;
import com.cloudant.sync.datastore.AttachmentStreamFactory;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.datastore.DatastoreImpl;
import com.cloudant.sync.datastore.DocumentNotFoundException;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Get the Document for a given Document ID and Revision ID
 *
 * @api_private
 */
public class GetDocumentCallable implements SQLCallable<DocumentRevision> {

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

    private static final Logger logger = Logger.getLogger(DatastoreImpl.class.getCanonicalName());


    public DocumentRevision call(SQLDatabase db) throws DocumentNotFoundException, AttachmentException, DatastoreException {

        Cursor cursor = null;
        try {
            String[] args = (rev == null) ? new String[]{id} : new String[]{id, rev};
            String sql = (rev == null) ? DatastoreImpl.GET_DOCUMENT_CURRENT_REVISION :
                    DatastoreImpl.GET_DOCUMENT_GIVEN_REVISION;
            cursor = db.rawQuery(sql, args);
            if (cursor.moveToFirst()) {
                long sequence = cursor.getLong(3);
                List<? extends Attachment> atts = new AttachmentsForRevisionCallable(
                        this.attachmentsDir, this.attachmentStreamFactory, sequence).call(db);
                return DatastoreImpl.getFullRevisionFromCurrentCursor(cursor, atts);
            } else {
                throw new DocumentNotFoundException(id, rev);
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error getting document with id: " + id + "and rev " + rev, e);
            throw new DatastoreException(String.format("Could not find document with id %s at " +
                    "revision %s", id, rev), e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

}
