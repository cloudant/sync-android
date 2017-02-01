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

import com.cloudant.sync.documentstore.Attachment;
import com.cloudant.sync.documentstore.AttachmentException;
import com.cloudant.sync.internal.documentstore.AttachmentManager;
import com.cloudant.sync.internal.documentstore.AttachmentStreamFactory;
import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.internal.documentstore.SavedAttachment;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.util.DatabaseUtils;

import java.io.File;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Get all Attachments for a given internal sequence number
 */
public class AttachmentsForRevisionCallable implements SQLCallable<Map<String, ? extends Attachment>> {

    private String attachmentsDir;
    private AttachmentStreamFactory attachmentStreamFactory;
    private long sequence;

    private static final Logger logger = Logger.getLogger(DatabaseImpl.class.getCanonicalName());

    /**
     * @param attachmentsDir          Location of attachments
     * @param attachmentStreamFactory Factory to manage access to attachment streams
     * @param sequence Internal sequence number of Revision with which these Attachments are associated
     */
    public AttachmentsForRevisionCallable(String attachmentsDir, AttachmentStreamFactory
            attachmentStreamFactory, long sequence) {
        this.attachmentsDir = attachmentsDir;
        this.attachmentStreamFactory = attachmentStreamFactory;
        this.sequence = sequence;
    }


    public Map<String, ? extends Attachment> call(SQLDatabase db) throws AttachmentException {

        Cursor c = null;
        try {
            Map<String, SavedAttachment> atts = new HashMap<String, SavedAttachment>();
            c = db.rawQuery(AttachmentManager.SQL_ATTACHMENTS_SELECT_ALL,
                    new String[]{String.valueOf(sequence)});
            while (c.moveToNext()) {
                String filename = c.getString(c.getColumnIndex("filename"));
                byte[] key = c.getBlob(c.getColumnIndex("key"));
                String type = c.getString(c.getColumnIndex("type"));
                int encoding = c.getInt(c.getColumnIndex("encoding"));
                long length = c.getInt(c.getColumnIndex("length"));
                long encodedLength = c.getInt(c.getColumnIndex("encoded_length"));
                int revpos = c.getInt(c.getColumnIndex("revpos"));
                File file = AttachmentManager.fileFromKey(db, key, attachmentsDir, false);

                atts.put(filename, new SavedAttachment(sequence, filename, key, type, Attachment.Encoding
                        .values()[encoding], length, encodedLength, revpos, file,
                        attachmentStreamFactory));
            }
            return atts;
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Failed to get attachments", e);
            throw new AttachmentException(e);
        } finally {
            DatabaseUtils.closeCursorQuietly(c);
        }
    }

}
