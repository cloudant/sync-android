/*
 * Copyright © 2017 IBM Corp. All rights reserved.
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

package com.cloudant.sync.internal.documentstore.helpers;

import com.cloudant.sync.documentstore.Attachment;
import com.cloudant.sync.documentstore.DocumentException;
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.internal.documentstore.AttachmentManager;
import com.cloudant.sync.internal.documentstore.AttachmentStreamFactory;
import com.cloudant.sync.internal.documentstore.InternalDocumentRevision;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by tomblench on 23/08/2017.
 */

public class GetRevisionsFromRawQuery {
    public static List<InternalDocumentRevision> get(
            SQLDatabase db, String sql, String[] args, String attachmentsDir,
            AttachmentStreamFactory attachmentStreamFactory)
            throws DocumentException, DocumentStoreException {

        List<InternalDocumentRevision> result = new ArrayList<InternalDocumentRevision>();
        Cursor cursor = null;

        try {
            cursor = db.rawQuery(sql, args);
            while (cursor.moveToNext()) {
                long sequence = cursor.getLong(3);
                Map<String, ? extends Attachment> atts = AttachmentManager.attachmentsForRevision(
                        db, attachmentsDir, attachmentStreamFactory, sequence);
                InternalDocumentRevision row = GetFullRevisionFromCurrentCursor.get(cursor, atts);
                result.add(row);
            }
        } catch (SQLException e) {
            throw new DocumentStoreException(e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
        return result;
    }
}
