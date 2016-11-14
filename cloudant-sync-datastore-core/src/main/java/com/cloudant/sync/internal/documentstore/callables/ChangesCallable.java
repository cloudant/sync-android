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
import com.cloudant.sync.documentstore.Changes;
import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Return the list of changes to the datastore, starting at a given `since` sequence value, limited
 * to a maximum number of `limit` changes
 *
 * @api_private
 */
public class ChangesCallable implements SQLCallable<Changes> {

    private long since;
    private int limit;

    private String attachmentsDir;
    private AttachmentStreamFactory attachmentStreamFactory;

    /**
     * @param since Starting sequence number to retrieve changes from
     * @param limit Maximum number of changes to retrieve
     * @param attachmentsDir          Location of attachments
     * @param attachmentStreamFactory Factory to manage access to attachment streams
     */
    public ChangesCallable(long since, int limit, String attachmentsDir, AttachmentStreamFactory
            attachmentStreamFactory) {
        this.since = since;
        this.limit = limit;
        this.attachmentsDir = attachmentsDir;
        this.attachmentStreamFactory = attachmentStreamFactory;
    }

    @Override
    public Changes call(SQLDatabase db) throws Exception {

        String[] args = {Long.toString(since), Long.toString(since +
                limit)};
        Cursor cursor = null;
        try {
            Long lastSequence = since;
            List<Long> ids = new ArrayList<Long>();
            cursor = db.rawQuery(DatabaseImpl.SQL_CHANGE_IDS_SINCE_LIMIT, args);
            while (cursor.moveToNext()) {
                ids.add(cursor.getLong(0));
                lastSequence = Math.max(lastSequence, cursor.getLong(1));
            }
            List<DocumentRevision> results = new GetDocumentsWithInternalIdsCallable(ids, attachmentsDir, attachmentStreamFactory).call(db);
            if (results.size() != ids.size()) {
                throw new IllegalStateException(String.format(Locale.ENGLISH,
                        "The number of documents does not match number of ids, " +
                                "something must be wrong here. Number of IDs: %s, " +
                                "number of documents: %s",
                        ids.size(),
                        results.size()
                ));
            }

            return new Changes(lastSequence, results);
        } catch (SQLException e) {
            throw new IllegalStateException("Error querying all changes since: " +
                    since + ", limit: " + limit, e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

}
