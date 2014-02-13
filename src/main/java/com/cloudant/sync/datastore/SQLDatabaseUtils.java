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

import com.cloudant.sync.sqlite.Cursor;

class SQLDatabaseUtils {

    static BasicDocumentRevision getFullRevisionFromCurrentCursor(Cursor cursor) {
        String docId = cursor.getString(0);
        long internalId = cursor.getLong(1);
        String revId = cursor.getString(2);
        long sequence = cursor.getLong(3);
        byte[] json = cursor.getBlob(4);
        boolean current = cursor.getInt(5) > 0;
        boolean deleted = cursor.getInt(6) > 0;

        long parent = -1L;
        if(cursor.columnType(7) == Cursor.FIELD_TYPE_INTEGER) {
            parent = cursor.getLong(7);
        } else if(cursor.columnType(7) == Cursor.FIELD_TYPE_NULL) {
        } else {
            throw new IllegalArgumentException("Unexpected type: " + cursor.columnType(7));
        }

        DocumentRevisionBuilder builder = new DocumentRevisionBuilder()
                .setDocId(docId)
                .setRevId(revId)
                .setBody(BasicDocumentBody.bodyWith(json))
                .setDeleted(deleted)
                .setSequence(sequence)
                .setInternalId(internalId)
                .setCurrnet(current)
                .setParent(parent);

        return builder.buildBasicDBObject();
    }

}
