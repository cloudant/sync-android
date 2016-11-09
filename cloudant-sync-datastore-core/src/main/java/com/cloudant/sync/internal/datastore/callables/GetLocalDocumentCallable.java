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

package com.cloudant.sync.internal.datastore.callables;

import com.cloudant.sync.internal.datastore.DatabaseImpl;
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.documentstore.DocumentBodyFactory;
import com.cloudant.sync.documentstore.DocumentNotFoundException;
import com.cloudant.sync.documentstore.LocalDocument;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Get the local Document for a given Document ID
 *
 * @api_private
 */
public class GetLocalDocumentCallable implements SQLCallable<LocalDocument> {

    private String docId;

    private static final Logger logger = Logger.getLogger(DatabaseImpl.class.getCanonicalName());

    /**
     * @param docId Document to fetch the local Document for
     */
    public GetLocalDocumentCallable(String docId) {
        this.docId = docId;
    }

    @Override
    public LocalDocument call(SQLDatabase database) throws DocumentStoreException, DocumentNotFoundException {
        Cursor cursor = null;
        try {
            String[] args = {docId};
            cursor = database.rawQuery("SELECT json FROM localdocs WHERE docid=?", args);
            if (cursor.moveToFirst()) {
                byte[] json = cursor.getBlob(0);

                return new LocalDocument(docId, DocumentBodyFactory.create(json));
            } else {
                throw new DocumentNotFoundException(String.format("No local document found with " +
                        "id: %s", docId));
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, String.format("Error getting local document with id: %s",
                    docId), e);
            throw new DocumentStoreException("Error getting local document with id: " + docId, e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }

    }
}
