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

import com.cloudant.android.ContentValues;
import com.cloudant.sync.datastore.DatabaseImpl;
import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.datastore.DocumentBody;
import com.cloudant.sync.datastore.DocumentException;
import com.cloudant.sync.datastore.LocalDocument;
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.sqlite.SQLDatabase;

import java.util.logging.Logger;

/**
 * Insert a local (non-replicated) Document
 *
 * @api_private
 */
public class InsertLocalDocumentCallable implements SQLCallable<LocalDocument> {

    private static final Logger logger = Logger.getLogger(DatabaseImpl.class.getCanonicalName());

    private String docId;
    private DocumentBody body;

    public InsertLocalDocumentCallable(String docId, DocumentBody body) {
        this.docId = docId;
        this.body = body;
    }

    @Override
    public LocalDocument call(SQLDatabase db) throws DocumentException, DatastoreException {
        ContentValues values = new ContentValues();
        values.put("docid", docId);
        values.put("json", body.asBytes());

        long rowId = db.insertWithOnConflict("localdocs", values, SQLDatabase
                .CONFLICT_REPLACE);
        if (rowId < 0) {
            throw new DocumentException("Failed to insert local document");
        } else {
            logger.finer(String.format("Local doc inserted: %d , %s", rowId, docId));
        }

        // TODO - just reconstruct LocalDocument rather than refetching, we already know the docId as it was passed in
        return new GetLocalDocumentCallable(docId).call(db);
    }
}
