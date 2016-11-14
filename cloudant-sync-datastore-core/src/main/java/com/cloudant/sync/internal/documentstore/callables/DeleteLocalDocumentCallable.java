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

import com.cloudant.sync.documentstore.DocumentNotFoundException;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;

/**
 * Delete a local (non-replicated) Document
 *
 * @api_private
 */
public class DeleteLocalDocumentCallable implements SQLCallable<Void> {

    private String docId;

    public DeleteLocalDocumentCallable(String docId) {
        this.docId = docId;
    }

    @Override
    public Void call(SQLDatabase db) throws DocumentNotFoundException {
        String[] whereArgs = {docId};
        int rowsDeleted = db.delete("localdocs", "docid=? ", whereArgs);
        if (rowsDeleted == 0) {
            throw new DocumentNotFoundException(docId, (String) null);
        }
        return null;
    }
}
