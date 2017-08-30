/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 */

package com.cloudant.sync.internal.documentstore.callables;

import com.cloudant.sync.internal.documentstore.AttachmentManager;
import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by tomblench on 29/08/2017.
 */

public class PurgeCallable implements SQLCallable<Void> {

    private static final Logger logger = Logger.getLogger(PurgeCallable.class.getCanonicalName());

    private final String documentId;

    private final String revisionId;

    private final String attachmentsDir;

    public PurgeCallable(String documentId, String revisionId, String attachmentsDir) {
        this.documentId = documentId;
        this.revisionId = revisionId;
        this.attachmentsDir = attachmentsDir;
    }

    @Override
    public Void call(SQLDatabase db) throws Exception {
        System.out.println("PurgeCallable documentstore");
        int nRows = db.delete("revs",
                "doc_id = (select doc_id from docs where docid = ?) AND revid = ?",
                new String[]{documentId, revisionId});
        if (nRows != 1) {
            logger.warning(String.format("purge expected to delete 1 row but actually deleted %d rows",
                    nRows));
        }
        logger.info(String.format("purged %d rows", nRows));
        AttachmentManager.purgeAttachments(db, attachmentsDir);
        return null;
    }
}
