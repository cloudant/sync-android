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

import com.cloudant.sync.internal.android.ContentValues;
import com.cloudant.sync.internal.documentstore.AttachmentManager;
import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;

import java.util.logging.Logger;

/**
 * Compact datastore by deleting JSON and attachments of non-leaf revisions
 *
 */
public class CompactCallable implements SQLCallable<Void> {

    private static final Logger logger = Logger.getLogger(DatabaseImpl.class.getCanonicalName());

    private String attachmentsDir;

    public CompactCallable(String attachmentsDir) {
        this.attachmentsDir = attachmentsDir;
    }

    @Override
    public Void call(SQLDatabase db) throws Exception {
        logger.finer("Deleting JSON of old revisions...");

        // set json = null for non-leaf nodes
        ContentValues args = new ContentValues();
        args.put("json", (String) null);
        int revsCompacted = db.update("revs", args, "sequence IN (SELECT parent FROM revs)", null);
        if (revsCompacted < 0) {
            throw new IllegalStateException("Error running compact SQL update");
        }
        logger.finer(String.format("Compacted %d revisions", revsCompacted));

        logger.finer("Deleting old attachments...");

        // delete attachments not referenced by leaf nodes
        AttachmentManager.purgeAttachments(db, attachmentsDir);

        // issue SQL vacuum
        logger.finer("Vacuuming SQLite database...");
        db.compactDatabase();
        return null;

    }
}
