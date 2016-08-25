/**
 * Copyright (c) 2015 IBM Cloudant. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.datastore.callables;

import com.cloudant.android.ContentValues;
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.sqlite.SQLDatabase;

import java.util.concurrent.Callable;
import java.util.logging.Logger;

/**
 * Inserts a new row into the `revs` table, returning new database sequence number.
 *
 * @api_private
 */
public class InsertRevisionCallable implements SQLCallable<Long> {
    private static final Logger logger = Logger.getLogger(InsertRevisionCallable.class.getCanonicalName());

    // doc_id in revs table
    public long docNumericId;
    public String revId;
    public long parentSequence;
    // is revision deleted?
    public boolean deleted;
    // is revision current? ("winning")
    public boolean current;
    public byte[] data;
    public boolean available;

    @Override
    public String toString() {
        return "InsertRevisionCallable {" +
                " docNumericId=" + docNumericId +
                ", revId='" + revId + "'" +
                ", parentSequence=" + parentSequence +
                ", deleted=" + deleted +
                ", current=" + current +
                ", available=" + available +
                '}';
    }

    @Override
    public Long call(SQLDatabase db) {
        long newSequence;
        ContentValues args = new ContentValues();
        args.put("doc_id", this.docNumericId);
        args.put("revid", this.revId);
        // parent field is a foreign key
        if (this.parentSequence > 0) {
            args.put("parent", this.parentSequence);
        }
        args.put("current", this.current);
        args.put("deleted", this.deleted);
        args.put("available", this.available);
        args.put("json", this.data);
        logger.fine("New revision inserted: " + this.docNumericId + ", " + this.revId);
        newSequence = db.insert("revs", args);
        if (newSequence < 0) {
            throw new IllegalStateException("Unknown error inserting new revision, please check log");
        }
        return newSequence;
    }
}
