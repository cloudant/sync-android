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

package com.cloudant.sync.internal.datastore.callables;

import com.cloudant.sync.internal.android.ContentValues;
import com.cloudant.sync.documentstore.DocumentStoreException;
import com.cloudant.sync.internal.sqlite.SQLCallable;
import com.cloudant.sync.internal.sqlite.SQLDatabase;

/**
 * <p>
 *     Set the {@code current} field in the revs table to true or false.
 * </p>
 * <p>
 *     The {@code current} field is used to track the "current" or "winning" revision in the
 *     case of conflicted document trees. This is updated according to the standard couch
 *     algorithm.
 * </p>
 *
 * @api_private
 */

public class SetCurrentCallable implements SQLCallable<Void> {

    private long sequence;
    private boolean valueOfCurrent;

    /**
     * @param sequence       Sequence number of revision
     * @param valueOfCurrent New value of {@code current} (true/false)
     *
     * @see PickWinningRevisionCallable
     */
    public SetCurrentCallable(long sequence, boolean valueOfCurrent) {
        this.sequence = sequence;
        this.valueOfCurrent = valueOfCurrent;
    }

    @Override
    public Void call(SQLDatabase db) throws DocumentStoreException {
        ContentValues updateContent = new ContentValues();
        updateContent.put("current", valueOfCurrent ? 1 : 0);
        String[] whereArgs = new String[]{String.valueOf(sequence)};
        db.update("revs", updateContent, "sequence=?", whereArgs);
        return null;
    }
}
