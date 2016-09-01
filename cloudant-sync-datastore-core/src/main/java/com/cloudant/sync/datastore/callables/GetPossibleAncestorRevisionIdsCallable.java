/**
 * Copyright (c) 2015 IBM Cloudant. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.datastore.callables;

import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.CouchUtils;
import com.cloudant.sync.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Given a revision, retrieve all revision IDs that have a generation
 * lower than the passed revision's generation. These are 'possible'
 * revisions because the revision IDs returned need not be on the same
 * branch of the document's revision tree as the passed revision.
 *
 * @api_private
 */
public class GetPossibleAncestorRevisionIdsCallable implements SQLCallable<List<String>> {
    private final String docId;
    private final String revId;
    private final int limit;

    /**
     * Creates a GetPossibleAncestorRevisionIdsCallable object to retrieve revision IDs.
     * @param docId Document to retrieve IDs.
     * @param revId IDs retrieved will have lower generation numbers than this.
     * @param limit Maximum IDs to retrieve.
     */
    public GetPossibleAncestorRevisionIdsCallable(String docId, String revId, int limit) {
        this.docId = docId;
        this.revId = revId;
        this.limit = limit;
    }

    @Override
    public List<String> call(SQLDatabase db) throws Exception {
        return getPossibleAncestorRevisionIDsInQueue(db, docId, revId, limit);
    }

    private List<String> getPossibleAncestorRevisionIDsInQueue(SQLDatabase db, final String docId,
                                                               final String revId, int limit)
            throws DatastoreException {
        int generation = CouchUtils.generationFromRevId(revId);
        if (generation <= 1) {
            return null;
        }

        String sql = "SELECT revid FROM revs, docs WHERE docs.docid=?" +
                " and revs.deleted=0 and revs.json not null and revs.doc_id = docs.doc_id" +
                " ORDER BY revs.sequence DESC";
        ArrayList<String> ids = new ArrayList<String>();
        Cursor c = null;
        try {
            c = db.rawQuery(sql, new String[]{docId});
            while (c.moveToNext() && limit > 0) {
                String ancestorRevId = c.getString(0);
                int ancestorGeneration = CouchUtils.generationFromRevId(ancestorRevId);
                if (ancestorGeneration < generation) {
                    ids.add(ancestorRevId);
                    limit--;
                }
            }
        } catch (SQLException sqe) {
            throw new DatastoreException(sqe);
        } finally {
            DatabaseUtils.closeCursorQuietly(c);
        }
        return ids;
    }
}
