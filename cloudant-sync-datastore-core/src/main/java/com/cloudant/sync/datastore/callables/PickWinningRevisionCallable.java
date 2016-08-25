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

package com.cloudant.sync.datastore.callables;

import com.cloudant.android.ContentValues;
import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLCallable;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.CouchUtils;
import com.cloudant.sync.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Callable that evaluates leaf nodes to identify the winning revision for a particular document ID.
 * Marks all revisions as not current, before marking a single winner.
 *
 * @api_private
 */
public class PickWinningRevisionCallable implements SQLCallable<Void> {

    // get all non-deleted leaf rev ids for a given doc id
    // gets all revs whose sequence is not a parent of another rev and the rev isn't deleted
    public static final String GET_NON_DELETED_LEAFS = "SELECT revs.revid, revs.sequence FROM " +
            "revs WHERE revs.doc_id = ? AND revs.deleted = 0 AND revs.sequence NOT IN " +
            "(SELECT DISTINCT parent FROM revs WHERE parent NOT NULL) ";

    // get all leaf rev ids for a given doc id
    // gets all revs whose sequence is not a parent of another rev
    public static final String GET_ALL_LEAFS = "SELECT revs.revid, revs.sequence FROM revs " +
            "WHERE revs.doc_id = ? AND revs.sequence NOT IN " +
            "(SELECT DISTINCT parent FROM revs WHERE parent NOT NULL) ";

    private final long docNumericId;

    /**
     * Identify and set the winning revision for the document specified by the supplied internal
     * document ID.
     *
     * @param docNumericId the numeric (internal) ID of the document to set the winning revision for
     */
    public PickWinningRevisionCallable(long docNumericId) {
        this.docNumericId = docNumericId;
    }

    /**
     * Execute the callable, selecting and marking the new winning revision.
     *
     * @param db the database to execute the callable against
     * @throws DatastoreException
     */
    @Override
    public Void call(SQLDatabase db) throws DatastoreException {

         /*
         Pick winner and mark the appropriate revision with the 'current' flag set
         - There can only be one winner in a tree (or set of trees - if there is no common root)
           at any one time, so if there is a new winner, we only have to mark the old winner as
           no longer 'current'. This is the 'previousWinner' object
         - The new winner is determined by:
           * consider only non-deleted leafs
           * sort according to the CouchDB sorting algorithm: highest rev wins, if there is a tie
             then do a lexicographical compare of the revision id strings
           * we do a reverse sort (highest first) and pick the 1st and mark it 'current'
           * special case: if all leafs are deleted, then apply sorting and selection criteria
             above to all leafs
         */

        // first get all non-deleted leafs
        SortedMap<String, Long> leafs = new TreeMap<String, Long>(new Comparator<String>() {
            @Override
            public int compare(String r1, String r2) {
                int generationCompare = CouchUtils.generationFromRevId(r1) -
                        CouchUtils.generationFromRevId(r2);
                // note that the return statements have a unary minus since we are reverse sorting
                if (generationCompare != 0) {
                    return -generationCompare;
                } else {
                    return -CouchUtils.getRevisionIdSuffix(r1).compareTo(CouchUtils
                            .getRevisionIdSuffix(r2));
                }
            }
        });

        Cursor cursor = null;
        try {
            cursor = db.rawQuery(GET_NON_DELETED_LEAFS, new String[]{Long.toString(docNumericId)});
            while (cursor.moveToNext()) {
                leafs.put(cursor.getString(0), cursor.getLong(1));
            }
        } catch (SQLException sqe) {
            throw new DatastoreException("Exception thrown whilst trying to fetch non-deleted " +
                    "leaf nodes.", sqe);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }

        // this is a corner case - all leaf nodes are deleted
        // re-get with the same query but without the revs.delete clause
        if (leafs.size() == 0) {
            try {
                cursor = db.rawQuery(GET_ALL_LEAFS, new String[]{Long.toString(docNumericId)});
                while (cursor.moveToNext()) {
                    leafs.put(cursor.getString(0), cursor.getLong(1));
                }
            } catch (SQLException sqe) {
                throw new DatastoreException("Exception thrown whilst trying to fetch all leaf " +
                        "nodes.", sqe);
            } finally {
                DatabaseUtils.closeCursorQuietly(cursor);
            }
        }

        // new winner will be at the top of the list
        long newWinnerSeq = leafs.get(leafs.firstKey());
        // set current=1 for winning sequence
        ContentValues currentTrue = new ContentValues();
        currentTrue.put("current", 1);
        db.update("revs", currentTrue,
                "sequence=?", new String[]{Long.toString(newWinnerSeq)});
        // set current=0 for all other leaf sequences with this doc_id
        ContentValues currentFalse = new ContentValues();
        currentFalse.put("current", 0);
        db.update("revs", currentFalse,
                "sequence!=? AND doc_id=? AND sequence NOT IN " +
                        "(SELECT DISTINCT parent FROM revs WHERE parent NOT NULL)",
                new String[]{Long.toString(newWinnerSeq), Long.toString(docNumericId)});
        return null;
    }
}
