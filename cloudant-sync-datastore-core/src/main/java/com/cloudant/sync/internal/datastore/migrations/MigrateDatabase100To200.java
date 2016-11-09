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

package com.cloudant.sync.internal.datastore.migrations;

import com.cloudant.sync.internal.android.ContentValues;
import com.cloudant.sync.internal.datastore.callables.PickWinningRevisionCallable;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.util.DatabaseUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * <p>
 * Migration to repair datastores impacted by issues 326 and 329.
 * </p>
 * <p>
 * Added migration on `Datastore` opening to repair datastores that have documents with
 * multiple identical revision IDs (as caused by issue #329). The migration will delete duplicate
 * revisions and correct the document tree. It will also re-evaluate the winning revision resolving
 * issues caused by #326.
 * </p>
 * <p>
 * It also deletes duplicate entries with the same sequence and attachment filename, which could
 * have been force inserted into the attachments table during a multipart attachment pull during
 * identical replications running in parallel.
 * </p>
 *
 * @api_private
 */
public class MigrateDatabase100To200 implements Migration {

    private static final Logger LOGGER = Logger.getLogger(MigrateDatabase100To200.class.getName());
    private static final String ALL_BUT_LOWEST_WITH_ID_REV = "(SELECT sequence FROM revs WHERE " +
            "doc_id = ? AND revid = ? AND sequence != ?)";
    private String[] schemaUpdates;

    public MigrateDatabase100To200(String[] schemaUpdates) {
        this.schemaUpdates = schemaUpdates;
    }

    @Override
    public void runMigration(SQLDatabase db) throws Exception {
        Cursor c = null;
        try {
            // Find docs with duplicate revision ids and get the lowest sequence for that revision
            List<DocRevSequence> lowestDuplicateRevs = new ArrayList<DocRevSequence>();
            c = db.rawQuery("SELECT doc_id, revid, min(sequence) FROM revs GROUP BY doc_id, revid" +
                    " HAVING COUNT(*) > 1;", null);
            while (c.moveToNext()) {
                lowestDuplicateRevs.add(new DocRevSequence(c.getLong(0), c.getString(1), c
                        .getLong(2)));
            }
            c.close();
            LOGGER.info(String.format("Found %d duplicated revisions.",
                    lowestDuplicateRevs.size()));


            // For each of the duplicate revs find the other duplicates
            for (DocRevSequence lowest : lowestDuplicateRevs) {

                // Now update any children of each duplicate replacing the parent with the lowest
                // rev and remove the duplicate rev
                ContentValues lowestSequenceAsParent = new ContentValues(1);
                lowestSequenceAsParent.put("parent", lowest.sequence);

                ContentValues lowestSequenceAsSequence = new ContentValues(1);
                lowestSequenceAsSequence.put("sequence", lowest.sequence);

                String[] allButLowestArgs = new String[]{lowest.doc_id.toString(), lowest.revid,
                        lowest.sequence.toString()};

                // Update any children of the duplicate to point to the lowest
                // (lowestSequenceAsParent)
                int childrenUpdated = db.update("revs", lowestSequenceAsParent, "parent IN " +
                        ALL_BUT_LOWEST_WITH_ID_REV, allButLowestArgs);

                if (childrenUpdated > 0) {
                    LOGGER.info(String.format("Updated %d children to have parent %d:%d",
                            childrenUpdated, lowest.doc_id, lowest.sequence));
                }

                // If multipart was used it is possible attachments were not added to the lowest
                // and could be on any of the revisions, so we need to update them too.
                // This might create duplicates on the lowest sequence, but we handle duplicates
                // later anyway.
                int attachmentsMigrated = db.update("attachments", lowestSequenceAsSequence,
                        "sequence IN " +
                                ALL_BUT_LOWEST_WITH_ID_REV, allButLowestArgs);

                if (attachmentsMigrated > 0) {
                    LOGGER.info(String.format("Migrated %d attachments to %d:%d",
                            attachmentsMigrated, lowest.doc_id, lowest.sequence));
                }

                // Finally delete the duplicate revs
                int deleted = db.delete("revs", "sequence IN " + ALL_BUT_LOWEST_WITH_ID_REV,
                        allButLowestArgs);

                if (deleted > 0) {
                    LOGGER.info(String.format("Deleted %d duplicate revisions of %d:%d", deleted,
                            lowest.doc_id, lowest.sequence));
                }

                // For the lowest sequence we should clean up any duplicate attachments that may
                // have resulted from a replication forceInsert using multi-part or our earlier
                // migration of attachments from higher sequence duplicates to the lowest.
                Map<String, Integer> duplicateAttachmentFilenames = new HashMap<String, Integer>();
                c = db.rawQuery("SELECT filename, COUNT(*) FROM attachments WHERE sequence=? " +
                        "GROUP BY " +
                        "filename HAVING COUNT(*) > 1", new String[]{lowest.sequence.toString()});
                while (c.moveToNext()) {
                    duplicateAttachmentFilenames.put(c.getString(0), c.getInt(1));
                }
                c.close();

                if (duplicateAttachmentFilenames.size() > 0) {
                    LOGGER.info(String.format("Found %d attachments with duplicates on %d:%d",
                            duplicateAttachmentFilenames.size(), lowest.doc_id, lowest.sequence));
                }

                int deletedAttachmentCount = 0;
                // Loop through the duplicate attachment entries
                for (Map.Entry<String, Integer> duplicateAttachmentFilename :
                        duplicateAttachmentFilenames.entrySet()) {

                    int duplicateCount = duplicateAttachmentFilename.getValue();

                    LOGGER.info(String.format("Found %d copies of attachment on %d:%d",
                            duplicateCount, lowest.doc_id, lowest.sequence));

                    String[] whereArgs = new String[]{lowest.sequence.toString(),
                            duplicateAttachmentFilename.getKey(),
                            Integer.toString(duplicateCount - 1)};

                    // Since the attachments should be identical it doesn't matter which entry we
                    // delete, but we'll leave the first row.
                    // Unfortunately DELETE ... LIMIT is generally not enabled in Android so we use
                    // a subquery to get the rowids for the duplicates and limit to one less than
                    // the total number of identical attachments we have so that we keep one.
                    int attachmentsDeleted = db.delete("attachments", "rowid IN (SELECT rowid " +
                            "FROM attachments WHERE sequence=? AND filename=? ORDER BY rowid DESC" +
                            " limit ?)", whereArgs);

                    LOGGER.info(String.format("Deleted %d copies of attachment on %d:%d",
                            attachmentsDeleted, lowest.doc_id, lowest.sequence));

                    deletedAttachmentCount++;
                }

                if (deletedAttachmentCount > 0) {
                    LOGGER.info(String.format("Deleted duplicates for %d attachments on %d:%d",
                            deletedAttachmentCount, lowest.doc_id, lowest.sequence));
                }

                // Finally resolve the winner for the document based on all the merged branches
                new PickWinningRevisionCallable(lowest.doc_id).call(db);
            }

            // now we have fixed any duplicates we can migrate the schemas
            new SchemaOnlyMigration(schemaUpdates).runMigration(db);

        } finally {
            DatabaseUtils.closeCursorQuietly(c);
        }
    }

    private static final class DocRevSequence {
        private final Long doc_id;
        private final Long sequence;
        private final String revid;

        private DocRevSequence(Long id, String rev, Long seq) {
            this.doc_id = id;
            this.revid = rev;
            this.sequence = seq;
        }
    }
}
