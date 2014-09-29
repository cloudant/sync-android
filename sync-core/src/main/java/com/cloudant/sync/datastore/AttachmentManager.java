/**
 * Copyright (c) 2014 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.datastore;

import com.cloudant.common.Log;
import com.cloudant.sync.sqlite.ContentValues;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.util.CouchUtils;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileExistsException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by tomblench on 14/03/2014.
 */
class AttachmentManager {

    private static final String LOG_TAG = "AttachmentManager";

    private static final String EXTENSION_NAME = "com.cloudant.attachments";

    private static final String SQL_ATTACHMENTS_SELECT = "SELECT sequence, " +
            "filename, " +
            "key, " +
            "type, " +
            "encoding, " +
            "length, " +
            "encoded_length, " +
            "revpos " +
            "FROM attachments " +
            "WHERE filename = ? and sequence = ?";

    private static final String SQL_ATTACHMENTS_SELECT_ALL = "SELECT sequence, " +
            "filename, " +
            "key, " +
            "type, " +
            "encoding, " +
            "length, " +
            "encoded_length, " +
            "revpos " +
            "FROM attachments " +
            "WHERE sequence = ?";

    private static final String SQL_ATTACHMENTS_SELECT_ALL_KEYS = "SELECT key " +
            "FROM attachments";

    public final String attachmentsDir;

    private BasicDatastore datastore;

    public AttachmentManager(BasicDatastore datastore) {
        this.datastore = datastore;
        this.attachmentsDir = datastore.extensionDataFolder(EXTENSION_NAME);
    }

    public void addAttachment(PreparedAttachment a, BasicDocumentRevision rev) throws IOException, SQLException {

        // do it this way to only go thru inputstream once
        // * write to temp location using copyinputstreamtofile
        // * get sha1
        // * stick it into database
        // * move file using sha1 as name

        ContentValues values = new ContentValues();
        long sequence = rev.getSequence();
        String filename = a.attachment.name;
        byte[] sha1 = a.sha1;
        String type = a.attachment.type;
        int encoding = a.attachment.encoding.ordinal();
        long length = a.tempFile.length();
        long revpos = CouchUtils.generationFromRevId(rev.getRevision());

        values.put("sequence", sequence);
        values.put("filename", filename);
        values.put("key", sha1);
        values.put("type", type);
        values.put("encoding", encoding);
        values.put("length", length);
        values.put("encoded_length", length);
        values.put("revpos", revpos);

        // delete and insert in case there is already an attachment at this seq (eg copied over from a previous rev)
        datastore.getSQLDatabase().delete("attachments", " filename = ? and sequence = ? ", new String[]{filename, String.valueOf(sequence)});
        long result = datastore.getSQLDatabase().insert("attachments", values);
        if (result == -1) {
            // if we can't insert into DB then don't copy the attachment
            a.tempFile.delete();
            throw new SQLException("Could not insert attachment " + a + " into database with values " + values + "; not copying to attachments directory");
        }
        // move file to blob store, with file name based on sha1
        File newFile = fileFromKey(sha1);
        try{
            FileUtils.moveFile(a.tempFile, newFile);
        } catch (FileExistsException fee) {
            // File with same SHA1 hash in the store, we assume it's the same content so can discard
            // the duplicate data we have just downloaded
            a.tempFile.delete();
        }
    }

    class PreparedAndSavedAttachments
    {
        List<SavedAttachment> savedAttachments = new ArrayList<SavedAttachment>();
        List<PreparedAttachment> preparedAttachments = new ArrayList<PreparedAttachment>();

        public boolean isEmpty() {
            return savedAttachments.isEmpty() && preparedAttachments.isEmpty();
        }
    }

    // take a set of attachments, and:
    // * if attachment is saved, add it to the saved list
    // * if attachment is not saved, prepare it, and add it to the prepared list
    // this way, the attachments are sifted through ready to be added in the database for a given revision
    protected PreparedAndSavedAttachments prepareAttachments(Collection<? extends Attachment> attachments) throws IOException {
        // actually a list of prepared or saved attachments
        PreparedAndSavedAttachments preparedAndSavedAttachments = new PreparedAndSavedAttachments();

        if (attachments == null || attachments.size() == 0) {
            // nothing to do
            return null;
        }

        for (Attachment a : attachments) {
            if (!(a instanceof SavedAttachment)) {
                preparedAndSavedAttachments.preparedAttachments.add(new PreparedAttachment(a, this.attachmentsDir));
            } else {
                preparedAndSavedAttachments.savedAttachments.add((SavedAttachment)a);
            }
        }

        return preparedAndSavedAttachments;
    }

    protected void setAttachments(BasicDocumentRevision rev, PreparedAndSavedAttachments preparedAndSavedAttachments) throws IOException {

        // set attachments for revision:
        // * prepared attachments are added
        // * copy existing attachments forward to the next revision

        if (preparedAndSavedAttachments == null || preparedAndSavedAttachments.isEmpty()) {
            // nothing to do
            return;
        }

        try {
            this.datastore.getSQLDatabase().beginTransaction();
            for (PreparedAttachment a : preparedAndSavedAttachments.preparedAttachments) {
                // go thru prepared attachments and add them
                this.addAttachment(a, rev);
            }
            for (SavedAttachment a : preparedAndSavedAttachments.savedAttachments) {
                // go thru existing (from previous rev) and new (from another document) saved attachments
                // and add them (the effect on existing attachments is to copy them forward to this revision)
                long parentSequence = ((SavedAttachment) a).seq;
                long newSequence = rev.getSequence();
                this.copyAttachment(parentSequence, newSequence, a.name);
            }
            this.datastore.getSQLDatabase().setTransactionSuccessful();
        } catch (SQLException sqe) {
            throw new SQLRuntimeException("SQLException setting attachment for rev"+rev, sqe);
        } finally {
            this.datastore.getSQLDatabase().endTransaction();
        }
    }

    protected Attachment getAttachment(BasicDocumentRevision rev, String attachmentName) {
        try {
            Cursor c = datastore.getSQLDatabase().rawQuery(SQL_ATTACHMENTS_SELECT,
                    new String[]{attachmentName, String.valueOf(rev.getSequence())});
            if (c.moveToFirst()) {
                int sequence = c.getInt(0);
                byte[] key = c.getBlob(2);
                String type = c.getString(3);
                int encoding = c.getInt(4);
                int revpos = c.getInt(7);
                File file = fileFromKey(key);
                return new SavedAttachment(attachmentName, revpos, sequence, key, type, file, Attachment.Encoding.values()[encoding]);
            }
            return null;
        } catch (SQLException e) {
            return null;
        }
    }

    protected List<? extends Attachment> attachmentsForRevision(long sequence) {
        try {
            LinkedList<SavedAttachment> atts = new LinkedList<SavedAttachment>();
            Cursor c = datastore.getSQLDatabase().rawQuery(SQL_ATTACHMENTS_SELECT_ALL,
                    new String[]{String.valueOf(sequence)});
            while (c.moveToNext()) {
                String name = c.getString(1);
                byte[] key = c.getBlob(2);
                String type = c.getString(3);
                int encoding = c.getInt(4);
                int revpos = c.getInt(7);
                File file = fileFromKey(key);
                atts.add(new SavedAttachment(name, revpos, sequence, key, type, file, Attachment.Encoding.values()[encoding]));
            }
            return atts;
        } catch (SQLException e) {
            return null;
        }
    }

    private void copyCursorValuesToNewSequence(Cursor c, long newSequence) {
        while (c.moveToNext()) {
            String filename = c.getString(1);
            byte[] key = c.getBlob(2);
            String type = c.getString(3);
            int encoding = c.getInt(4);
            int length = c.getInt(5);
            int encoded_length = c.getInt(6);
            int revpos = c.getInt(7);

            ContentValues values = new ContentValues();
            values.put("sequence", newSequence);
            values.put("filename", filename);
            values.put("key", key);
            values.put("type", type);
            values.put("encoding", encoding);
            values.put("length", length);
            values.put("encoded_length", encoded_length);
            values.put("revpos", revpos);
            datastore.getSQLDatabase().insert("attachments", values);
        }
    }

    /**
     * Called by BasicDatastore to copy one attachment to a new revision
     * @param parentSequence
     */
    protected void copyAttachment(long parentSequence, long newSequence, String filename) throws SQLException {
        Cursor c = datastore.getSQLDatabase().rawQuery(SQL_ATTACHMENTS_SELECT,
                new String[]{filename, String.valueOf(parentSequence)});
        copyCursorValuesToNewSequence(c, newSequence);
    }

    /**
     * Called by BasicDatastore to copy attachments to a new revision
     * @param parentSequence
     */
    protected void copyAttachments(long parentSequence, long newSequence) throws SQLException {
        Cursor c = datastore.getSQLDatabase().rawQuery(SQL_ATTACHMENTS_SELECT_ALL,
                new String[]{String.valueOf(parentSequence)});
        copyCursorValuesToNewSequence(c, newSequence);
    }

    protected void purgeAttachments() {
        // it's easier to deal with Strings since java doesn't know how to compare byte[]s
        Set<String> currentKeys = new HashSet<String>();
        try {
            // delete attachment table entries for revs which have been purged
            datastore.getSQLDatabase().delete("attachments", "sequence IN " +
                "(SELECT sequence from revs WHERE json IS null)", null);
            // get all keys from attachments table for leaf nodes
            Cursor c = datastore.getSQLDatabase().rawQuery(SQL_ATTACHMENTS_SELECT_ALL_KEYS, null);
            while (c.moveToNext()) {
                byte[] key = c.getBlob(0);
                currentKeys.add(keyToString(key));
            }
            // iterate thru attachments dir
            for (File f : new File(attachmentsDir).listFiles()) {
                // if file isn't in the keys list, delete it
                String keyForFile = f.getName();
                if (!currentKeys.contains(keyForFile)) {
                    try {
                        boolean deleted = f.delete();
                        if (!deleted) {
                            Log.w(LOG_TAG, "Could not delete file from BLOB store: " + f.getAbsolutePath());
                        }
                    } catch (SecurityException e) {
                        Log.w(LOG_TAG, "SecurityException when trying to delete file from BLOB store: " + f.getAbsolutePath() + ", "+e);
                    }
                }
            }
        } catch (SQLException e) {
            Log.e(LOG_TAG, "Problem in purgeAttachments, executing SQL to fetch all attachment keys "+e);
        }
    }

    private String keyToString(byte[] key) {
        return new String(new Hex().encode(key));
    }

    private File fileFromKey(byte[] key) {
        File file = new File(attachmentsDir, keyToString(key));
        return file;
    }
}

