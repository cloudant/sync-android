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
import com.cloudant.sync.util.Misc;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileExistsException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

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
            " FROM attachments " +
            " WHERE filename = ? and sequence = ?";

    private static final String SQL_ATTACHMENTS_SELECT_ALL = "SELECT sequence, " +
            "filename, " +
            "key, " +
            "type, " +
            "encoding, " +
            "length, " +
            "encoded_length, " +
            "revpos " +
            " FROM attachments " +
            " WHERE sequence = ?";

    private static final String SQL_ATTACHMENTS_SELECT_ALL_KEYS = "SELECT key " +
            " FROM attachments";

    public final String attachmentsDir;

    private BasicDatastore datastore;

    public AttachmentManager(BasicDatastore datastore) {
        this.datastore = datastore;
        this.attachmentsDir = datastore.extensionDataFolder(EXTENSION_NAME);
    }

    public void addAttachment(PreparedAttachment a, DocumentRevision rev) throws IOException, SQLException {

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
        long length = a.attachment.getSize();
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

    protected DocumentRevision updateAttachments(DocumentRevision rev, List<? extends Attachment> attachments) throws ConflictException {

        // add attachments and then return new revision:
        // * save new (unmodified) revision which will have new _attachments when synced
        // * for each attachment, add attachment to db linked to this revision

        List<PreparedAttachment> preparedAttachments = new ArrayList<PreparedAttachment>();

        try {
            for (Attachment a : attachments) {
                preparedAttachments.add(new PreparedAttachment(a, this.attachmentsDir));
            }
        } catch (IOException e) {
            Log.e(LOG_TAG, "Failed to prepare attachment for rev "+rev+": "+e);
            return null;
        }

        try {
            this.datastore.getSQLDatabase().beginTransaction();

            DocumentRevision newDocument = datastore.updateDocument(rev.getId(),
                    rev.getRevision(),
                    rev.getBody());

            boolean ok = true;

            try {
                for (PreparedAttachment a : preparedAttachments) {
                    this.addAttachment(a, newDocument);
                }
            } catch (Exception e) {
                Log.e(LOG_TAG, "Failed to add attachment for rev "+rev+"; exception was "+e.getMessage());
                ok = false;
            }

            if (ok) {
                this.datastore.getSQLDatabase().setTransactionSuccessful();
            }

            if (ok) {
                return newDocument;
            } else {
                return null;
            }
        } finally {
            this.datastore.getSQLDatabase().endTransaction();
        }
    }

    protected Attachment getAttachment(DocumentRevision rev, String attachmentName) {
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

    protected List<? extends Attachment> attachmentsForRevision(DocumentRevision rev) {
        try {
            LinkedList<SavedAttachment> atts = new LinkedList<SavedAttachment>();
            long sequence = rev.getSequence();
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


    protected DocumentRevision removeAttachments(DocumentRevision rev, String[] attachmentNames)
            throws ConflictException {

        boolean rowsDeleted = false;

        // args looks like {attName_1, ..., attName_n, sequence}
        String[] args = new String[attachmentNames.length+1];
        System.arraycopy(attachmentNames, 0, args, 0, attachmentNames.length);
        args[args.length-1] = String.valueOf(rev.getSequence());

        rowsDeleted = datastore.getSQLDatabase().delete("attachments",
                String.format("filename in (%s) and sequence = ?",
                        SQLDatabaseUtils.makePlaceholders(attachmentNames.length)),
                args) > 0;

        if (!rowsDeleted) {
            Log.w(LOG_TAG, "No attachments were deleted for rev "+rev+" with attachmentNames "+ Arrays.toString(attachmentNames));
        }

        if (rowsDeleted) {
            // return a new rev for the version with attachment removed
            return datastore.updateDocument(rev.getId(), rev.getRevision(), rev.getBody());
        } else {
            // nothing deleted, just return the same rev
            return rev;
        }
    }

    protected void purgeAttachments() {
        // it's easier to deal with Strings since java doesn't know how to compare byte[]s
        Set<String> currentKeys = new HashSet<String>();
        try {
            // get all keys from attachments table
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

