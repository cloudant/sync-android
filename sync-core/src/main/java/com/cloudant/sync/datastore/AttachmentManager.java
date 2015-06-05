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

import com.cloudant.sync.sqlite.ContentValues;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.CouchUtils;
import com.cloudant.sync.util.DatabaseUtils;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileExistsException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An AttachmentManager handles attachment related tasks: adding, removing and retrieving
 * attachments for documents from disk. It handles both disk read/write and managing the
 * attachment related tables in the datastore's database.
 *
 * Attachments are stored on disk in an extension directory, {@code EXTENSION_NAME}.
 */
class AttachmentManager {

    private static final String EXTENSION_NAME = "com.cloudant.attachments";
    private static final Logger logger = Logger.getLogger(AttachmentManager.class.getCanonicalName());

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

    /**
     * Name of database mapping key to filename.
     */
    public static final String ATTACHMENTS_KEY_FILENAME = "attachments_key_filename";
    /**
     * SQL statement to look up filename for key.
     */
    public static final String SQL_FILENAME_LOOKUP_QUERY = String.format(
            "SELECT filename FROM %1$s WHERE key=?", ATTACHMENTS_KEY_FILENAME);
    /**
     * SQL statement to return all key,filename mappings.
     */
    public static final String SQL_ATTACHMENTS_SELECT_KEYS_FILENAMES = String.format(
            "SELECT key,filename FROM %1$s", ATTACHMENTS_KEY_FILENAME);
    /**
     * Random number generator used to generate filenames.
     */
    private static final Random filenameRandom = new Random();

    public final String attachmentsDir;

    private final AttachmentStreamFactory attachmentStreamFactory;

    public AttachmentManager(BasicDatastore datastore) {
        this.attachmentsDir = datastore.extensionDataFolder(EXTENSION_NAME);
        this.attachmentStreamFactory = new AttachmentStreamFactory(datastore.getKeyProvider());
    }

    public void addAttachment(SQLDatabase db,PreparedAttachment a, BasicDocumentRevision rev) throws  AttachmentNotSavedException {

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
        long length = a.length;
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
        db.delete("attachments", " filename = ? and sequence = ? ", new String[]{filename,
                String.valueOf(sequence)});
        long result = db.insert("attachments", values);
        if (result == -1) {
            // if we can't insert into DB then don't copy the attachment
            a.tempFile.delete();
            throw new AttachmentNotSavedException("Could not insert attachment " + a + " into database with values " + values + "; not copying to attachments directory");
        }
        // move file to blob store, with file name based on sha1
        File newFile = null;
        try {
            newFile = fileFromKey(db, sha1);
        } catch (AttachmentException ex) {
            // As we couldn't generate a filename, we can't save the attachment. Clean up
            // temporary file and throw an exception.
            if (a.tempFile.exists()){
                a.tempFile.delete();
            }
            throw new AttachmentNotSavedException("Couldn't generate name for new attachment", ex);
        }

        try {
            FileUtils.moveFile(a.tempFile, newFile);
        } catch (FileExistsException fee) {
            // File with same SHA1 hash in the store, we assume it's the same content so can discard
            // the duplicate data we have just downloaded
            a.tempFile.delete();
        } catch (FileNotFoundException e) {
            //If we had an error moving the file, but we already have a copy of the attachment in
            // the datastore, we can ignore the problem as a copy of the data is already in the
            // datastore. Otherwise, we must throw an exception.
            if(!newFile.exists()){
                throw new AttachmentNotSavedException(e);
            }
        } catch (IOException e) {
            //We have errored while moving the file, we should clean up after ourselves before
            //throwing an attachment exception
            if(newFile.exists()){
                newFile.delete();
            }
            if (a.tempFile.exists()){
                a.tempFile.delete();
            }
            throw new AttachmentNotSavedException(e);
        }

    }

    /**
     * Creates a PreparedAttachment from {@code att}, preparing it for insertion into
     * the datastore.
     *
     * @param att Attachment to prepare for insertion into datastore
     * @return PreparedAttachment, which can be used in addAttachment methods
     * @throws AttachmentException if there was an error preparing the attachment, e.g., reading
     *                  attachment data.
     */
    public PreparedAttachment prepareAttachment(Attachment att)
            throws AttachmentException {
        return new PreparedAttachment(
                att, this.attachmentsDir, this.attachmentStreamFactory);
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
    protected PreparedAndSavedAttachments prepareAttachments(Collection<? extends Attachment> attachments) throws AttachmentException {
        // actually a list of prepared or saved attachments
        PreparedAndSavedAttachments preparedAndSavedAttachments = new PreparedAndSavedAttachments();

        if (attachments == null || attachments.size() == 0) {
            // nothing to do
            return null;
        }

        for (Attachment a : attachments) {
            if (!(a instanceof SavedAttachment)) {
                preparedAndSavedAttachments.preparedAttachments.add(
                        new PreparedAttachment(a, this.attachmentsDir, this.attachmentStreamFactory));
            } else {
                preparedAndSavedAttachments.savedAttachments.add((SavedAttachment)a);
            }
        }

        return preparedAndSavedAttachments;
    }

    protected void setAttachments(SQLDatabase db,BasicDocumentRevision rev, PreparedAndSavedAttachments preparedAndSavedAttachments) throws AttachmentNotSavedException, DatastoreException {

        // set attachments for revision:
        // * prepared attachments are added
        // * copy existing attachments forward to the next revision

        if (preparedAndSavedAttachments == null || preparedAndSavedAttachments.isEmpty()) {
            // nothing to do
            return;
        }

        try {
            for (PreparedAttachment a : preparedAndSavedAttachments.preparedAttachments) {
                // go thru prepared attachments and add them
                this.addAttachment(db,a, rev);
            }
            for (SavedAttachment a : preparedAndSavedAttachments.savedAttachments) {
                // go thru existing (from previous rev) and new (from another document) saved attachments
                // and add them (the effect on existing attachments is to copy them forward to this revision)
                long parentSequence = a.seq;
                long newSequence = rev.getSequence();
                this.copyAttachment(db,parentSequence, newSequence, a.name);
            }
        } catch (SQLException sqe) {
            throw new DatastoreException("SQLException setting attachment for rev"+rev, sqe);
        }
    }

    protected Attachment getAttachment(SQLDatabase db, BasicDocumentRevision rev, String attachmentName) throws AttachmentException {
        Cursor c = null;
        try {
             c = db.rawQuery(SQL_ATTACHMENTS_SELECT,
                     new String[]{attachmentName, String.valueOf(rev.getSequence())});
            if (c.moveToFirst()) {
                int sequence = c.getInt(0);
                byte[] key = c.getBlob(2);
                String type = c.getString(3);
                int encoding = c.getInt(4);
                int revpos = c.getInt(7);
                File file = fileFromKey(db, key);
                return new SavedAttachment(attachmentName, revpos, sequence, key, type, file,
                        Attachment.Encoding.values()[encoding], this.attachmentStreamFactory);
            }

            return null;
        } catch (SQLException e) {
            logger.log(Level.SEVERE,
                    String.format("Failed to get %1$s for doc %2$s", rev.getId(), attachmentName),
                    e);
            throw new AttachmentException(e);
        } finally {
            DatabaseUtils.closeCursorQuietly(c);
        }
    }

    protected List<? extends Attachment> attachmentsForRevision(SQLDatabase db, long sequence) throws AttachmentException {
        Cursor c = null;
        try {
            LinkedList<SavedAttachment> atts = new LinkedList<SavedAttachment>();
            c = db.rawQuery(SQL_ATTACHMENTS_SELECT_ALL,
                    new String[]{String.valueOf(sequence)});
            while (c.moveToNext()) {
                String name = c.getString(1);
                byte[] key = c.getBlob(2);
                String type = c.getString(3);
                int encoding = c.getInt(4);
                int revpos = c.getInt(7);
                File file = fileFromKey(db, key);
                atts.add(new SavedAttachment(name, revpos, sequence, key, type, file,
                        Attachment.Encoding.values()[encoding], this.attachmentStreamFactory));
            }
            return atts;
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Failed to get attachments", e);
            throw new AttachmentException(e);
        } finally {
            DatabaseUtils.closeCursorQuietly(c);
        }
    }

    private void copyCursorValuesToNewSequence(SQLDatabase db, Cursor c, long newSequence) {
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
            db.insert("attachments", values);
        }
    }

    /**
     * Copy a single attachment for a given revision to a new revision.
     *
     * @param db database to use
     * @param parentSequence identifies sequence number of revision to copy attachment data from
     * @param newSequence identifies sequence number of revision to copy attachment data to
     * @param filename filename of attachment to copy
     */
    protected void copyAttachment(SQLDatabase db, long parentSequence, long newSequence, String filename) throws SQLException {
        Cursor c = null;
        try{
            c = db.rawQuery(SQL_ATTACHMENTS_SELECT,
                    new String[]{filename, String.valueOf(parentSequence)});
            copyCursorValuesToNewSequence(db,c, newSequence);
        }finally{
            DatabaseUtils.closeCursorQuietly(c);
        }
    }

    /**
     * Copy all attachments for a given revision to a new revision.
     *
     * @param db database to use
     * @param parentSequence identifies sequence number of revision to copy attachment data from
     * @param newSequence identifies sequence number of revision to copy attachment data to
     */
    protected void copyAttachments(SQLDatabase db, long parentSequence, long newSequence) throws DatastoreException {
        Cursor c = null;
        try {
             c = db.rawQuery(SQL_ATTACHMENTS_SELECT_ALL,
                    new String[]{String.valueOf(parentSequence)});
            copyCursorValuesToNewSequence(db, c, newSequence);
        } catch (SQLException e){
            throw new DatastoreException("Failed to copy attachments", e);
        }finally {
            DatabaseUtils.closeCursorQuietly(c);
        }
    }

    /**
     * Called by BasicDatastore on the execution queue, this needs have the db passed ot it
     * @param db database to purge attachments from
     */
    protected void purgeAttachments(SQLDatabase db) {
        // it's easier to deal with Strings since java doesn't know how to compare byte[]s
        Set<String> currentKeys = new HashSet<String>();
        Cursor c = null;
        try {
            // delete attachment table entries for revs which have been purged
            db.delete("attachments", "sequence IN " +
                    "(SELECT sequence from revs WHERE json IS null)", null);

            // get all keys from attachments table
            c = db.rawQuery(SQL_ATTACHMENTS_SELECT_ALL_KEYS, null);
            while (c.moveToNext()) {
                byte[] key = c.getBlob(0);
                currentKeys.add(keyToString(key));
            }

            // Now iterate through the attachments_key_filename, deleting items we need to
            // (both db row and file on disk).
            File attachments = new File(attachmentsDir);
            c = db.rawQuery(SQL_ATTACHMENTS_SELECT_KEYS_FILENAMES, null);
            while (c.moveToNext()) {
                String keyForFile = c.getString(0);

                if (!currentKeys.contains(keyForFile)) {
                    File f = new File(attachments, c.getString(1));
                    try {
                        boolean deleted = f.delete();
                        if (deleted) {
                            db.delete(ATTACHMENTS_KEY_FILENAME, "key = ?", new String[]{keyForFile});
                        } else {
                            logger.warning("Could not delete file from BLOB store: " +
                                    f.getAbsolutePath());
                        }
                    } catch (SecurityException e) {
                        logger.log(Level.WARNING, "SecurityException when trying to delete file " +
                                "from BLOB store: " + f.getAbsolutePath(), e);
                    }
                }
            }

        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Problem in purgeAttachments, executing SQL to fetch all attachment keys ", e);
        }finally {
            DatabaseUtils.closeCursorQuietly(c);
        }
    }

    private static String keyToString(byte[] key) {
        return new String(new Hex().encode(key));
    }

    /**
     * Lookup or create a on disk File representation of blob, in {@code db} using {@code key}.
     *
     * Existing attachments will have an entry in db, so the method just looks this up
     * and returns the File object for the path.
     *
     * For new attachments, the {@code key} doesn't already have an associated filename,
     * one is generated and inserted into the database before returning a File object
     * for blob associated with {@code key}.
     *
     * @param db database to use.
     * @param key key to lookup filename for.
     * @return File object for blob associated with {@code key}.
     */
    File fileFromKey(SQLDatabase db, byte[] key) throws AttachmentException {

        String keyString = keyToString(key);
        String filename = null;

        db.beginTransaction();
        try {
            Cursor c = db.rawQuery(SQL_FILENAME_LOOKUP_QUERY, new String[]{ keyString });
            if (c.moveToFirst()) {
                filename = c.getString(0);
                System.out.println(String.format("FOUND filename %1$s for key %2$s", filename, keyString));
            } else {
                filename = generateFilenameForKey(db, keyString);
                System.out.println(String.format("ADDED filename %1$s for key %2$s", filename, keyString));
            }
            c.close();
            db.setTransactionSuccessful();
        } catch (SQLException e) {
            e.printStackTrace();
            filename = null;
        } finally {
            db.endTransaction();
        }

        if (filename != null) {
            return new File(attachmentsDir, filename);
        } else {
            // generateFilenameForKey throws an exception if we couldn't generate, this
            // means we couldn't get one from the database.
            throw new AttachmentException("Couldn't retrieve filename for attachment");
        }
    }


    /**
     * Iterate candidate filenames generated from the filenameRandom generator
     * until we find one which doesn't already exist.
     *
     * We try inserting the new record into attachments_key_filename to find a
     * unique filename rather than checking on disk filenames. This is because we
     * can make use of the fact that this method is called on a serial database
     * queue to make sure our name is unique, whereas we don't have that guarantee
     * for on-disk filenames. This works because filename is declared
     * UNIQUE in the attachments_key_filename table.
     *
     * We allow up to 200 random name generations, which should give us many millions
     * of files before a name fails to be generated and makes sure this method doesn't
     * loop forever.
     *
     * @param db database to use
     * @param keyString blobs key
     */
    static String generateFilenameForKey(SQLDatabase db, String keyString) throws NameGenerationException {

        String filename = null;

        long result = -1;  // -1 is error for insert call
        int tries = 0;
        while (result == -1 && tries < 200) {
            byte[] randomBytes = new byte[20];
            filenameRandom.nextBytes(randomBytes);
            String candidate = keyToString(randomBytes);

            ContentValues contentValues = new ContentValues();
            contentValues.put("key", keyString);
            contentValues.put("filename", candidate);
            result = db.insert(ATTACHMENTS_KEY_FILENAME, contentValues);

            if (result != -1) {  // i.e., insert worked, filename unique
                filename = candidate;
            }

            tries++;
        }

        if (filename != null) {
            return filename;
        } else {
            throw new NameGenerationException("Couldn't generate unique filename for attachment");
        }
    }

    public static class NameGenerationException extends AttachmentException {

        NameGenerationException(String msg) {
            super(msg);
        }

    }
}

