package com.cloudant.sync.datastore;

import com.cloudant.common.Log;
import com.cloudant.sync.sqlite.ContentValues;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.util.CouchUtils;
import com.cloudant.sync.util.Misc;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by tomblench on 14/03/2014.
 */
public class AttachmentManager {

    private static final String LOG_TAG = "AttachmentManager";

    private static final String EXTENSION_NAME = "com.cloudant.attachments";

    private static final String SQL_ATTACHMENTS_SELECT = "SELECT sequence, filename, key, type, length, revpos " +
            " FROM attachments " +
            " WHERE filename = ? and sequence = ?";

    private static final String SQL_ATTACHMENTS_SELECT_ALL = "SELECT sequence, filename, key, type, length, revpos " +
            " FROM attachments " +
            " WHERE sequence = ?";

    private String attachmentsDir;

    private BasicDatastore datastore;

    private enum Encoding {
        Plain
    }

    public AttachmentManager(BasicDatastore datastore) {
        this.datastore = datastore;
        this.attachmentsDir = datastore.extensionDataFolder(EXTENSION_NAME);
    }

    public DocumentRevision updateAttachments(DocumentRevision rev, List<? extends Attachment> attachments) throws ConflictException, IOException {
        // add attachments and then return new revision

        // make a new rev for the version with attachments
        DocumentRevision newDocument = datastore.updateDocument(rev.getId(), rev.getRevision(), rev.getBody());

        // save new (unmodified) revision which will have new _attachments when synced
        // ...
        // get these properties:
        // length
        // content type
        // digests sha1 and md5
        // ...
        // save to blob store
        // ...
        // add attachment to db linked to this revision

        for (Attachment a : attachments) {
                byte[] md5 = Misc.getMd5(a.getInputStream());

                ContentValues values = new ContentValues();
                long sequence = newDocument.getSequence();
                String filename = a.name;
                byte[] sha1 = Misc.getSha1(a.getInputStream());
                String type = a.type;
                int encoding = Encoding.Plain.ordinal();
                long length = a.size;
                long revpos = CouchUtils.generationFromRevId(newDocument.getRevision());

                values.put("sequence", sequence);
                values.put("filename", filename);
                values.put("key", sha1);
                values.put("type", type);
                //values.put("encoding", encoding);
                values.put("length", length);
                //values.put("encoded_length", length);
                values.put("revpos", revpos);

                long result = datastore.getSQLDatabase().insert("attachments", values);
                if (result == -1) {
                    // if we can't insert into DB then don't copy the attachment
                    Log.e(LOG_TAG, "Could not insert attachment " + a + " into database; not copying to attachments directory");
                    continue;
                }
                // move file to blob store, with file name based on sha1
                File newFile = fileFromKey(sha1);
                FileUtils.copyInputStreamToFile(a.getInputStream(), newFile);
        }
        return newDocument;
    }

    public Attachment getAttachment(DocumentRevision rev, String attachmentName) {
        try {
            Cursor c = datastore.getSQLDatabase().rawQuery(SQL_ATTACHMENTS_SELECT, new String[]{attachmentName, String.valueOf(rev.getSequence())});
            if (c.moveToFirst()) {
                int sequence = c.getInt(0);
                byte[] key = c.getBlob(2);
                String type = c.getString(3);
                int length = c.getInt(4);
                int revpos = c.getInt(5);
                File file = fileFromKey(key);
                return new SavedAttachment(attachmentName, revpos, sequence, key, type, file);
            }
            return null;
        } catch (SQLException e) {
            return null;
        }
    }

    public List<? extends Attachment> attachmentsForRevision(DocumentRevision rev) {
        return this.attachmentsForRevision(rev.getSequence());
    }

    public List<? extends Attachment> attachmentsForRevision(long sequence) {
        try {
            LinkedList<SavedAttachment> atts = new LinkedList<SavedAttachment>();
            Cursor c = datastore.getSQLDatabase().rawQuery(SQL_ATTACHMENTS_SELECT_ALL, new String[]{String.valueOf(sequence)});
            while (c.moveToNext()) {
                String name = c.getString(1);
                byte[] key = c.getBlob(2);
                String type = c.getString(3);
                int length = c.getInt(4);
                int revpos = c.getInt(5);
                File file = fileFromKey(key);
                atts.add(new SavedAttachment(name, revpos, sequence, key, type, file));
            }
            return atts;
        } catch (SQLException e) {
            return null;
        }
    }

    public DocumentRevision removeAttachments(DocumentRevision rev, String[] attachmentNames) throws ConflictException {

        int nDeleted = 0;

        for (String attachmentName : attachmentNames) {
            // first see if it exists
            SavedAttachment a = (SavedAttachment) this.getAttachment(rev, attachmentName);
            if (a == null) {
                continue;
            }
            // get the file in blob store
            File f = this.fileFromKey(a.key);

            // delete att from database table
            datastore.getSQLDatabase().delete("attachments", " filename = ? and sequence = ? ", new String[]{attachmentName, String.valueOf(rev.getSequence())});

            // delete attachments from blob store
            f.delete();

            nDeleted++;
        }

        if (nDeleted > 0) {
            // return a new rev for the version with attachment removed
            return datastore.updateDocument(rev.getId(), rev.getRevision(), rev.getBody());
        }

        // nothing deleted, just return the same rev
        return rev;
    }

        protected File fileFromKey(byte[] key) {
        File file = new File(attachmentsDir, new String(new Hex().encode(key)));
        return file;
    }

}

