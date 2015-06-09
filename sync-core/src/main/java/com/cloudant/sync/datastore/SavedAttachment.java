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

import com.cloudant.sync.replication.PushAttachmentsInline;
import com.cloudant.sync.sqlite.Cursor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

/**
 * Created by tomblench on 14/03/2014.
 */

/**
 * An Attachment which has been retrieved from the Database
 */
class SavedAttachment extends Attachment {

    protected final long revpos;
    protected final long seq;
    protected final byte[] key;  // sha of file, used for file path on disk.
    protected final long length;
    protected final long encodedLength;
    private final File file;

    // how many bytes should an attachment be to be considered large?
    static final int largeSizeBytes = 65536;
    private static final Logger logger = Logger.getLogger(SavedAttachment.class.getCanonicalName());

    protected SavedAttachment(File f, Cursor c) {
        super(c.getString(c.getColumnIndex("filename")),
                c.getString(c.getColumnIndex("type")),
                Attachment.Encoding.values()[c.getInt(c.getColumnIndex("encoding"))]);

        this.file = f;
        this.revpos = c.getLong(c.getColumnIndex("revpos"));
        this.seq = c.getLong(c.getColumnIndex("sequence"));
        this.key = c.getBlob(c.getColumnIndex("key"));
        this.length = c.getLong(c.getColumnIndex("length"));
        this.encodedLength = c.getLong(c.getColumnIndex("encoded_length"));
    }

    public InputStream getInputStream() throws IOException {
        if (encoding == Encoding.Gzip) {
            return new GZIPInputStream(new FileInputStream(file));
        } else {
            return new FileInputStream(file);
        }
    }

    public boolean isLarge() {
        return this.onDiskLength() > largeSizeBytes;
    }

    public boolean shouldInline(PushAttachmentsInline inlinePreference) {
        // push_attachments_inline: false = always push multipart; small = push small attachments inline; true = always push inline
        if (inlinePreference == PushAttachmentsInline.False || inlinePreference == PushAttachmentsInline.Small && this.isLarge()) {
            logger.finer("inline false");
            return false;
        } else {
            logger.finer("inline true");
            return true;
        }
    }

    // size of file, as stored on disk
    // note that this may be different from file.length() due to encryption
    public long onDiskLength() {
        return encoding == Encoding.Plain ? length : encodedLength;
    }
}
