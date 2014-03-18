package com.cloudant.sync.datastore;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

/**
 * Created by tomblench on 14/03/2014.
 */
public class SavedAttachment extends Attachment {

    public SavedAttachment(String name, long revpos, long seq, byte[] key, String type, File file) {
        this.name = name;
        this.revpos = revpos;
        this.seq = seq;
        this.key = key;
        this.type = type;
        this.file = file;
    }

    public InputStream getInputStream() {
        try {
            return new FileInputStream(file);
        } catch (Exception e) {
            return null;
        }
    }

    protected final long revpos;
    protected final long seq;
    protected final byte[] key;  // sha of file, used for file path on disk.
    private final File file;

}
