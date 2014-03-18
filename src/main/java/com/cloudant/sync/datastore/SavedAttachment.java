package com.cloudant.sync.datastore;

import java.io.File;

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

    public final String name;
    public final long revpos;
    public final long seq;
    public final byte[] key;  // sha of file, used for file path on disk.
    public final String type;
    public final File file;

}
