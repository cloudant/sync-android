package com.cloudant.sync.datastore;

import java.io.File;

/**
 * Created by tomblench on 11/03/2014.
 */
public class UnsavedFileAttachment extends Attachment {

    public UnsavedFileAttachment(File file, String type) {
        this.file = file;
        this.type = type;
    }

    final File file;
    final String type; // mime type

}
