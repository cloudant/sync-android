package com.cloudant.sync.datastore;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

/**
 * Created by tomblench on 11/03/2014.
 */
public class UnsavedFileAttachment extends Attachment {

    public UnsavedFileAttachment(File file, String type) {
        this.name = file.getName();
        this.file = file;
        this.type = type;
    }

    public InputStream getInputStream() {
        try {
            return new FileInputStream(file);
        } catch (Exception e) {
            return null;
        }
    }

    private final File file;

}
