package com.cloudant.sync.datastore;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by tomblench on 11/03/2014.
 */
public class UnsavedFileAttachment extends Attachment {

    public UnsavedFileAttachment(File file, String type) {
        this.name = file.getName();
        try {
            this.inputStream = new FileInputStream(file);
        } catch (IOException ioe) {
            System.out.println("problem setting inputstream "+ioe);
        }
        this.type = type;
    }

    public UnsavedFileAttachment(InputStream inputStream, String name, String type) {
        this.name = name;
        this.inputStream = inputStream;
        this.type = type;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    private InputStream inputStream;
}
