package com.cloudant.sync.datastore;

/**
 * Created by tomblench on 28/04/2014.
 */

import java.io.IOException;
import java.io.InputStream;

/**
 * An Attachment which is read from a stream before saving to the database
 */
public class UnsavedStreamAttachment extends Attachment {

    public UnsavedStreamAttachment(InputStream stream, String name, String type) {
        this.stream = stream;
        this.name = name;
        this.type = type;
    }

    public InputStream getInputStream() throws IOException {
        return stream;
    }

    private InputStream stream;

}
