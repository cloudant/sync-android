package com.cloudant.sync.datastore;

import java.io.InputStream;

/**
 * Created by tomblench on 11/03/2014.
 */
public abstract class Attachment {

    String name;
    String type; // mime type
    long size;

    abstract InputStream getInputStream();

}
