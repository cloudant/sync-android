package com.cloudant.sync.datastore;

import java.io.InputStream;

/**
 * Created by tomblench on 11/03/2014.
 */
public abstract class Attachment {

    public String name;
    public String type; // mime type
    public long size;

    public abstract InputStream getInputStream();

}
