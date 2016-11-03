package com.cloudant.sync.datastore;

/**
 * Exception thrown when an existing datastore cannot be opened or a new datastore cannot be created
 *
 * @api_public
 */

public class DatastoreNotOpenedException extends DatastoreException {

    public DatastoreNotOpenedException(String message){
        super(message);
    }

    public DatastoreNotOpenedException(Throwable causedBy){
        super(causedBy);
    }

    public DatastoreNotOpenedException(String message, Throwable causedBy){
        super(message,causedBy);
    }

}
