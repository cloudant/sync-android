package com.cloudant.sync.datastore;

/**
 * Created by tomblench on 02/11/2016.
 *
 * @api_public
 */

public class DatastoreNotDeletedException extends DatastoreException {

    public DatastoreNotDeletedException(String message){
        super(message);
    }

    public DatastoreNotDeletedException(Throwable causedBy){
        super(causedBy);
    }

    public DatastoreNotDeletedException(String message, Throwable causedBy){
        super(message,causedBy);
    }

}
