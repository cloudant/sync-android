package com.cloudant.sync.notifications;

/**
 * <p>Event for database delete</p>
 *
 * <p>This event is posted by
 * {@link com.cloudant.sync.datastore.DatastoreManager#deleteDatastore(String) daleteDatastore(String)}</p>
 */
public class DatabaseDeleted extends DatabaseModified {

    /**
     * Event for database delete
     * 
     * @param dbName
     *            The name of the Datastore that was deleted
     */
    public DatabaseDeleted(String dbName) {
        super(dbName);
    }
}
