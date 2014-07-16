package com.cloudant.sync.notifications;

/**
 * <p>Event for database created, it is only posted when the database
 * is created on the disk.</p>
 *
 *
 * <p>This event is posted by
 * {@link com.cloudant.sync.datastore.DatastoreManager#openDatastore(String) openDatastore(String)}
 * </p>
 *
 */
public class DatabaseCreated extends DatabaseModified {

    /**
     * Event for database created for the first time.
     *
     * @param dbName
     *            The name of the datastore that was created
     */
    public DatabaseCreated(String dbName) {
        super(dbName);
    }

}
