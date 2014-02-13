package com.cloudant.sync.notifications;

/**
 * <p>Event for database closed</p>
 *
 * <p>This event is posted by {@link com.cloudant.sync.datastore.Datastore#close()}</p>
 */
public class DatabaseClosed extends DatabaseModified {

    /**
     * <p>Event for database closed</p>
     *
     * @param dbName
     *            The name of the Datastore that was closed
     */
    public DatabaseClosed(String dbName) {
        super(dbName);
    }
}