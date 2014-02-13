package com.cloudant.sync.notifications;

/**
 * <p>Event for database opened.</p>
 *
 * <p>The event is only posted the first time a database is opened for
 * a given {@link com.cloudant.sync.datastore.DatastoreManager}</p>
 *
 * <p>When closed Database (by calling {@link com.cloudant.sync.datastore.Datastore#close()})
 * is opened again, the event is also fired.</p>
 *
 * <p>This event is posted by
 * {@link com.cloudant.sync.datastore.DatastoreManager#openDatastore(String) openDatastore(String)}
 * </p>
 */
public class DatabaseOpened extends DatabaseModified {

    /**
     * Event for database opened.
     *
     * @param dbName
     *            The name of the datastore that was opened
     */
    public DatabaseOpened(String dbName) {
        super(dbName);
    }

}
