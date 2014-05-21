package com.cloudant.sync.notifications;

public class DatabaseModified {

    /**
     * Generic event for database create/delete
     * 
     * @param dbName
     *            The name of the Datastore that was created or deleted
     */
    public DatabaseModified(String dbName) {
        this.dbName = dbName;
    }

    public final String dbName;
}
