/*
 * Copyright (c) 2016 IBM Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.datastore;

import com.cloudant.sync.datastore.encryption.KeyProvider;
import com.cloudant.sync.datastore.encryption.NullKeyProvider;
import com.cloudant.sync.event.EventBus;
import com.cloudant.sync.notifications.DatabaseClosed;
import com.cloudant.sync.notifications.DatabaseCreated;
import com.cloudant.sync.notifications.DatabaseDeleted;
import com.cloudant.sync.notifications.DatabaseOpened;
import com.cloudant.sync.query.IndexManager;
import com.cloudant.sync.query.IndexManagerImpl;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 * Class representing a local store of JSON documents, attachments, query indexes, etc.
 * </p>
 *
 * <p>
 * Users should create new stores or open existing stores using the static {@link #getInstance(File)}
 * or {@link #getInstance(File, KeyProvider)} methods.
 * </p>
 *
 * <p>
 * Users can perform CRUD (create, read, update, delete) operations on JSON documents and attachments
 * by invoking methods on the {@link #database} member.
 * </p>
 *
 * <p>
 * Users can perform index and query operations on JSON documents by invoking methods on the
 * {@link #query} member.
 * </p>
 *
 * <p>
 * In most cases, users are advised to open store instances and use them for the entire lifetime of
 * their application. This is more efficient than opening and closing instances for the same
 * underlying file system location multiple times throughout the lifetime of the application.
 * Users should call {@link #close()} after using a store to shut down in an orderly manner and free
 * up resources.
 * </p>
 *
 * @api_public
 */
public class DocumentStore {

    private static final String EXTENSIONS_LOCATION_NAME = "extensions";

    public final Database database;
    public final IndexManager query;
    protected final String databaseName; // only used for events
    private final File location; // needed for close/delete
    private final File extensionsLocation; // for extensions: currently attachments and indexes

    /* This map should only be accessed inside synchronized(openDatastores) blocks */
    private static final Map<File, DocumentStore> documentStores = new HashMap<File, DocumentStore>();

    private static EventBus eventBus = new EventBus();

    private static final Logger logger = Logger.getLogger(DocumentStore.class.getCanonicalName());

    private DocumentStore(File location, KeyProvider keyProvider) throws DatastoreException, IOException, SQLException {
        try {
            this.location = location;
            this.extensionsLocation = new File(location, EXTENSIONS_LOCATION_NAME);
            this.databaseName = location.toString();
            this.database = new DatabaseImpl(location, extensionsLocation, keyProvider);
            this.query = new IndexManagerImpl(database, extensionsLocation, keyProvider);
        } catch (DatastoreException e) {
            closeQuietlyOnException();
            throw e;
        } catch (IOException e) {
            closeQuietlyOnException();
            throw e;
        } catch (SQLException e) {
            closeQuietlyOnException();
            throw e;
        }
    }

    private void closeQuietlyOnException() {
        if (database != null) {
            ((DatabaseImpl) database).close();
        }
        if (query != null) {
            ((IndexManagerImpl) query).close();
        }
    }

    /**
     * <p>
     * Get an instance of an existing or newly created store.
     * </p>
     * <p>
     * Equivalent to calling {@link #getInstance(File, KeyProvider)} getInstance} with a
     * {@link NullKeyProvider}.
     * </p>
     * @param location The location on the file system where the underlying files should be stored.
     *                 Must be a directory.
     * @return An existing or newly created store.
     * @throws DatastoreNotOpenedException if the database located at {@code location} cannot be
     *                                     opened (if it already exists) or created.
     */
    public static DocumentStore getInstance(File location) throws
        DatastoreNotOpenedException {
        return getInstance(location, new NullKeyProvider());
    }

    /**
     * <p>
     * Get an instance of an existing or newly created store.
     * </p>
     * <p>
     * If encryption is enabled for this platform, passing a {@link KeyProvider} containing a non-null
     * key will open or create an encrypted database. If opening a database, the key used to
     * create the database must be used. If encryption is not enabled for this platform, returning
     * a non-null key will result in an exception.
     * </p>
     * @param location The location on the file system where the underlying files should be stored.
     *                 Must be a directory.
     * @param provider KeyProvider object. Use a {@link NullKeyProvider} if the database shouldn't
     *                 be encrypted.
     * @return An existing or newly created store.
     * @throws DatastoreNotOpenedException if the database located at {@code location} cannot be
     *                                     opened (if it already exists) or created.
     */
    public static DocumentStore getInstance(File location, KeyProvider provider) throws DatastoreNotOpenedException {
        try {
            synchronized (documentStores) {
                DocumentStore ds = documentStores.get(location);
                // See if the file exists already, so we can raise the right event.
                // SQLDatabaseFactory.openOrCreateSQLDatabase will create directories and file if
                // required.
                boolean created = !location.exists();
                if (ds == null) {
                    ds = new DocumentStore(location, provider);
                    documentStores.put(location, ds);
                    if (created) {
                        eventBus.post(new DatabaseCreated(ds.databaseName));
                    }
                    eventBus.post(new DatabaseOpened(ds.databaseName));
                }
                return ds;
            }
        }
        catch (IOException e) {
            throw new DatastoreNotOpenedException("Database not found: " + location, e);
        } catch (SQLException e) {
            // thrown during schema upgrades, etc
            throw new DatastoreNotOpenedException("Database not initialized correctly: " + location, e);
        } catch (IllegalArgumentException e) {
            // thrown by SQLDatabaseFactory if directory is not writable
            throw new DatastoreNotOpenedException("Database location not accessible: " + location, e);
        } catch (DatastoreNotOpenedException e) {
            throw e;
        } catch (DatastoreException e) {
            throw new DatastoreNotOpenedException("Datastore not initialized correctly: " + location, e);
        }
    }

    @SuppressWarnings("unchecked")
    public void close() {
        synchronized (documentStores) {
            DocumentStore ds = documentStores.remove(location);
            if (ds != null) {
                ((DatabaseImpl)database).close();
                ((IndexManagerImpl)query).close();
            }
            else {
                throw new IllegalStateException("DocumentStore "+location+" already closed");
            }
        }
        eventBus.post(new DatabaseClosed(databaseName));
    }

    public void delete() throws DatastoreNotDeletedException {
        try {
            this.close();
        } catch (Exception e) {
            // caught exception could be something benign like datastore already closed, so just log
            logger.log(Level.WARNING, "Caught exception whilst closing DocumentStore in delete()", e);
        }
        if (!location.exists()) {
            String msg = String.format("Datastore %s doesn't exist on disk", location);
            logger.warning(msg);
            throw new DatastoreNotDeletedException(msg);
        } else {
            try {
                FileUtils.deleteDirectory(location);
            } catch (IOException ioe) {
                String msg = String.format("Datastore %s not deleted", location);
                logger.log(Level.WARNING, msg, ioe);
                throw new DatastoreNotDeletedException(msg, ioe);
            }
            eventBus.post(new DatabaseDeleted(databaseName));
        }
    }

    /**
     * <p>Returns the EventBus which this Datastore posts
     * {@link com.cloudant.sync.notifications.DatabaseModified Database Notification Events} to.</p>
     * @return the DocumentStore's EventBus
     *
     * @see <a href="https://github.com/cloudant/sync-android/blob/master/doc/events.md">
     *     Events documentation</a>
     */
    public static EventBus getEventBus() {
        return eventBus;
    }

}
