/*
 * Copyright © 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.documentstore;

import com.cloudant.sync.documentstore.encryption.KeyProvider;
import com.cloudant.sync.documentstore.encryption.NullKeyProvider;
import com.cloudant.sync.event.EventBus;
import com.cloudant.sync.event.notifications.DocumentStoreClosed;
import com.cloudant.sync.event.notifications.DocumentStoreCreated;
import com.cloudant.sync.event.notifications.DocumentStoreDeleted;
import com.cloudant.sync.event.notifications.DocumentStoreModified;
import com.cloudant.sync.event.notifications.DocumentStoreOpened;
import com.cloudant.sync.internal.documentstore.DatabaseImpl;
import com.cloudant.sync.internal.query.QueryImpl;
import com.cloudant.sync.query.Query;

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
 * by invoking methods through {@link #database()}.
 * </p>
 *
 * <p>
 * Users can perform index and query operations on JSON documents by invoking methods through
 * {@link #query()}.
 * </p>
 *
 * <p>
 * In most cases, users are advised to obtain a single store instance via
 * {@link DocumentStore#getInstance} and use it for the entire lifetime of their application.
 * This is more efficient than opening and closing instances for the same underlying file system
 * location multiple times throughout the lifetime of the application.
 * Users should call {@link #close()} after using a store to shut down in an orderly manner and free
 * up resources.
 * </p>
 */
public class DocumentStore {

    private static final String EXTENSIONS_LOCATION_NAME = "extensions";

    private final DatabaseImpl database;
    private final Query query;
    protected final String databaseName; // only used for events
    private final File location; // needed for close/delete
    private final File extensionsLocation; // for extensions: currently attachments and indexes

    /* This map should only be accessed inside synchronized(documentStores) blocks */
    private static final Map<File, DocumentStore> documentStores = new HashMap<File, DocumentStore>();

    private static EventBus eventBus = new EventBus();

    private static final Logger logger = Logger.getLogger(DocumentStore.class.getCanonicalName());

    private DocumentStore(File location, KeyProvider keyProvider) throws DocumentStoreException, IOException, SQLException {
        try {
            this.location = location;
            this.extensionsLocation = new File(location, EXTENSIONS_LOCATION_NAME);
            this.databaseName = location.toString();
            this.database = new DatabaseImpl(location, extensionsLocation, keyProvider);
            this.query = new QueryImpl(database, extensionsLocation, keyProvider);
        } catch (DocumentStoreException e) {
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
     * @throws DocumentStoreNotOpenedException if the database located at {@code location} cannot be
     *                                     opened (if it already exists) or created.
     */
    public static DocumentStore getInstance(File location) throws
            DocumentStoreNotOpenedException {
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
     * @throws DocumentStoreNotOpenedException if the database located at {@code location} cannot be
     *                                     opened (if it already exists) or created.
     */
    public static DocumentStore getInstance(File location, KeyProvider provider) throws DocumentStoreNotOpenedException {
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
                        eventBus.post(new DocumentStoreCreated(ds.databaseName));
                    }
                    eventBus.post(new DocumentStoreOpened(ds.databaseName));
                }
                return ds;
            }
        }
        catch (IOException e) {
            throw new DocumentStoreNotOpenedException("Database not found: " + location, e);
        } catch (SQLException e) {
            // thrown during schema upgrades, etc
            throw new DocumentStoreNotOpenedException("Database not initialized correctly: " + location, e);
        } catch (IllegalArgumentException e) {
            // thrown by SQLDatabaseFactory if directory is not writable
            throw new DocumentStoreNotOpenedException("Database location not accessible: " + location, e);
        } catch (DocumentStoreNotOpenedException e) {
            throw e;
        } catch (DocumentStoreException e) {
            throw new DocumentStoreNotOpenedException("Datastore not initialized correctly: " + location, e);
        }
    }

    /**
     * <p>
     * Get a reference to the {@link Database} object.
     * </p>
     *
     * <p>
     * Users can perform CRUD (create, read, update, delete) operations on JSON documents and
     * attachments by invoking methods on this object.
     * </p>
     *
     * @return a reference to the {@link Database} object
     */
    public Database database() {
        return database;
    }

    /**
     * WARNING: accessing APIs exposed on the
     * {@link com.cloudant.sync.documentstore.advanced.Database} class returned by this method is
     * not required for typical use cases. Refer to the javadoc in the
     * {@link com.cloudant.sync.documentstore.advanced} package before using this.
     *
     * @return interface to advanced database methods
     */
    public com.cloudant.sync.documentstore.advanced.Database advanced() {
        return database;
    }

    /**
     * <p>
     * Get a reference to the {@link Query} object.
     * </p>
     *
     * <p>
     * Users can perform index and query operations on JSON documents by invoking methods on this
     * object.
     * </p>
     *
     * @return a reference to the {@link Query} object
     */
    public Query query() {
        return query;
    }

    /**
     * <p>
     * Closes the DocumentStore instance.
     * If multiple references are held to the {@link DocumentStore} instance, then this method
     * should only be called when all objects are finished using the instance.
     * </p>
     */
    @SuppressWarnings("unchecked")
    public void close() {
        synchronized (documentStores) {
            DocumentStore ds = documentStores.remove(location);
            if (ds != null) {
                database.close();
                ((QueryImpl)query).close();
            }
            else {
                throw new IllegalStateException("DocumentStore "+location+" already closed");
            }
        }
        eventBus.post(new DocumentStoreClosed(databaseName));
    }

    /**
     * <p>
     * Deletes the DocumentStore instance.
     * </p>
     *
     * @throws DocumentStoreNotDeletedException if the DocumentStore doesn't exist on disk or
     * if there was an error deleting the DocumentStore directory.
     */
    public void delete() throws DocumentStoreNotDeletedException {
        try {
            this.close();
        } catch (Exception e) {
            // caught exception could be something benign like DocumentStore already closed, so just log
            logger.log(Level.WARNING, "Caught exception whilst closing DocumentStore in delete()", e);
        }
        if (!location.exists()) {
            String msg = String.format("DocumentStore %s doesn't exist on disk", location);
            logger.warning(msg);
            throw new DocumentStoreNotDeletedException(msg);
        } else {
            try {
                FileUtils.deleteDirectory(location);
            } catch (IOException ioe) {
                String msg = String.format("DocumentStore %s not deleted", location);
                logger.log(Level.WARNING, msg, ioe);
                throw new DocumentStoreNotDeletedException(msg, ioe);
            }
            eventBus.post(new DocumentStoreDeleted(databaseName));
        }
    }

    /**
     * <p>Returns the EventBus which this DocumentStore posts
     * {@link DocumentStoreModified Database Notification Events} to.</p>
     * @return the DocumentStore's EventBus
     *
     * @see <a target="_blank" href="https://github.com/cloudant/sync-android/blob/master/doc/events.md">
     *     Events documentation</a>
     */
    public static EventBus getEventBus() {
        return eventBus;
    }

    private void closeQuietlyOnException() {
        if (database != null) {
            database.close();
        }
        if (query != null) {
            ((QueryImpl) query).close();
        }
    }

}
