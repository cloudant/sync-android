/**
 * Original iOS version by  Jens Alfke, ported to Android by Marty Schoch
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 *
 * Modifications for this distribution by Cloudant, Inc., Copyright (c) 2013 Cloudant, Inc.
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
import com.cloudant.sync.event.Subscribe;
import com.cloudant.sync.notifications.DatabaseClosed;
import com.cloudant.sync.notifications.DatabaseCreated;
import com.cloudant.sync.notifications.DatabaseDeleted;
import com.cloudant.sync.notifications.DatabaseOpened;
import com.cloudant.sync.util.Misc;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

/**
 * <p>Manages a set of {@link Database} objects, with their underlying disk
 * storage residing in a given directory.</p>
 *
 * <p>In general, a directory used for storing datastores -- that is, managed
 * by this manager -- shouldn't be used for storing other data. The manager
 * object assumes all data within is managed by itself, so adding other files
 * to the directory may cause them to be deleted.</p>
 *
 * <p>A datastore's on-disk representation is a single file containing
 * all its data. In future, there may be other files and folders per
 * datastore.</p>
 *
 * @api_public
 */
public class DatastoreManager {

    // Static map to manage DatastoreManager instances (one per directory path)
    private static final ConcurrentMap<File, DatastoreManager> datastoreManagers = new
            ConcurrentHashMap<File, DatastoreManager>();

    private final static String LOG_TAG = "DatastoreManager";
    private final static Logger logger = Logger.getLogger(DatastoreManager.class.getCanonicalName());

    private final String path;

    /* This map should only be accessed inside synchronized(openDatastores) blocks */
    private final Map<String, CloudantSync> openedDatastores = new HashMap<String, CloudantSync>();

    /**
     * The regex used to validate a datastore name, {@value}.
     */
    protected static final String LEGAL_CHARACTERS = "^[a-zA-Z]+[a-zA-Z0-9_\\Q-$()/\\E]*";

    private final EventBus eventBus = new EventBus();

    /**
     * <p>Constructs a {@code DatastoreManager} to manage a directory.</p>
     *
     * <p>Datastores are created within the {@code directoryPath} directory.
     * In general, this folder should be under the control of, and only used
     * by, a single {@code DatastoreManager} object at any time.</p>
     *<P>
     * Internal use only, external callers should use DatastoreManager.getInstance().
     *</P>
     * @param directoryPath root directory to manage

     * @throws IllegalArgumentException if the {@code directoryPath} is not a
     *          directory or isn't writable.
     *
     */
    private DatastoreManager(File directoryPath) {
        logger.fine("Datastore path: " + directoryPath);

        if(!directoryPath.exists()){
            directoryPath.mkdir();
        }

        if(!directoryPath.isDirectory() ) {
            throw new IllegalArgumentException("Input path is not a valid directory");
        } else if(!directoryPath.canWrite()) {
            throw new IllegalArgumentException("Datastore directory is not writable");
        }
        this.path = directoryPath.getAbsolutePath();
    }

    /**
     * <P>Gets a {@code DatastoreManager} to manage the specified directory of datastores.</P>
     *
     * @param directoryPath root directory to manage
     * @see DatastoreManager#getInstance(File)
     * @return a new or existing DatastoreManager instance for the specified directory.
     */
    public static DatastoreManager getInstance(String directoryPath) {
        return DatastoreManager.getInstance(new File(directoryPath));
    }

    /**
     * <P>Gets a {@code DatastoreManager} to manage the specified directory of datastores.</P>
     * <P>This folder should be under the control of, and only used by, a single
     * {@code DatastoreManager} instance at any time.
     * </P>
     * <P>accessing a DatastoreManager via this method guarantees that only a single
     * DatastoreManager instance exists for the specified path within the static scope of this
     * DatastoreManager class.
     * </P>
     * <P>
     * This method is thread safe and it is acceptable to repeatedly call this method to re-obtain
     * the same DatastoreManager instance.
     * </P>
     *
     * @param directoryPath root directory to manage
     * @return a new or existing DatastoreManager instance for the specified directory.
     * @throws IllegalArgumentException if the {@code directoryPath} is not a
     *                                  directory or isn't writable.
     */
    public static DatastoreManager getInstance(File directoryPath) {
        // Uses the deprecated public constructor until we can change it to a private constructor.
        DatastoreManager manager = new DatastoreManager(directoryPath);
        DatastoreManager existingManager = datastoreManagers.putIfAbsent(directoryPath, manager);
        return (existingManager == null) ? manager : existingManager;
    }

    /**
     * Lists all the names of {@link Database Datastores} managed by this DatastoreManager
     *
     * @return List of {@link Database Datastores} names.
     */
    public List<String> listAllDatastores() {
        List<String> datastores = new ArrayList<String>();
        File dsManagerDir = new File(this.path);
        for(File file:dsManagerDir.listFiles()){
            boolean isStore = file.isDirectory() && new File(file, "db.sync").isFile();
            if (isStore) {
                //replace . with a slash, on disk / are replaced with dots
                datastores.add(file.getName().replace(".", "/"));
            }
        }

        return datastores;
    }

    /**
     * <p>Returns the path to the directory this object manages.</p>
     * @return the absolute path to the directory this object manages.
     */
    public String getPath() {
        return path;
    }

    /**
     * <p>Opens a datastore.</p>
     *
     * <p>Equivalent to calling {@link #openDatastore(String, KeyProvider)} with
     * a {@code NullKeyProvider}.</p>
     *
     * @param dbName name of datastore to open
     * @return {@code Datastore} with the given name
     *
     * @throws com.cloudant.sync.datastore.DatastoreNotCreatedException Thrown when
     * the datastore could not be created or opened.
     *
     * @see DatastoreManager#getEventBus() 
     */
    public CloudantSync openDatastore(String dbName) throws DatastoreNotCreatedException {
        return this.openDatastore(dbName, new NullKeyProvider());
    }

    /**
     * <p>Opens a datastore.</p>
     *
     * <p>Opens an existing datastore on disk, or creates a new one with the given name.</p>
     *
     * <p>If encryption is enabled for this platform, passing a KeyProvider containing a non-null
     * key will open or create an encrypted database. If opening a database, the key used to
     * create the database must be used. If encryption is not enabled for this platform, returning
     * a non-null key will result in an exception.</p>
     *
     * <p>If there is no existing datastore with the given name, and one is successfully
     * created, a {@link DatabaseOpened DatabaseOpened} event is posted on the event bus.</p>
     *
     * <p>Datastores are uniqued: calling this method with the name of an already open datastore
     * will return the existing {@link Database} object.</p>
     *
     * @param dbName name of datastore to open
     * @param provider  KeyProvider object; use a NullKeyProvider if database shouldn't be encrypted.
     *
     * @throws DatastoreNotCreatedException if the database cannot be opened
     *
     * @return {@code Datastore} with the given name
     *
     * @see DatastoreManager#getEventBus()
     */
    public CloudantSync openDatastore(String dbName, KeyProvider provider) throws
            DatastoreNotCreatedException {
        Misc.checkArgument(dbName.matches(LEGAL_CHARACTERS),
                "A database must be named with all lowercase letters (a-z), digits (0-9),"
                        + " or any of the _$()+-/ characters. The name has to start with a"
                        + " lowercase letter (a-z).");
        synchronized (openedDatastores) {
            CloudantSync ds = openedDatastores.get(dbName);
            if (ds == null) {
                ds = createDatastore(dbName, provider);
                ds.database.getEventBus().register(this);
                openedDatastores.put(dbName, ds);
            }
            return ds;
        }
    }

    /**
     * <p>Deletes a datastore's files from disk.</p>
     *
     * <p>This operation deletes a datastore's files from disk. It is therefore
     * a not undo-able. To confirm, this only deletes local data; data
     * replicated to remote databases is not affected.</p>
     *
     * <p>Any {@link Database} objects referring to the deleted files will be
     * in an unknown state. Therefore, they should be disposed of prior to
     * deleting the data. Currently, no checks for open datastores are carried
     * out before attempting the delete.</p>
     *
     * <p>If the datastore was successfully deleted, a 
     * {@link com.cloudant.sync.notifications.DatabaseDeleted DatabaseDeleted} 
     * event is posted on the event bus.</p>
     *
     * @param dbName Name of database to create
     *
     * @throws IOException if the datastore doesn't exist on disk or there is
     *      a problem deleting the files.
     *
     * @see DatastoreManager#getEventBus() 
     */
    public void deleteDatastore(String dbName) throws IOException {
        Misc.checkNotNull(dbName, "Datastore name");

        synchronized (openedDatastores) {
            CloudantSync ds = openedDatastores.remove(dbName);
            if (ds != null) {
                ds.database.close();
                ds.query.close();
            }
            String dbDirectory = getDatastoreDirectory(dbName);
            File dir = new File(dbDirectory);
            if (!dir.exists()) {
                String msg = String.format(
                        "Datastore %s doesn't exist on disk", dbName
                        );
                throw new IOException(msg);
            } else {
                FileUtils.deleteDirectory(dir);
                eventBus.post(new DatabaseDeleted(dbName));
            }
        }
    }

    /**
     * <p>Creates a datastore object for a given name.</p>
     *
     * <p>This method will either open an existing database on disk or create a new one.</p>
     *
     * @param dbName Name of database to create
     * @param provider KeyProvider object; use a NullKeyProvider if database shouldn't be encrypted.
     * @return initialise datastore object
     * @throws DatastoreNotCreatedException if the database cannot be opened
     */
    private CloudantSync createDatastore(String dbName, KeyProvider provider) throws DatastoreNotCreatedException {
        try {
            String dbDirectory = this.getDatastoreDirectory(dbName);
            boolean dbDirectoryExist = new File(dbDirectory).exists();
            logger.info("path: " + this.path);
            logger.info("dbDirectory: " + dbDirectory);
            logger.info("dbDirectoryExist: " + dbDirectoryExist);
            // dbDirectory will created in BasicDatastore constructor
            // if it does not exist

            //Pass database directory, database name, and SQLCipher key provider
            CloudantSync ds = new CloudantSync(dbDirectory, dbName, provider);

            if(!dbDirectoryExist) {
                this.eventBus.post(new DatabaseCreated(dbName));
            }
            eventBus.post(new DatabaseOpened(dbName));
            return ds;
        } catch (IOException e) {
            throw new DatastoreNotCreatedException("Database not found: " + dbName, e);
        } catch (SQLException e) {
            throw new DatastoreNotCreatedException("Database not initialized correctly: " + dbName, e);
        } catch (DatastoreException e) {
            throw new DatastoreNotCreatedException("Datastore not initialized correctly: " + dbName, e);
        }
    }

    private String getDatastoreDirectory(String dbName) {
        return FilenameUtils.concat(this.path, dbName.replace("/","."));
    }

    /**
     * <p>Returns the EventBus which this DatastoreManager posts
     * {@link com.cloudant.sync.notifications.DatabaseModified Database Notification Events} to.</p>
     * @return the DatastoreManager's EventBus
     *
     * @see <a href="https://github.com/cloudant/sync-android/blob/master/doc/events.md">
     *     Events documentation</a>
     */
    public EventBus getEventBus() {
        return eventBus;
    }

    @Subscribe
    public void onDatabaseClosed(DatabaseClosed databaseClosed) {
        synchronized (openedDatastores) {
            this.openedDatastores.remove(databaseClosed.dbName);
        }
        this.eventBus.post(databaseClosed);
    }
}
