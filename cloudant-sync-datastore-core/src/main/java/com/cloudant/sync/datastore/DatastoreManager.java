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
import com.cloudant.sync.notifications.DatabaseClosed;
import com.cloudant.sync.notifications.DatabaseCreated;
import com.cloudant.sync.notifications.DatabaseDeleted;
import com.cloudant.sync.notifications.DatabaseOpened;
import com.google.common.base.Preconditions;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * <p>Manages a set of {@link Datastore} objects, with their underlying disk
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
 */
public class DatastoreManager {

    private final static String LOG_TAG = "DatastoreManager";
    private final static Logger logger = Logger.getLogger(DatastoreManager.class.getCanonicalName());

    private final String path;

    private final Map<String, Datastore> openedDatastores = Collections.synchronizedMap(new HashMap<String, Datastore>());

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
     *
     * @param directoryPath root directory to manage
     *
     * @see DatastoreManager#DatastoreManager(java.io.File)
     */
    public DatastoreManager(String directoryPath) {
        this(new File(directoryPath));
    }

    /**
     * <p>Constructs a {@code DatastoreManager} to manage a directory.</p>
     *
     * <p>Datastores are created within the {@code directoryPath} directory.
     * In general, this folder should be under the control of, and only used
     * by, a single {@code DatastoreManager} object at any time.</p>
     *
     * @param directoryPath root directory to manage
     *
     * @throws IllegalArgumentException if the {@code directoryPath} is not a
     *          directory or isn't writable.
     */
    public DatastoreManager(File directoryPath) {
        logger.fine("Datastore path: " + directoryPath);
        if(!directoryPath.isDirectory() ) {
            throw new IllegalArgumentException("Input path is not a valid directory");
        } else if(!directoryPath.canWrite()) {
            throw new IllegalArgumentException("Datastore directory is not writable");
        }
        this.path = directoryPath.getAbsolutePath();
    }

    /**
     * Lists all the names of {@link com.cloudant.sync.datastore.Datastore Datastores} managed by this DatastoreManager
     *
     * @return List of {@link com.cloudant.sync.datastore.Datastore Datastores} names.
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
    public Datastore openDatastore(String dbName) throws DatastoreNotCreatedException {
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
     * will return the existing {@link Datastore} object.</p>
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
    public Datastore openDatastore(String dbName, KeyProvider provider) throws DatastoreNotCreatedException {
        Preconditions.checkArgument(dbName.matches(LEGAL_CHARACTERS),
                "A database must be named with all lowercase letters (a-z), digits (0-9),"
                        + " or any of the _$()+-/ characters. The name has to start with a"
                        + " lowercase letter (a-z).");
        if (!openedDatastores.containsKey(dbName)) {
            synchronized (openedDatastores) {
                if (!openedDatastores.containsKey(dbName)) {
                    Datastore ds = createDatastore(dbName, provider);
                    ds.getEventBus().register(this);
                    openedDatastores.put(dbName, ds);
                }
            }
        }
        return openedDatastores.get(dbName);
    }

    /**
     * <p>Deletes a datastore's files from disk.</p>
     *
     * <p>This operation deletes a datastore's files from disk. It is therefore
     * a not undo-able. To confirm, this only deletes local data; data
     * replicated to remote databases is not affected.</p>
     *
     * <p>Any {@link Datastore} objects referring to the deleted files will be
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
        Preconditions.checkNotNull(dbName, "Datastore name must not be null");

        synchronized (openedDatastores) {
            if (openedDatastores.containsKey(dbName)) {
                openedDatastores.get(dbName).close();
                openedDatastores.remove(dbName);
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
    private Datastore createDatastore(String dbName, KeyProvider provider) throws DatastoreNotCreatedException {
        try {
            String dbDirectory = this.getDatastoreDirectory(dbName);
            boolean dbDirectoryExist = new File(dbDirectory).exists();
            logger.info("path: " + this.path);
            logger.info("dbDirectory: " + dbDirectory);
            logger.info("dbDirectoryExist: " + dbDirectoryExist);
            // dbDirectory will created in BasicDatastore constructor
            // if it does not exist

            //Pass database directory, database name, and SQLCipher key provider
            BasicDatastore ds = new BasicDatastore(dbDirectory, dbName, provider);

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
     * @see <a href="https://code.google.com/p/guava-libraries/wiki/EventBusExplained">Google Guava EventBus documentation</a>
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
