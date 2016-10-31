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
import com.cloudant.sync.util.Misc;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by tomblench on 27/09/2016.
 */

public class DocumentStore {

    public final Database database;
    public final IndexManager query;
    protected final String databaseName; // only used for events
    private final File location; // needed for close

    /* This map should only be accessed inside synchronized(openDatastores) blocks */
    private static final Map<File, DocumentStore> documentStores = new HashMap<File, DocumentStore>();

    private static EventBus eventBus = new EventBus();

    private static final Logger logger = Logger.getLogger(DocumentStore.class.getCanonicalName());

    private DocumentStore(File location, KeyProvider keyProvider) throws DatastoreException, IOException, SQLException {
        this.location = location;
        this.databaseName = location.getName();
        this.database = new DatabaseImpl(location, keyProvider);
        // this will be used to observe purge() events from database and forward them to indexmanager
        this.database.getEventBus().register(this);
        this.query = new IndexManagerImpl(database);
    }

    public static DocumentStore getInstance(File location) throws DatastoreNotCreatedException {
        return getInstance(location, new NullKeyProvider());
    }

    public static DocumentStore getInstance(File location, KeyProvider provider) throws DatastoreNotCreatedException {
        boolean created = checkPathAndCreateIfNeeded(location);
        try {
            synchronized (documentStores) {
                DocumentStore ds = documentStores.get(location);
                if (ds == null) {
                    ds = new DocumentStore(location, provider);
                    documentStores.put(location, ds);
                    eventBus.post(new DatabaseOpened(ds.databaseName));
                }
                if (created) {
                    eventBus.post(new DatabaseCreated(ds.databaseName));
                }
                return ds;
            }
        }
        catch (IOException e) {
            throw new DatastoreNotCreatedException("Database not found: " + location, e);
        } catch (SQLException e) {
            throw new DatastoreNotCreatedException("Database not initialized correctly: " + location, e);
        } catch (DatastoreException e) {
            throw new DatastoreNotCreatedException("Datastore not initialized correctly: " + location, e);
        }
    }

    private static boolean checkPathAndCreateIfNeeded(File location) {
        logger.fine("Datastore path: " + location);

        boolean created = false;
        if(!location.exists()){
            created = location.mkdir();
        }

        Misc.checkArgument(location.isDirectory(), "Input path is not a valid directory");
        Misc.checkArgument(location.canWrite(), "Datastore directory is not writable");
        return created;
    }

    public void close() {
        synchronized (documentStores) {
            DocumentStore ds = documentStores.get(location);
            if (ds != null) {
                ((DatabaseImpl)database).close();
                ((IndexManagerImpl)query).close();
                documentStores.remove(location);
            }
            else {
                throw new IllegalStateException("DocumentStore "+location+" already closed");
            }
        }
        eventBus.post(new DatabaseClosed(databaseName));
    }

    public void delete() throws IOException {
        synchronized (documentStores) {
            DocumentStore ds = documentStores.remove(location);
            if (ds != null) {
                ((DatabaseImpl)database).close();
                ((IndexManagerImpl)query).close();
            }
            if (!location.exists()) {
                String msg = String.format(
                        "Datastore %s doesn't exist on disk", location
                );
                logger.warning(msg);
                throw new IOException(msg);
            } else {
                FileUtils.deleteDirectory(location);
                eventBus.post(new DatabaseDeleted(databaseName));
            }
        }
    }

    public static EventBus getEventBus() {
        return eventBus;
    }

}
