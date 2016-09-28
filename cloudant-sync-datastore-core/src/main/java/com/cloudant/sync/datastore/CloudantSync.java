package com.cloudant.sync.datastore;

import com.cloudant.sync.datastore.encryption.KeyProvider;
import com.cloudant.sync.query.IndexManager;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by tomblench on 27/09/2016.
 */

public class CloudantSync {

    public final Database database;
    public final IndexManager query;

    protected CloudantSync(String dir, String name, KeyProvider keyProvider) throws DatastoreException, IOException, SQLException {
        this.database = new DatabaseImpl(dir, name, keyProvider);
        this.query = new IndexManagerImpl(database);
    }

}
