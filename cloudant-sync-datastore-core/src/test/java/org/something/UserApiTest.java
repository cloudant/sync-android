package org.something;

import com.cloudant.sync.datastore.CloudantSync;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.DatastoreNotCreatedException;

import org.junit.Test;

/**
 * Created by tomblench on 27/09/2016.
 */

public class UserApiTest {

    @Test
    public void test() throws DatastoreNotCreatedException {
        CloudantSync d = DatastoreManager.getInstance("/tmp").openDatastore("blah");
    }

}
