package com.cloudant.sync.query;

import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.datastore.DatastoreImpl;
import com.cloudant.sync.util.TestUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by tomblench on 26/09/2016.
 */

public class IndexManagerConstructorTest {

    // force IndexManager constructor to fail by making its directory non-writable
    @Test
    public void constructorThrowsExceptionCorrectly() {

        String path = null;
        try {
            path = TestUtils.createTempTestingDir(IndexManagerConstructorTest.class.getName());
            Datastore ds = new DatastoreImpl(path, "datastore");
            ds.getAllDocumentIds(); //force SQLite to run schema upgrades pending on queue
            new File(path).setWritable(false);
            IndexManager im = new IndexManager(ds);
        } catch (IOException ioe) {
            // got expected exception
            return;
        } catch (SQLException sqe) {
            Assert.fail("Constructor should have thrown an IOException, but threw "+sqe);
        } catch (DatastoreException de) {
            Assert.fail("Constructor should have thrown an IOException, but threw "+de);
        } finally {
            if (path != null) {
                new File(path).setWritable(true);
                TestUtils.deleteTempTestingDir(path);
            }
        }
        Assert.fail("Constructor should have thrown an IOException");

    }

}
