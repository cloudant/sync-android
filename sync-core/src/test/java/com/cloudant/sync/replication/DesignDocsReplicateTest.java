package com.cloudant.sync.replication;

import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.mazha.AnimalDb;
import com.cloudant.mazha.CouchClient;
import com.cloudant.mazha.CouchDbInfo;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Created by tomblench on 11/06/2014.
 */

@Category(RequireRunningCouchDB.class)
public class DesignDocsReplicateTest extends ReplicationTestBase {

    /*
     * Test that we can push and pull design docs correctly
     */
    @Test
    public void designDocsReplicateTest() throws Exception {
        AnimalDb.populate(couchClient);
        // assert all docs including design doc present
        Assert.assertEquals(11, couchClient.getDbInfo(getDbName()).getDocCount());
        // pull back docs including design doc
        pull();
        // trash db
        couchClient.deleteDb(getDbName());
        couchClient.createDb(getDbName());
        // assert DB empty
        Assert.assertEquals(0, couchClient.getDbInfo(getDbName()).getDocCount());
        // and push to it
        push();
        CouchDbInfo ci2 = couchClient.getDbInfo(getDbName());
        // assert all docs including design doc present
        Assert.assertEquals(11, couchClient.getDbInfo(getDbName()).getDocCount());
    }

    private void pull() throws Exception {
        TestStrategyListener listener = new TestStrategyListener();
        BasicPullStrategy pull = new BasicPullStrategy(this.createPullReplication());
        pull.getEventBus().register(listener);

        Thread t = new Thread(pull);
        t.start();
        t.join();
        Assert.assertTrue(listener.finishCalled);
        Assert.assertFalse(listener.errorCalled);
    }

    private void push() throws Exception {
        TestStrategyListener listener = new TestStrategyListener();
        BasicPushStrategy pull = new BasicPushStrategy(this.createPushReplication());
        pull.getEventBus().register(listener);

        Thread t = new Thread(pull);
        t.start();
        t.join();
        Assert.assertTrue(listener.finishCalled);
        Assert.assertFalse(listener.errorCalled);
    }

}
