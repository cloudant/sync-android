/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.replication;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.mazha.AnimalDb;
import com.cloudant.mazha.ChangesResult;
import com.cloudant.mazha.ClientTestUtils;
import com.cloudant.mazha.CouchClient;
import com.cloudant.mazha.CouchDbInfo;
import com.cloudant.mazha.Response;
import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.datastore.DocumentRevisionTree;
import com.cloudant.sync.query.IndexManager;
import com.cloudant.sync.query.QueryResult;
import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Category(RequireRunningCouchDB.class)
public class BasicPullStrategyTest extends ReplicationTestBase {

    PullConfiguration config = null;
    BasicPullStrategy replicator = null;

    private Bar getDocument(String id) throws Exception {
        BasicDocumentRevision rev = this.datastore.getDocument(id);
        Bar bar = new Bar();
        Map<String, Object> m = rev.getBody().asMap();
        bar.setAge((Integer)m.get("age"));
        bar.setName((String)m.get("name"));
        bar.setId(rev.getId());
        bar.setRevision(rev.getRevision());
        return bar;
    }

    @Test
    public void pull_nothing_na() throws Exception {
        this.pull(0);
        Assert.assertEquals(0, replicator.getDocumentCounter());
        Assert.assertEquals(1, replicator.getBatchCounter());
    }

    @Test
    public void pull_oneDocOneRev_revisionShouldBePulled() throws Exception {
        oneDocCreatedAndThenPulled();
        Assert.assertEquals(1, replicator.getDocumentCounter());
        Assert.assertEquals(1, replicator.getBatchCounter());
    }

    private Bar oneDocCreatedAndThenPulled() throws Exception {
        Bar bar1 = BarUtils.createBar(remoteDb, "Tom", 31);
        this.pull(1);
        Bar bar2 = this.getDocument(bar1.getId());
        Assert.assertEquals(bar1, bar2);
        return bar1;
    }

    @Test
    public void pull_oneDocTwoRevs_bothRevisionsShouldBePulled() throws Exception {
        Bar bar1 = BarUtils.createBar(remoteDb, "Tom", 31);
        Bar bar2 = BarUtils.updateBar(remoteDb, bar1.getId(), "Jerry", 41);
        this.pull(1);
        Bar bar3 = this.getDocument(bar1.getId());
        Assert.assertEquals(bar2, bar3);
        Assert.assertThat(bar3.getRevision(), startsWith("2-"));
        Assert.assertEquals(1, replicator.getDocumentCounter());
        Assert.assertEquals(1, replicator.getBatchCounter());
    }

    @Test
    public void pull_twoDocs_bothDocRevisionsShouldBePulled() throws Exception {
        Bar bar1 = BarUtils.createBar(remoteDb, "Tom", 31);
        Bar bar2 = BarUtils.createBar(remoteDb, "Jerry", 41);
        this.pull(2);
        Bar bar3 = this.getDocument(bar1.getId());
        Assert.assertEquals(bar1, bar3);
        Bar bar4 = this.getDocument(bar2.getId());
        Assert.assertEquals(bar2, bar4);

        Assert.assertEquals(2, replicator.getDocumentCounter());
        Assert.assertEquals(1, replicator.getBatchCounter());
    }

    @Test
    public void pull_twoDocsWithBatchSizeOne_bothDocRevisionsShouldBePulled() throws Exception {
        this.config = new PullConfiguration(1, PullConfiguration.DEFAULT_MAX_BATCH_COUNTER_PER_RUN,
                PullConfiguration.DEFAULT_INSERT_BATCH_SIZE, PullConfiguration.DEFAULT_PULL_ATTACHMENTS_INLINE);
        Assert.assertEquals(1, this.config.changeLimitPerBatch);

        Bar bar1 = BarUtils.createBar(remoteDb, "Tom", 31);
        Bar bar2 = BarUtils.createBar(remoteDb, "Jerry", 41);
        this.pull(2);
        Bar bar3 = this.getDocument(bar1.getId());
        Assert.assertEquals(bar1, bar3);
        Bar bar4 = this.getDocument(bar2.getId());
        Assert.assertEquals(bar2, bar4);

        Assert.assertEquals(2, replicator.getDocumentCounter());
        Assert.assertEquals(3, replicator.getBatchCounter());
    }

    @Test
    public void pull_oneDocUpdatedAfterPull_newRevisionShouldBePulled() throws Exception {
        Bar bar1 = oneDocCreatedAndThenPulled();

        Bar bar3 = BarUtils.updateBar(remoteDb, bar1.getId(), "Jerry", 41);
        this.pull(1);
        Bar bar4 = this.getDocument(bar1.getId());
        Assert.assertEquals(bar3, bar4);

        Assert.assertEquals(1, replicator.getDocumentCounter());
        Assert.assertEquals(1, replicator.getBatchCounter());
    }

    @Test
    public void pull_oneDocDeleted_newRevisionShouldBePull() throws Exception {
        Bar bar1 = oneDocCreatedAndThenPulled();
        Response res = BarUtils.deleteBar(remoteDb, bar1.getId());
        this.pull(1);
        BasicDocumentRevision object = datastore.getDocument(res.getId(), res.getRev());
        Assert.assertNotNull(object);
        Assert.assertTrue(object.isDeleted());
    }

    @Test
    public void pull_oneDocThreeLeafs_allLeafsShouldBePullCorrectly() throws Exception {
        Bar bar1 = BarUtils.createBar(remoteDb, "NoName", 31);
        String[] openRevs = BarUtils.createThreeLeafs(remoteDb, bar1.getId());

        this.pull(1);

        DocumentRevisionTree docTree = datastore.getAllRevisionsOfDocument(bar1.getId());
        Assert.assertEquals(3, docTree.leafs().size());
        List<String> leafRevs = findRevisionOfLeafs(docTree);
        Assert.assertThat(leafRevs, hasItems(openRevs));

        Assert.assertEquals(1, replicator.getDocumentCounter());
        Assert.assertEquals(1, replicator.getBatchCounter());
    }

    @Test
    public void pull_localDbError_replicationAbort() throws Exception {
        DatastoreExtended localDb = mock(DatastoreExtended.class);

        BasicPullStrategy replication = new BasicPullStrategy(this.createPullReplication(), null);
        replication.targetDb = new DatastoreWrapper(localDb);
        replication.getEventBus().register(new TestStrategyListener());

        // Expected
        when(localDb.getLocalDocument("_local/" + remoteDb.getIdentifier()))
                .thenThrow(new RuntimeException("Mocked error"));

        replication.run();

        Assert.assertEquals(0, replication.getDocumentCounter());
        Assert.assertEquals(1, replication.getBatchCounter());

    }

    private List<String> findRevisionOfLeafs(DocumentRevisionTree docTree) {
        List<String> leafRevs = new ArrayList<String>();
        for(DocumentRevisionTree.DocumentRevisionNode obj : docTree.leafs()) {
            leafRevs.add(obj.getData().getRevision());
        }
        return leafRevs;
    }

    private void pull(int expectedDocs) throws Exception {
        this.pull(expectedDocs, null);
    }

    private void pull(int expectedDocs, Replication.Filter filter) throws Exception {
        TestStrategyListener listener = new TestStrategyListener();
        PullReplication pullReplication = this.createPullReplication();
        pullReplication.filter = filter;

        this.replicator = new BasicPullStrategy(pullReplication, this.config);
        this.replicator.getEventBus().register(listener);
        this.replicator.run();
        Assert.assertTrue(listener.finishCalled);
        Assert.assertFalse(listener.errorCalled);
        Assert.assertEquals(expectedDocs, listener.documentsReplicated);
    }

    @Test
    public void pull_twoTreeBothHasOneRevision_success() throws Exception {
        CouchClient client = remoteDb.getCouchClient();
        Bar bar = oneDocCreatedAndThenPulled();
        {
            Bar bar1 = this.getDocument(bar.getId());
            bar1.setRevision("1-zzz");
            bar1.setName("Jerry");
            bar1.setAge(100);

            List<Response> responses = client.bulkCreateDocs(Arrays.asList(new Object[]{bar1}));
            Assert.assertEquals(0, responses.size());
        }
        this.pull(1);
        {
            Bar bar2 = this.getDocument(bar.getId());
            Assert.assertEquals("1-zzz", bar2.getRevision());
            Assert.assertEquals("Jerry", bar2.getName());
            Assert.assertTrue(100 == bar2.getAge());
        }
    }

    @Test
    public void pull_documentIdInChinese_success() throws Exception {
        String id = "\u738b\u4e1c\u5347";
        Bar bar = BarUtils.createBar(this.remoteDb, id, "Tom", 21);
        Assert.assertEquals(id, bar.getId());
        this.pull(1);
        {
            Bar bar2 = this.getDocument(bar.getId());
            Assert.assertEquals(bar.getId(), bar2.getId());
            Assert.assertEquals(bar.getRevision(), bar2.getRevision());
            Assert.assertEquals("Tom", bar2.getName());
            Assert.assertTrue(21 == bar2.getAge());
        }
    }

    @Test
    public void pull_documentIdColons_success() throws Exception {
        String id = ":this:has:colons:";
        Bar bar = BarUtils.createBar(this.remoteDb, id, "Tom", 21);
        Assert.assertEquals(id, bar.getId());
        this.pull(1);
        {
            Bar bar2 = this.getDocument(bar.getId());
            Assert.assertEquals(bar.getId(), bar2.getId());
            Assert.assertEquals(bar.getRevision(), bar2.getRevision());
            Assert.assertEquals("Tom", bar2.getName());
            Assert.assertTrue(21 == bar2.getAge());
        }
    }

    @Test
    public void pull_oneDocOneRevResetCheckpoint_revisionShouldNotBePulled() throws Exception {
        oneDocCreatedAndThenPulled();
        Assert.assertEquals(1, replicator.getDocumentCounter());
        Assert.assertEquals(1, replicator.getBatchCounter());

        resetCheckpoint();

        this.pull(0);
        Assert.assertEquals(0, replicator.getDocumentCounter());
        Assert.assertEquals(1, replicator.getBatchCounter());
    }

    private void resetCheckpoint() throws Exception {
        DatastoreWrapper wrapper = new DatastoreWrapper(this.datastore);
        wrapper.putCheckpoint(this.remoteDb.getIdentifier(), "0");
    }

    @Test
    public void pull_filterBirdFromAnimalDb_twoDocShouldBePulled() throws Exception {
        Assert.assertEquals(0, datastore.getDocumentCount());

        AnimalDb.populate(remoteDb.couchClient);
        Replication.Filter filter = new Replication.Filter("animal/bird");
        this.pull(2, filter);

        Assert.assertEquals(2, datastore.getDocumentCount());
        String[] birds = {"snipe", "kookaburra"};
        for(String mammal : birds) {
            Assert.assertTrue(datastore.containsDocument(mammal));
        }
    }

    @Test
    public void pull_filterMammalFromAnimalDbUsingParameterizedFilter_eightDocShouldBePulled()
            throws Exception {
        Assert.assertEquals(0, datastore.getDocumentCount());

        AnimalDb.populate(remoteDb.couchClient);
        Replication.Filter filter = new Replication.Filter("animal/by_class",
                ImmutableMap.of("class", "mammal"));
        this.pull(8, filter);

        Assert.assertEquals(8, datastore.getDocumentCount());
        String[] mammals = {"aardvark", "badger", "elephant", "giraffe", "lemur", "llama", "panda", "zebra"};
        for(String mammal : mammals) {
            Assert.assertTrue(datastore.containsDocument(mammal));
        }
    }

    @Test
    public void pull_filterSmallFromAnimalDbUsingIntegerFilter_eightDocShouldBePulled()
            throws Exception {
        Assert.assertEquals(0, datastore.getDocumentCount());

        AnimalDb.populate(remoteDb.couchClient);
        Replication.Filter filter = new Replication.Filter("animal/small",
                ImmutableMap.of("max_length", "2"));
        this.pull(6, filter);

        Assert.assertEquals(6, datastore.getDocumentCount());
        String[] mammals = {"badger", "kookaburra", "lemur", "llama", "panda", "snipe"};
        for(String mammal : mammals) {
            Assert.assertTrue(mammal + " should be in the datastore", datastore.containsDocument(mammal));
        }
    }

    @Test
    public void pull_filterSmallFromAnimalDbUsingNullFilter_eightDocShouldBePulled()
            throws Exception {
        Assert.assertEquals(0, datastore.getDocumentCount());

        AnimalDb.populate(remoteDb.couchClient);
        Replication.Filter filter = new Replication.Filter("animal/by_chinese_name",
                ImmutableMap.of("chinese_name", "\u718a\u732b"));
        this.pull(1, filter);

        Assert.assertEquals(1, datastore.getDocumentCount());
        String[] mammals = {"panda"};
        for(String mammal : mammals) {
            Assert.assertTrue(mammal + " should be in the datastore", datastore.containsDocument(mammal));
        }
    }

    @Test
    public void pull_emptyFilterKey_noDocReturned()
            throws Exception {
        Assert.assertEquals(0, datastore.getDocumentCount());

        AnimalDb.populate(remoteDb.couchClient);
        Replication.Filter filter = new Replication.Filter("animal/by_chinese_name",
                ImmutableMap.of("", "\u718a\u732b"));
        this.pull(0, filter);

        Assert.assertEquals(0, datastore.getDocumentCount());
    }

    @Test
    public void pull_indexesUpdated()
            throws Exception {
        Assert.assertEquals(0, datastore.getDocumentCount());

        IndexManager im = new IndexManager(datastore);
        try {
            im.ensureIndexed(Arrays.<Object>asList("diet"), "diet");

            AnimalDb.populateWithoutFilter(remoteDb.couchClient);
            this.pull(10);

            Assert.assertEquals(10, datastore.getDocumentCount());

            Map<String, Object> query = new HashMap<String, Object>();
            query.put("diet", "herbivore");
            QueryResult qr = im.find(query);
            Assert.assertEquals(4, qr.size());
        } finally {
            im.close();
        }

    }

    @Test
    public void pull_changesNewerThanOpenRevs() throws Exception {
        // test for regressions against the following scenario:
        // - changes feed tells us that 1-rev is a new rev we don't have
        // - on remote db, 2-rev gets added and becomes the new leaf
        // - compact() is called and 1-rev is compacted away
        // - we try to pull 1-rev but it is no longer there
        // the solution to this is to re-fetch the changes feed at the same checkpoint and continue
        // replication - in the hope that the changes feed now reflects the revs available

        // upload some docs
        Bar bar1 = BarUtils.createBar(remoteDb, "Tom", 31);

        // capture 'real' changes feed - after first rev
        final ChangesResult changesResult1 = remoteDb.changes(null, 1000);

        // upload some more docs
        Bar bar2 = BarUtils.updateBar(remoteDb, bar1.getId(), "Jerry", 41);

        // capture 'real' changes feed - after second rev
        final ChangesResult changesResult2 = remoteDb.changes(null, 1000);

        // compact away the first rev
        URI postURI = new URI(couchClient.getRootUri().toString() + "/_compact");
        Assert.assertEquals(202, ClientTestUtils.executeHttpPostRequest(postURI, ""));

        CouchDbInfo info = couchClient.getDbInfo();

        while(info.isCompactRunning()) {
            Thread.sleep(1000);
            info = couchClient.getDbInfo();
        };

        // set up replication...
        PullReplication pullReplication = this.createPullReplication();
        BasicPullStrategy customReplicator = new BasicPullStrategy(pullReplication, null, this.config);

        // inject our mocked changesresult via a 'spy'
        // ChangesResultAnswer returns the 'wrong' answer first and the 'right' after every
        // invocation after that
        customReplicator.sourceDb = spy(customReplicator.sourceDb);
        doAnswer(new ChangesResultAnswer(changesResult1, changesResult2))
                .when(customReplicator.sourceDb).
                changes((Replication.Filter)
                        anyObject(), anyObject(), anyInt());
        // do the actual replication
        customReplicator.run();

        // assert latest doc retrieved
        Map m = datastore.getDocument(bar1.getId()).asMap();
        Assert.assertEquals(41, m.get("age"));
        Assert.assertEquals("Jerry", m.get("name"));
    }

    private class ChangesResultAnswer implements Answer {
        private ChangesResult changesResult1;
        private ChangesResult changesResult2;
        private int invocations;

        public ChangesResultAnswer(ChangesResult changesResult1,
                                   ChangesResult changesResult2) {
            this.changesResult1 = changesResult1;
            this.changesResult2 = changesResult2;
            this.invocations = 0;
        }

        public Object answer(InvocationOnMock invocation) {
            if (invocations++ == 0) {
                return changesResult1;
            } else {
                return changesResult2;
            }
        }
    }
}
