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

import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.mazha.NoResourceException;
import com.cloudant.sync.datastore.DocumentBody;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.DocumentRevisionBuilder;
import com.cloudant.sync.datastore.MutableDocumentRevision;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;


@Category(RequireRunningCouchDB.class)
public class BasicPushStrategyTest extends ReplicationTestBase {

    PushConfiguration currentSetting = null;
    BasicPushStrategy replicator;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        currentSetting = createPushReplicationSetting();
    }

    private PushConfiguration createPushReplicationSetting() {
        return new PushConfiguration(PushConfiguration.DEFAULT_CHANGES_LIMIT_PER_BATCH,
                PushConfiguration.DEFAULT_MAX_BATCH_COUNTER_PER_RUN, 1, PushConfiguration.DEFAULT_PUSH_ATTACHMENTS_INLINE);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void push_nothing_lastSequenceShouldStillBeNull() throws Exception {
        this.push();
        assertPushReplicationStatus(0, 1, null);
    }

    @Test
    public void push_noChangesAfterOneDoc_lastSequenceShouldNotBeTouched() throws Exception {
        // Prepare
        oneDocCreatedAndThenPush();
        assertPushReplicationStatus(1, 2, "1");

        // Test
        this.push();
        assertPushReplicationStatus(0, 1, "1");
    }

    private void assertPushReplicationStatus(int documentCounter, int batchCounter, String lastSequence) {
        Assert.assertEquals("DocumentRevisionTree counter", documentCounter, replicator.getDocumentCounter());
        Assert.assertEquals("Batch counter", batchCounter, replicator.getBatchCounter());
        Assert.assertEquals("Last sequence", lastSequence, remoteDb.getCheckpoint(this.replicator.getReplicationId()));
    }

    @Test
    public void push_oneDocOneRev_revisionShouldBePulled() throws Exception {
        oneDocCreatedAndThenPush();
        assertPushReplicationStatus(1, 2, "1");
    }

    @Test
    public void push_oneDocAfterAnother_lastSequenceShouldNotBeTouched() throws Exception {
        // Prepare
        oneDocCreatedAndThenPush();
        assertPushReplicationStatus(1, 2, "1");

        // Test
        oneDocCreatedAndThenPush();
        assertPushReplicationStatus(1, 2, "2");
    }

    @Test
    public void push_oneUpdatedDocAfterAnother_lastSequenceShouldBeUpdatedCorrectly() throws Exception {
        // Prepare
        oneDocCreatedThenUpdatedAndThenPush();
        assertPushReplicationStatus(1, 2, "2");

        // Test
        oneDocCreatedThenUpdatedAndThenPush();
        assertPushReplicationStatus(1, 2, "4");

        // Another push(), and the last sequence should not be touched
        this.push();
        assertPushReplicationStatus(0, 1, "4");
    }

    @Test
    public void push_oneDeletedDocAfterAnother_lastSequenceShouldBeUpdatedCorrectly() throws  Exception {
        // Prepare
        oneDocCreatedThenUpdatedAndThenPush();
        assertPushReplicationStatus(1, 2, "2");

        // Test
        oneDocCreatedThenUpdatedThenDeletedAndThenPush();
        assertPushReplicationStatus(1, 2, "5");
    }

    @Test
    public void push_twoDocs_lastSequenceShouldBeUpdatedCorrectly() throws Exception {
        twoDocCreatedAndThenPush();
        assertPushReplicationStatus(2, 2, "2");
    }

    @Test
    public void test_oneDocCreatedAndThenPush() throws Exception {
        oneDocCreatedAndThenPush();
        assertPushReplicationStatus(1, 2, "1");
    }

    private Bar oneDocCreatedAndThenPush() throws Exception {
        Bar bar1 = BarUtils.createBar(datastore, "Tom", 31);
        this.push();
        Bar bar2 = couchClient.getDocument(bar1.getId(), Bar.class);
        Assert.assertEquals(bar1, bar2);
        return bar1;
    }

    private Bar[] twoDocCreatedAndThenPush() throws Exception {
        Bar bar1 = BarUtils.createBar(datastore, "Tom", 31);
        Bar bar2 = BarUtils.createBar(datastore, "Jerry", 50);
        this.push();
        Bar bar3 = couchClient.getDocument(bar1.getId(), Bar.class);
        Assert.assertEquals(bar1, bar3);

        Bar bar4 = couchClient.getDocument(bar2.getId(), Bar.class);
        Assert.assertEquals(bar2, bar4);
        return new Bar[]{bar1, bar2};
    }

    @Test
    public void push_oneDocCreatedThenUpdatedAndThenPush() throws Exception {
        oneDocCreatedThenUpdatedAndThenPush();
        assertPushReplicationStatus(1, 2, "2");
    }

    private Bar oneDocCreatedThenUpdatedAndThenPush() throws Exception {
        Bar bar1 = BarUtils.createBar(datastore, "Tom", 31);
        Bar bar2 = BarUtils.updateBar(datastore, bar1.getId(), "Jerry", 50);
        this.push();
        Bar bar3 = couchClient.getDocument(bar1.getId(), Bar.class);
        Assert.assertEquals(bar2, bar3);
        return bar1;
    }

    @Test
    public void push_oneDocCreatedThenUpdatedThenDeletedAndThenPush() throws Exception {
        oneDocCreatedThenUpdatedThenDeletedAndThenPush();
        assertPushReplicationStatus(1, 2, "3");
    }

    private void oneDocCreatedThenUpdatedThenDeletedAndThenPush() throws Exception {
        Bar bar = oneDocCreatedThenUpdatedAndThenPush();
        BarUtils.deleteBar(datastore, bar.getId());

        this.push();
        try {
            couchClient.getDocument(bar.getId(), Bar.class);
            Assert.fail();
        } catch (NoResourceException e) {}
    }

    private void push() throws Exception {
        TestStrategyListener listener = new TestStrategyListener();
        PushReplication pushReplication = this.createPushReplication();
        this.replicator = new BasicPushStrategy(pushReplication, currentSetting);
        this.replicator.eventBus.register(listener);
        this.replicator.run();
        Assert.assertTrue(listener.finishCalled);
        Assert.assertFalse(listener.errorCalled);
    }

    @Test
    public void push_changeLimitIsOne_batchCounterShouldBeCorrect() throws Exception {
        currentSetting = new PushConfiguration(1, PushConfiguration.DEFAULT_MAX_BATCH_COUNTER_PER_RUN,
                PushConfiguration.DEFAULT_BULK_INSERT_SIZE, PushConfiguration.DEFAULT_PUSH_ATTACHMENTS_INLINE);
        twoDocsCreatedAndThenPushed();
        assertPushReplicationStatus(2, 3, "2");
    }

    private void twoDocsCreatedAndThenPushed() throws Exception {
        Bar bar1 = BarUtils.createBar(datastore, "Tom", 31);
        Bar bar2 = BarUtils.createBar(datastore, "Jerry", 50);
        this.push();
        Bar bar3 = couchClient.getDocument(bar1.getId(), Bar.class);
        Bar bar4 = couchClient.getDocument(bar2.getId(), Bar.class);
        Assert.assertEquals(bar1, bar3);
        Assert.assertEquals(bar2, bar4);
    }

    @Test
    public void push_twoBranchForSameTree_allBranchesShouldBePushed() throws Exception {
        currentSetting = new PushConfiguration(1, PushConfiguration.DEFAULT_MAX_BATCH_COUNTER_PER_RUN,
                PushConfiguration.DEFAULT_BULK_INSERT_SIZE, PushConfiguration.DEFAULT_PUSH_ATTACHMENTS_INLINE);

        BasicDocumentRevision rev = createDbObject("5-e", createDBBody("Tom"));
        datastore.forceInsert(rev, "1-a", "2-b", "3-c", "4-d", "5-e");

        // 5-x will be the winner
        BasicDocumentRevision rev2 = createDbObject("5-x", createDBBody("Jerry"));
        datastore.forceInsert(rev2, "1-a", "2-b", "3-c", "4-d", "5-x");

        this.push();
        // two tree belongs to one doc
        assertPushReplicationStatus(1, 7, "6");

        Map<String, Object> m = couchClient.getDocument("1");
        Assert.assertEquals("1", m.get("_id"));
        Assert.assertEquals("5-x", m.get("_rev"));
        Assert.assertEquals("Jerry", m.get("name"));
    }

    public DocumentBody createDBBody(String name) {
        Map<String, Object> m = new HashMap<String, Object>();
        m.put("name", name);
        return DocumentBodyFactory.create(m);
    }

    private BasicDocumentRevision createDbObject(String rev, DocumentBody body) {
        DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
        builder.setDocId("1");
        builder.setRevId(rev);
        builder.setDeleted(false);
        builder.setBody(body);
        return builder.build();
    }

    @Test
    public void push_twoTrees_allTreeShouldBePushed() throws Exception {
        currentSetting = new PushConfiguration(1, PushConfiguration.DEFAULT_MAX_BATCH_COUNTER_PER_RUN,
                PushConfiguration.DEFAULT_BULK_INSERT_SIZE, PushConfiguration.DEFAULT_PUSH_ATTACHMENTS_INLINE);

        BasicDocumentRevision rev = createDbObject("4-d", createDBBody("Tom"));
        datastore.forceInsert(rev, "1-a", "2-b", "3-c", "4-d");

        // 4-d will be the winner
        BasicDocumentRevision rev2 = createDbObject("3-z", createDBBody("Jerry"));
        datastore.forceInsert(rev2, "1-x", "2-y", "3-z");

        this.push();

        // two tree belongs to one doc, so only one doc is processed
        assertPushReplicationStatus(1, 8, "7");

        Map<String, Object> m = couchClient.getDocument("1");
        Assert.assertEquals("1", m.get("_id"));
        Assert.assertEquals("4-d", m.get("_rev"));
        Assert.assertEquals("Tom", m.get("name"));
    }

    @Test
    public void push_documentWithIdInChinese_docBePushed() throws Exception {
        currentSetting = new PushConfiguration(1, PushConfiguration.DEFAULT_MAX_BATCH_COUNTER_PER_RUN,
                PushConfiguration.DEFAULT_BULK_INSERT_SIZE, PushConfiguration.DEFAULT_PUSH_ATTACHMENTS_INLINE);

        String id = "\u738b\u4e1c\u5347";
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.body = createDBBody("Tom");
        rev.docId = id;
        BasicDocumentRevision saved = datastore.createDocumentFromRevision(rev);

        this.push();
        assertPushReplicationStatus(1, 2, "1");

        Map<String, Object> m = couchClient.getDocument(id);
        Assert.assertEquals(id, m.get("_id"));
        Assert.assertEquals(saved.getRevision(), m.get("_rev"));
        Assert.assertEquals("Tom", m.get("name"));
    }


    @Test
    public void push_oneDocOneRevAndResetCheckpoint_docShouldNotBePushedAgain() throws Exception {
        oneDocCreatedAndThenPush();
        assertPushReplicationStatus(1, 2, "1");

        CouchClientWrapper wrapper = new CouchClientWrapper(this.couchClient);
        System.out.println("Checkpoint: " + wrapper.getCheckpoint(this.replicator.getReplicationId()));
        wrapper.putCheckpoint(this.replicator.getReplicationId(), "0");
        System.out.println("Checkpoint: " + wrapper.getCheckpoint(this.replicator.getReplicationId()));

        this.push();
        assertPushReplicationStatus(0, 2, "1");
    }

}
