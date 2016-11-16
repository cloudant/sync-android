/*
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.sync.internal.replication;

import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.sync.internal.documentstore.InternalDocumentRevision;
import com.cloudant.sync.internal.mazha.NoResourceException;
import com.cloudant.sync.documentstore.DocumentBody;
import com.cloudant.sync.documentstore.DocumentBodyFactory;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.internal.documentstore.DocumentRevisionBuilder;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;


@Category(RequireRunningCouchDB.class)
public class PushStrategyTest extends ReplicationTestBase {

    // some helpers...

    // we use this utility method rather than ReplicationTestBase.push() because some
    // methods want to make assertions on the BasicPushStrategy after running the replication
    private void push(PushStrategy replicator, int expectedDocs) throws Exception {
        TestStrategyListener listener = new TestStrategyListener();
        replicator.eventBus.register(listener);
        replicator.run();
        listener.assertReplicationCompletedOrThrow();
        Assert.assertEquals(expectedDocs, listener.documentsReplicated);
    }

    private void assertPushReplicationStatus(PushStrategy replicator, int documentCounter,
                                             int batchCounter, String lastSequence) throws
            Exception {
        Assert.assertEquals("DocumentRevisionTree counter",
                documentCounter,
                replicator.getDocumentCounter());
        Assert.assertEquals("Batch counter",
                batchCounter,
                replicator.getBatchCounter());
        Assert.assertEquals("Last sequence",
                lastSequence,
                remoteDb.getCheckpoint(replicator.getReplicationId()));
    }

    public DocumentBody createDBBody(String name) {
        Map<String, Object> m = new HashMap<String, Object>();
        m.put("name", name);
        return DocumentBodyFactory.create(m);
    }

    private InternalDocumentRevision createDbObject(String rev, DocumentBody body) {
        DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
        builder.setDocId("1");
        builder.setRevId(rev);
        builder.setDeleted(false);
        builder.setBody(body);
        return builder.build();
    }

    // tests...

    @Test
    public void push_nothing_lastSequenceShouldStillBeNull() throws Exception {
        PushStrategy replicator = super.getPushStrategy();
        this.push(replicator, 0);
        assertPushReplicationStatus(replicator, 0, 1, null);
    }

    @Test
    public void push_noChangesAfterOneDoc_lastSequenceShouldNotBeTouched() throws Exception {
        PushStrategy replicator = super.getPushStrategy();

        // Prepare
        oneDocCreatedAndThenPush(replicator);
        assertPushReplicationStatus(replicator, 1, 2, "1");

        // Test
        this.push(replicator, 0);
        assertPushReplicationStatus(replicator, 0, 1, "1");
    }


    @Test
    public void push_oneDocOneRev_revisionShouldBePulled() throws Exception {
        PushStrategy replicator = super.getPushStrategy();

        oneDocCreatedAndThenPush(replicator);
        assertPushReplicationStatus(replicator, 1, 2, "1");
    }

    @Test
    public void push_oneDocAfterAnother_lastSequenceShouldNotBeTouched() throws Exception {
        PushStrategy replicator = super.getPushStrategy();

        // Prepare
        oneDocCreatedAndThenPush(replicator);
        assertPushReplicationStatus(replicator, 1, 2, "1");

        // Test
        oneDocCreatedAndThenPush(replicator);
        assertPushReplicationStatus(replicator, 1, 2, "2");
    }

    @Test
    public void push_oneUpdatedDocAfterAnother_lastSequenceShouldBeUpdatedCorrectly() throws
            Exception {
        PushStrategy replicator = super.getPushStrategy();

        // Prepare
        oneDocCreatedThenUpdatedAndThenPush(replicator);
        assertPushReplicationStatus(replicator, 1, 2, "2");

        // Test
        oneDocCreatedThenUpdatedAndThenPush(replicator);
        assertPushReplicationStatus(replicator, 1, 2, "4");

        // Another push(), and the last sequence should not be touched
        this.push(replicator, 0);
        assertPushReplicationStatus(replicator, 0, 1, "4");
    }

    @Test
    public void push_oneDeletedDocAfterAnother_lastSequenceShouldBeUpdatedCorrectly() throws
            Exception {
        PushStrategy replicator = super.getPushStrategy();

        // Prepare
        oneDocCreatedThenUpdatedAndThenPush(replicator);
        assertPushReplicationStatus(replicator, 1, 2, "2");

        // Test
        oneDocCreatedThenUpdatedThenDeletedAndThenPush(replicator);
        assertPushReplicationStatus(replicator, 1, 2, "5");
    }

    @Test
    public void push_twoDocs_lastSequenceShouldBeUpdatedCorrectly() throws Exception {
        PushStrategy replicator = super.getPushStrategy();

        twoDocCreatedAndThenPush(replicator);
        assertPushReplicationStatus(replicator, 2, 2, "2");
    }

    @Test
    public void test_oneDocCreatedAndThenPush() throws Exception {
        PushStrategy replicator = super.getPushStrategy();

        oneDocCreatedAndThenPush(replicator);
        assertPushReplicationStatus(replicator, 1, 2, "1");
    }

    private Bar oneDocCreatedAndThenPush(PushStrategy replicator) throws Exception {
        Bar bar1 = BarUtils.createBar(datastore, "Tom", 31);
        this.push(replicator, 1);
        Bar bar2 = couchClient.getDocument(bar1.getId(), Bar.class);
        Assert.assertEquals(bar1, bar2);
        return bar1;
    }

    private Bar[] twoDocCreatedAndThenPush(PushStrategy replicator) throws Exception {
        Bar bar1 = BarUtils.createBar(datastore, "Tom", 31);
        Bar bar2 = BarUtils.createBar(datastore, "Jerry", 50);
        this.push(replicator, 2);
        Bar bar3 = couchClient.getDocument(bar1.getId(), Bar.class);
        Assert.assertEquals(bar1, bar3);

        Bar bar4 = couchClient.getDocument(bar2.getId(), Bar.class);
        Assert.assertEquals(bar2, bar4);
        return new Bar[]{bar1, bar2};
    }

    @Test
    public void push_oneDocCreatedThenUpdatedAndThenPush() throws Exception {
        PushStrategy replicator = super.getPushStrategy();

        oneDocCreatedThenUpdatedAndThenPush(replicator);
        assertPushReplicationStatus(replicator, 1, 2, "2");
    }

    private Bar oneDocCreatedThenUpdatedAndThenPush(PushStrategy replicator) throws Exception {
        Bar bar1 = BarUtils.createBar(datastore, "Tom", 31);
        Bar bar2 = BarUtils.updateBar(datastore, bar1.getId(), "Jerry", 50);
        this.push(replicator, 1);
        Bar bar3 = couchClient.getDocument(bar1.getId(), Bar.class);
        Assert.assertEquals(bar2, bar3);
        return bar1;
    }

    @Test
    public void push_oneDocCreatedThenUpdatedThenDeletedAndThenPush() throws Exception {
        PushStrategy replicator = super.getPushStrategy();

        oneDocCreatedThenUpdatedThenDeletedAndThenPush(replicator);
        assertPushReplicationStatus(replicator, 1, 2, "3");
    }

    private void oneDocCreatedThenUpdatedThenDeletedAndThenPush(PushStrategy replicator)
            throws Exception {
        Bar bar = oneDocCreatedThenUpdatedAndThenPush(replicator);
        BarUtils.deleteBar(datastore, bar.getId());

        this.push(replicator, 1);
        try {
            couchClient.getDocument(bar.getId(), Bar.class);
            Assert.fail();
        } catch (NoResourceException e) {
        }
    }

    @Test
    public void push_oneDocOneRevAndResetCheckpoint_docShouldNotBePushedAgain() throws Exception {
        PushStrategy replicator = super.getPushStrategy();

        oneDocCreatedAndThenPush(replicator);
        assertPushReplicationStatus(replicator, 1, 2, "1");

        CouchClientWrapper wrapper = new CouchClientWrapper(this.couchClient);
        System.out.println("Checkpoint: " + wrapper.getCheckpoint(replicator.getReplicationId()));
        wrapper.putCheckpoint(replicator.getReplicationId(), "0");
        System.out.println("Checkpoint: " + wrapper.getCheckpoint(replicator.getReplicationId()));

        this.push(replicator, 0);
        assertPushReplicationStatus(replicator, 0, 2, "1");
    }

    // tests below build a custom config of 1 change per batch

    @Test
    public void push_changeLimitIsOne_batchCounterShouldBeCorrect() throws Exception {
        PushStrategy replicator = (PushStrategy) ((ReplicatorImpl) super
                .getPushBuilder().changeLimitPerBatch(1).build()).strategy;

        twoDocsCreatedAndThenPushed(replicator);
        assertPushReplicationStatus(replicator, 2, 3, "2");
    }

    private void twoDocsCreatedAndThenPushed(PushStrategy replicator) throws Exception {
        Bar bar1 = BarUtils.createBar(datastore, "Tom", 31);
        Bar bar2 = BarUtils.createBar(datastore, "Jerry", 50);
        this.push(replicator, 2);
        Bar bar3 = couchClient.getDocument(bar1.getId(), Bar.class);
        Bar bar4 = couchClient.getDocument(bar2.getId(), Bar.class);
        Assert.assertEquals(bar1, bar3);
        Assert.assertEquals(bar2, bar4);
    }

    @Test
    public void push_twoBranchForSameTree_allBranchesShouldBePushed() throws Exception {
        PushStrategy replicator = (PushStrategy) ((ReplicatorImpl) super
                .getPushBuilder().changeLimitPerBatch(1).build()).strategy;

        InternalDocumentRevision rev = createDbObject("5-e", createDBBody("Tom"));
        datastore.forceInsert(rev, "1-a", "2-b", "3-c", "4-d", "5-e");

        // 5-x will be the winner
        InternalDocumentRevision rev2 = createDbObject("5-x", createDBBody("Jerry"));
        datastore.forceInsert(rev2, "1-a", "2-b", "3-c", "4-d", "5-x");

        this.push(replicator, 1);
        // two tree belongs to one doc
        assertPushReplicationStatus(replicator, 1, 7, "6");

        Map<String, Object> m = couchClient.getDocument("1");
        Assert.assertEquals("1", m.get("_id"));
        Assert.assertEquals("5-x", m.get("_rev"));
        Assert.assertEquals("Jerry", m.get("name"));
    }


    @Test
    public void push_twoTrees_allTreeShouldBePushed() throws Exception {
        PushStrategy replicator = (PushStrategy) ((ReplicatorImpl) super
                .getPushBuilder().changeLimitPerBatch(1).build()).strategy;

        InternalDocumentRevision rev = createDbObject("4-d", createDBBody("Tom"));
        datastore.forceInsert(rev, "1-a", "2-b", "3-c", "4-d");

        // 4-d will be the winner
        InternalDocumentRevision rev2 = createDbObject("3-z", createDBBody("Jerry"));
        datastore.forceInsert(rev2, "1-x", "2-y", "3-z");

        this.push(replicator, 1);

        // two tree belongs to one doc, so only one doc is processed
        assertPushReplicationStatus(replicator, 1, 8, "7");

        Map<String, Object> m = couchClient.getDocument("1");
        Assert.assertEquals("1", m.get("_id"));
        Assert.assertEquals("4-d", m.get("_rev"));
        Assert.assertEquals("Tom", m.get("name"));
    }

    @Test
    public void push_documentWithIdInChinese_docBePushed() throws Exception {
        PushStrategy replicator = super.getPushStrategy();

        String id = "\u738b\u4e1c\u5347";
        DocumentRevision rev = new DocumentRevision(id);
        rev.setBody(createDBBody("Tom"));
        DocumentRevision saved = datastore.createDocumentFromRevision(rev);

        this.push(replicator, 1);
        assertPushReplicationStatus(replicator, 1, 2, "1");

        Map<String, Object> m = couchClient.getDocument(id);
        Assert.assertEquals(id, m.get("_id"));
        Assert.assertEquals(saved.getRevision(), m.get("_rev"));
        Assert.assertEquals("Tom", m.get("name"));
    }


}
