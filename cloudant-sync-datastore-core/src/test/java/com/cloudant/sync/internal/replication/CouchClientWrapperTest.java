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

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;

import com.cloudant.common.CouchTestBase;
import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.sync.internal.mazha.ChangesResult;
import com.cloudant.sync.internal.mazha.CouchConfig;
import com.cloudant.sync.internal.mazha.DocumentRevs;
import com.cloudant.sync.internal.mazha.NoResourceException;
import com.cloudant.sync.internal.mazha.Response;
import com.cloudant.sync.documentstore.DocumentBody;
import com.cloudant.sync.documentstore.DocumentBodyFactory;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.internal.datastore.DocumentRevisionBuilder;
import com.cloudant.sync.internal.common.CouchUtils;
import com.cloudant.sync.util.TestUtils;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Category(RequireRunningCouchDB.class)
public class CouchClientWrapperTest extends CouchTestBase {

    public static final String CLOUDANT_TEST_DB_NAME = "couch_client_wrapper_test";

    final static String documentOneFile = "fixture/document_1.json";
    final static String documentTwoFile = "fixture/document_2.json";
    static DocumentBody bodyOne = null;
    static DocumentBody bodyTwo = null;

    public static final String REPLICATOR_IDENTIFIER = "replicator_identifier";
    public static final String SEQUENCE_1 = "10-ksdkdsi";
    public static final String SEQUENCE_2 = "101-adfoasd83";

    public static CouchClientWrapper remoteDb;

    @Before
    public void setup() throws IOException {
        CouchConfig config = super.getCouchConfig(CLOUDANT_TEST_DB_NAME + System
                .currentTimeMillis());
        remoteDb = new CouchClientWrapper(config.getRootUri(), config.getRequestInterceptors(),
                config.getResponseInterceptors());
        bodyOne = DocumentBodyFactory.create(FileUtils.readFileToByteArray(TestUtils.loadFixture
                (documentOneFile)));
        bodyTwo = DocumentBodyFactory.create(FileUtils.readFileToByteArray(TestUtils.loadFixture
                (documentTwoFile)));

        CouchClientWrapperDbUtils.deleteDbQuietly(remoteDb);
        remoteDb.createDatabase();
    }

    @After
    public void tearDown() {
        CouchClientWrapperDbUtils.deleteDbQuietly(remoteDb);
    }

    @Test
    public void exists_dbExists_true() {
        Assert.assertTrue(remoteDb.exists());
    }

    @Test
    public void exists_dbExists_false() {
        CouchConfig config = super.getCouchConfig("db_not_exists");
        CouchClientWrapper couchClientWrapper = new CouchClientWrapper(config.getRootUri(),
                config.getRequestInterceptors(), config.getResponseInterceptors());
        Assert.assertFalse(couchClientWrapper.exists());
    }

    @Test
    public void getCouchClient() {
        Assert.assertNotNull(remoteDb.getCouchClient());
    }

    @Test
    public void getIdentifier() {
        String identifier = remoteDb.getIdentifier();
        Pattern couchDbId = Pattern.compile("http(s)?://.+/couch_client_wrapper_test.+");
        Assert.assertTrue(couchDbId.matcher(identifier).matches());
    }

    @Test
    public void getCheckpoint_getNotExist_nullMustBeReturn() {
        String sequence = remoteDb.getCheckpoint(REPLICATOR_IDENTIFIER);
        Assert.assertThat(sequence, is(nullValue()));
    }

    @Test
    public void getCheckpoint_putAndGet_correctSequenceMustReturn() {
        remoteDb.putCheckpoint(REPLICATOR_IDENTIFIER, SEQUENCE_1);
        String sequence1 = remoteDb.getCheckpoint(REPLICATOR_IDENTIFIER);
        Assert.assertEquals(SEQUENCE_1, sequence1);

        remoteDb.putCheckpoint(REPLICATOR_IDENTIFIER, SEQUENCE_2);
        String sequence2 = remoteDb.getCheckpoint(REPLICATOR_IDENTIFIER);
        Assert.assertEquals(SEQUENCE_2, sequence2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void putCheckpoint_putNull_exception() {
        remoteDb.putCheckpoint(REPLICATOR_IDENTIFIER, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getCheckpoint_nullCheckpointId_exception() {
        remoteDb.getCheckpoint(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getCheckpoint_emptyCheckpointId_exception() {
        remoteDb.getCheckpoint("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void putCheckpoint_nullCheckpointId_exception() {
        remoteDb.putCheckpoint(null, "101");
    }

    @Test(expected = IllegalArgumentException.class)
    public void putCheckpoint_emptyCheckpointId_exception() {
        remoteDb.putCheckpoint("", "101");
    }

    @Test
    public void changes_getChangesAfterTwoUpdates() {

        ChangesResult changes0 = remoteDb.changes(null, 1000);
        Assert.assertEquals(0, changes0.size());

        Object lastSequence0 = changes0.getLastSeq();

        Response[] responses = createTwoDocumentsInRemoteDb(remoteDb);

        ChangesResult changes1 = remoteDb.changes(lastSequence0, 1000);

        Assert.assertEquals(2, changes1.size());
        List<String> changedDocIds = findIdOfChangedDocs(changes1);
        Assert.assertThat(changedDocIds, hasItems(responses[0].getId(), responses[1].getId()));
    }

    public Response[] createTwoDocumentsInRemoteDb(CouchClientWrapper db) {
        Bar bar1 = new Bar();
        Response res1 = db.create(bar1);
        Assert.assertNotNull(res1);

        Bar bar2 = new Bar();
        Response res2 = db.create(bar2);
        bar2 = db.get(Bar.class, res2.getId());

        Response res3 = db.update(bar2.getId(), bar2);
        Assert.assertNotNull(res3);
        return new Response[]{res1, res2};
    }

    private List<String> findIdOfChangedDocs(ChangesResult changes1) {
        List<String> changedDocIds = new ArrayList<String>();
        for (ChangesResult.Row row : changes1.getResults()) {
            changedDocIds.add(row.getId());
        }
        return changedDocIds;
    }

    @Test
    public void getRevisions_giveDocumentId() {
        boolean pullAttachmentsInline = false;

        Response[] responses = createDocAndUpdateTwoTimes(remoteDb);

        ArrayList<String> revIds = new ArrayList<String>();
        ArrayList<String> attsSince = new ArrayList<String>();
        revIds.add(responses[2].getRev());

        List<DocumentRevs> documentRevs = remoteDb.getRevisions(responses[0].getId(), revIds,
                attsSince, pullAttachmentsInline);

        Assert.assertNotNull(documentRevs);
        Assert.assertEquals(1, documentRevs.size());
        Assert.assertEquals(3, documentRevs.get(0).getRevisions().getIds().size());
        Assert.assertEquals(3, documentRevs.get(0).getRevisions().getStart());
        Assert.assertEquals(responses[2].getRev(), documentRevs.get(0).getRev());
        Assert.assertThat(documentRevs.get(0).getRevisions().getIds(), hasItems(findRevisionIds
                (responses)));
    }

    private String[] findRevisionIds(Response[] responses) {
        return new String[]{CouchUtils.getRevisionIdSuffix(responses[0].getRev()),
                CouchUtils.getRevisionIdSuffix(responses[1].getRev()),
                CouchUtils.getRevisionIdSuffix(responses[2].getRev())};
    }

    public Response[] createDocAndUpdateTwoTimes(CouchClientWrapper db) {
        Response res1 = db.create(new Bar());
        Bar bar1 = db.get(Bar.class, res1.getId());

        Response res2 = db.update(bar1.getId(), bar1);
        Assert.assertNotNull(res2);
        Assert.assertEquals(res1.getId(), res2.getId());
        Bar bar2 = db.get(Bar.class, res1.getId());

        Response res3 = db.update(bar2.getId(), bar2);
        Assert.assertEquals(res1.getId(), res3.getId());
        Bar bar3 = db.get(Bar.class, res1.getId());
        Assert.assertNotNull(bar3);
        return new Response[]{res1, res2, res3};
    }

    @Test
    public void getRevisions_deletedDocument() {
        boolean pullAttachmentsInline = false;

        Response[] responses = createDocUpdateTwoTimesThenDelete(remoteDb);

        ArrayList<String> revIds = new ArrayList<String>();
        ArrayList<String> attsSince = new ArrayList<String>();
        revIds.add(responses[3].getRev());

        List<DocumentRevs> documentRevs = remoteDb.getRevisions(responses[0].getId(), revIds,
                attsSince, pullAttachmentsInline);

        Assert.assertEquals(1, documentRevs.size());
        Assert.assertTrue(documentRevs.get(0).getDeleted());
        Assert.assertEquals(4, documentRevs.get(0).getRevisions().getStart());
        Assert.assertEquals(4, documentRevs.get(0).getRevisions().getIds().size());
    }

    public Response[] createDocUpdateTwoTimesThenDelete(CouchClientWrapper remoteDb) {
        Response res1 = remoteDb.create(new Bar());
        Bar bar1 = remoteDb.get(Bar.class, res1.getId());

        Response res2 = remoteDb.update(bar1.getId(), bar1);
        Assert.assertNotNull(res2);
        Assert.assertEquals(res1.getId(), res2.getId());
        Bar bar2 = remoteDb.get(Bar.class, res1.getId());

        Response res3 = remoteDb.update(bar2.getId(), bar2);
        Assert.assertEquals(res1.getId(), res3.getId());
        Bar bar3 = remoteDb.get(Bar.class, res1.getId());
        Assert.assertNotNull(bar3);

        Response res4 = remoteDb.delete(bar3.getId(), bar3.getRevision());
        Assert.assertEquals(res1.getId(), res4.getId());
        try {
            Bar bar4 = remoteDb.get(Bar.class, res1.getId());
            Assert.fail();
        } catch (NoResourceException e) {
        }
        return new Response[]{res1, res2, res3, res4};
    }

    @Test
    public void bulk_twoDocs_docsShouldBeCreated() {

        String objectId1 = "haha";
        String objectId2 = "hehe";

        List<DocumentRevision> objects = createTwoDBObjects(remoteDb, objectId1, objectId2);

        remoteDb.bulkCreateDocs(objects);

        Map<String, Object> obj1 = remoteDb.get(Map.class, objectId1);
        Assert.assertNotNull(obj1);
        Assert.assertEquals(objects.get(0).getRevision(), obj1.get("_rev"));


        Map<String, Object> obj2 = remoteDb.get(Map.class, objectId2);
        Assert.assertNotNull(obj2);
        Assert.assertNotNull(obj2.get("_rev"));
        Assert.assertEquals(objects.get(1).getRevision(), obj2.get("_rev"));
    }

    public List<DocumentRevision> createTwoDBObjects(CouchClientWrapper remoteDb, String
            id1, String id2) {
        List<DocumentRevision> objects = new ArrayList<DocumentRevision>();

        DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
        builder.setDocId(id1);
        builder.setRevId(CouchUtils.getFirstRevisionId());
        builder.setBody(bodyOne);

        DocumentRevision todo1 = builder.build();
        objects.add(todo1);

        DocumentRevisionBuilder builder2 = new DocumentRevisionBuilder();
        builder2.setDocId(id2);
        builder2.setRevId(CouchUtils.getFirstRevisionId());
        builder2.setBody(bodyTwo);

        DocumentRevision tdo2 = builder2.build();
        objects.add(tdo2);

        return objects;
    }

    @Test
    public void accessAndUpdateRemoteDbWithSlashInName() throws Exception {
        //do a little set up for this specific test
        CouchConfig config = super.getCouchConfig("myslash%2Fencoded_db");
        CouchClientWrapper slashDb = new CouchClientWrapper(config.getRootUri(),
                config.getRequestInterceptors(),
                config.getResponseInterceptors());
        slashDb.createDatabase();
        try {
            Bar bar1 = new Bar();
            Response res1 = slashDb.create(bar1);
            Assert.assertNotNull(res1);
            Assert.assertTrue(res1.getOk());
        } finally {
            CouchClientWrapperDbUtils.deleteDbQuietly(slashDb);
        }
    }
}
