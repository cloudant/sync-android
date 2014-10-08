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

package com.cloudant.sync.datastore;

import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.TestUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudant.sync.notifications.DocumentModified;
import com.google.common.eventbus.Subscribe;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

public class BasicDBCoreObservableTest {

    String database_dir;
    String documentOneFile = "fixture/document_1.json";
    String documentTwoFile = "fixture/document_2.json";

    SQLDatabase database = null;
    BasicDatastore core = null;
    byte[] jsonData = null;
    DocumentBody bodyOne = null;
    DocumentBody bodyTwo = null;

    TestObserver testObserver;

    @Before
    public void setUp() {
        this.database_dir = TestUtils.createTempTestingDir(BasicDBCoreObservableTest.class.getName());
    }

    @After
    public void tearDown() {
        TestUtils.deleteTempTestingDir(this.database_dir);
        core.close();
    }

    @Test
    public void createDocument_bodyOnly_success() throws ConflictException, IOException, SQLException {

        this.core = new BasicDatastore(database_dir, "test");
        this.database = this.core.getSQLDatabase();

        this.jsonData = FileUtils.readFileToByteArray(TestUtils.loadFixture(documentOneFile));
        this.bodyOne = new BasicDocumentBody(jsonData);

        this.jsonData = FileUtils.readFileToByteArray(TestUtils.loadFixture(documentTwoFile));
        this.bodyTwo = new BasicDocumentBody(jsonData);

        this.testObserver = new TestObserver(core);
        this.core.getEventBus().register(testObserver);

        Assert.assertEquals(-1L, testObserver.getSequence());

        MutableDocumentRevision doc1Mut = new MutableDocumentRevision();
        doc1Mut.body = bodyOne;
        BasicDocumentRevision doc1 = core.createDocumentFromRevision(doc1Mut);
        Assert.assertNotNull(doc1);
        Assert.assertEquals(1L, core.getLastSequence());
        Assert.assertEquals(1L, testObserver.getSequence());

        MutableDocumentRevision doc2Mut = new MutableDocumentRevision();
        doc2Mut.body = bodyOne;
        BasicDocumentRevision doc2 = core.createDocumentFromRevision(doc2Mut);
        Assert.assertNotNull(doc2);
        Assert.assertEquals(2L, core.getLastSequence());
        Assert.assertEquals(2L, testObserver.getSequence());

        MutableDocumentRevision doc1_1Mut = doc1.mutableCopy();
        doc1_1Mut.body = bodyTwo;
        BasicDocumentRevision doc1_1 = core.updateDocumentFromRevision(doc1_1Mut);
        Assert.assertNotNull(doc1_1);
        Assert.assertEquals(3L, core.getLastSequence());
        Assert.assertEquals(3L, testObserver.getSequence());
    }

    public static class TestObserver {        
        Datastore core ;
        public TestObserver(Datastore core) {
            this.core = core;
        }

        long sequence = -1l;
        public long getSequence() {
            return sequence;
        }
        
        @Subscribe
        public void onDocumentModified(DocumentModified dm) {
            this.sequence = core.getLastSequence();            
        }

    }

}
