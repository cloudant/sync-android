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

import com.cloudant.sync.datastore.encryption.NullKeyProvider;
import com.cloudant.sync.event.Subscribe;
import com.cloudant.sync.notifications.DocumentModified;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.TestUtils;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class BasicDBCoreObservableTest {

    String database_dir;
    String documentOneFile = "fixture/document_1.json";
    String documentTwoFile = "fixture/document_2.json";

    SQLDatabase database = null;
    DatabaseImpl core = null;
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
        core.close();
        TestUtils.deleteTempTestingDir(this.database_dir);
    }

    @Test
    public void createDocument_bodyOnly_success() throws Exception {

        this.core = new DatabaseImpl(new File(database_dir, "test"), new NullKeyProvider());

        this.jsonData = FileUtils.readFileToByteArray(TestUtils.loadFixture(documentOneFile));
        this.bodyOne = new DocumentBodyImpl(jsonData);

        this.jsonData = FileUtils.readFileToByteArray(TestUtils.loadFixture(documentTwoFile));
        this.bodyTwo = new DocumentBodyImpl(jsonData);

        this.testObserver = new TestObserver(core);
        this.core.getEventBus().register(testObserver);

        Assert.assertEquals(-1L, testObserver.getSequence());

        DocumentRevision doc1Mut = new DocumentRevision();
        doc1Mut.setBody(bodyOne);
        DocumentRevision doc1 = core.createDocumentFromRevision(doc1Mut);
        Assert.assertNotNull(doc1);
        Assert.assertEquals(1L, core.getLastSequence());
        Assert.assertEquals(1L, testObserver.getSequence());

        DocumentRevision doc2Mut = new DocumentRevision();
        doc2Mut.setBody(bodyOne);
        DocumentRevision doc2 = core.createDocumentFromRevision(doc2Mut);
        Assert.assertNotNull(doc2);
        Assert.assertEquals(2L, core.getLastSequence());
        Assert.assertEquals(2L, testObserver.getSequence());

        DocumentRevision doc1_1Mut = doc1;
        doc1_1Mut.setBody(bodyTwo);
        DocumentRevision doc1_1 = core.updateDocumentFromRevision(doc1_1Mut);
        Assert.assertNotNull(doc1_1);
        Assert.assertEquals(3L, core.getLastSequence());
        Assert.assertEquals(3L, testObserver.getSequence());
    }

    public static class TestObserver {        
        Database core ;
        public TestObserver(Database core) {
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
