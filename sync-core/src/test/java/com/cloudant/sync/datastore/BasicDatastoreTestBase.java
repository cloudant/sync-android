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

import com.cloudant.sync.util.CouchUtils;
import com.cloudant.sync.util.TestUtils;

import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;

import java.io.File;

public abstract class BasicDatastoreTestBase extends DatastoreTestBase {


    String documentOneFile = "fixture/document_1.json";
    String documentTwoFile = "fixture/document_2.json";

    byte[] jsonData = null;
    DocumentBody bodyOne = null;
    DocumentBody bodyTwo = null;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        jsonData = FileUtils.readFileToByteArray(TestUtils.loadFixture(documentOneFile));
        bodyOne = new BasicDocumentBody(jsonData);

        jsonData = FileUtils.readFileToByteArray(TestUtils.loadFixture(documentTwoFile));
        bodyTwo = new BasicDocumentBody(jsonData);
    }

    @After
    public void tearDown() throws Exception {
        super.testDown();
    }

    void createTwoDocuments() {
        BasicDocumentRevision rev_1 = datastore.createDocument(bodyOne);
        validateNewlyCreatedDocument(rev_1);
        BasicDocumentRevision rev_2 = datastore.createDocument(bodyTwo);
        validateNewlyCreatedDocument(rev_2);
    }

    BasicDocumentRevision[] createThreeDocuments() throws ConflictException {
        BasicDocumentRevision rev_1 = datastore.createDocument(bodyOne);
        validateNewlyCreatedDocument(rev_1);
        BasicDocumentRevision rev_2 = datastore.createDocument(bodyTwo);
        validateNewlyCreatedDocument(rev_2);
        BasicDocumentRevision rev_3 = datastore.createDocument(bodyTwo);
        validateNewlyCreatedDocument(rev_3);
        BasicDocumentRevision rev_4 = (BasicDocumentRevision)datastore.updateDocument(rev_3.getId(), rev_3.getRevision(), bodyOne);
        Assert.assertNotNull(rev_4);
        return new BasicDocumentRevision[] { rev_1, rev_2, rev_4 };
    }

    void validateNewlyCreatedDocument(BasicDocumentRevision rev) {
        Assert.assertNotNull(rev);
        CouchUtils.validateDocumentId(rev.getId());
        CouchUtils.validateRevisionId(rev.getRevision());
        Assert.assertEquals(1, CouchUtils.generationFromRevId(rev.getRevision()));
        Assert.assertTrue(rev.isCurrent());
        Assert.assertTrue(rev.getParent() == -1L);
    }

    void validateNewlyCreateLocalDocument(BasicDocumentRevision rev) {
        Assert.assertNotNull(rev);
        CouchUtils.validateDocumentId(rev.getId());
        Assert.assertEquals("1-local", rev.getRevision());
        Assert.assertTrue(rev.isCurrent());
    }
}
