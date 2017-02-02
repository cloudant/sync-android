/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
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

package com.cloudant.sync.internal.documentstore;

import com.cloudant.sync.documentstore.DocumentBody;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.internal.common.CouchUtils;
import com.cloudant.sync.util.TestUtils;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;

;

public abstract class BasicDatastoreTestBase extends DatastoreTestBase {


    String documentOneFile = "fixture/document_1.json";
    String documentTwoFile = "fixture/document_2.json";

    byte[] jsonData = null;
    DocumentBody bodyOne = null;
    DocumentBody bodyTwo = null;

    @Before
    public void setUp() throws Exception {
        jsonData = FileUtils.readFileToByteArray(TestUtils.loadFixture(documentOneFile));
        bodyOne = new DocumentBodyImpl(jsonData);

        jsonData = FileUtils.readFileToByteArray(TestUtils.loadFixture(documentTwoFile));
        bodyTwo = new DocumentBodyImpl(jsonData);
    }

    void createTwoDocuments() throws Exception {
        DocumentRevision rev_1Mut = new DocumentRevision();
        rev_1Mut.setBody(bodyOne);
        DocumentRevision rev_1 = datastore.create(rev_1Mut);
        validateNewlyCreatedDocument(rev_1);
        DocumentRevision rev_2Mut = new DocumentRevision();
        rev_2Mut.setBody(bodyTwo);
        DocumentRevision rev_2 = datastore.create(rev_2Mut);
        validateNewlyCreatedDocument(rev_2);
    }

    InternalDocumentRevision[] createThreeDocuments() throws Exception {
        DocumentRevision rev_1 = new DocumentRevision();
        rev_1.setBody(bodyOne);
        InternalDocumentRevision rev_1_i = (InternalDocumentRevision)datastore.create(rev_1);
        validateNewlyCreatedDocument(rev_1_i);

        DocumentRevision rev_2 = new DocumentRevision();
        rev_2.setBody(bodyTwo);
        InternalDocumentRevision rev_2_i = (InternalDocumentRevision)datastore.create(rev_2);
        validateNewlyCreatedDocument(rev_1_i);

        DocumentRevision rev_3 = new DocumentRevision();
        rev_3.setBody(bodyTwo);
        DocumentRevision rev_3_a = datastore.create(rev_3);
        validateNewlyCreatedDocument(rev_3_a);
        rev_3_a.setBody(bodyOne);
        InternalDocumentRevision rev_3_i = (InternalDocumentRevision)datastore.update(rev_3_a);
        Assert.assertNotNull(rev_3_i);

        return new InternalDocumentRevision[] { rev_1_i, rev_2_i, rev_3_i };
    }

    void validateNewlyCreatedDocument(DocumentRevision rev) {
        Assert.assertNotNull(rev);
        CouchUtils.validateDocumentId(rev.getId());
        CouchUtils.validateRevisionId(rev.getRevision());
        Assert.assertEquals(1, CouchUtils.generationFromRevId(rev.getRevision()));
        Assert.assertTrue(((InternalDocumentRevision)rev).isCurrent());
        Assert.assertTrue(((InternalDocumentRevision)rev).getParent() == -1L);
    }

    void validateNewlyCreateLocalDocument(DocumentRevision rev) {
        Assert.assertNotNull(rev);
        CouchUtils.validateDocumentId(rev.getId());
        Assert.assertEquals("1-local", rev.getRevision());
        Assert.assertTrue(((InternalDocumentRevision)rev).isCurrent());
    }
}
