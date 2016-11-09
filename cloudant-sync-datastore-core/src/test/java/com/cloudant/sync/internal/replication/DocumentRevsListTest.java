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

package com.cloudant.sync.internal.replication;

import com.cloudant.sync.internal.mazha.DocumentRevs;
import com.cloudant.sync.internal.datastore.DocumentRevsList;
import com.cloudant.sync.internal.util.JSONUtils;
import com.cloudant.sync.util.TestUtils;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class DocumentRevsListTest {

    DocumentRevs documentRevs1;
    DocumentRevs documentRevs2;
    List<DocumentRevs> documentRevs;

    @Before
    public void setup() throws IOException {
        documentRevs1 = getDocumentRevsFromFile(TestUtils.loadFixture("" +
                "fixture/document_revs_only_one_revision.json"));
        documentRevs2 = getDocumentRevsFromFile(TestUtils.loadFixture("fixture/document_revs_with_everything.json"));
        documentRevs = new ArrayList<DocumentRevs>();
        documentRevs.add(documentRevs1);
        documentRevs.add(documentRevs2);
    }

    @Test(expected = NullPointerException.class)
    public void null_exception() {
        new DocumentRevsList(null);
    }

    @Test
    public void get() {
        DocumentRevsList documentRevsList = new DocumentRevsList(documentRevs);
        Assert.assertEquals("cdb1a2fec33d146fe07a44ea823bf3ae", documentRevsList.get(0).getId());
        Assert.assertEquals(4, documentRevsList.get(1).getRevisions().getIds().size());
    }

    @Test
    public void iterate() {
        DocumentRevsList documentRevsList = new DocumentRevsList(documentRevs);
        Iterator<DocumentRevs> i = documentRevsList.iterator();
        Assert.assertTrue(i.hasNext());
        Assert.assertNotNull(i.next());
        Assert.assertTrue(i.hasNext());
        Assert.assertNotNull(i.next());
        Assert.assertFalse(i.hasNext());
    }


    private DocumentRevs getDocumentRevsFromFile(File file) throws IOException {
        byte[] data = FileUtils.readFileToByteArray(file);
        return JSONUtils.deserialize(data, DocumentRevs.class);
    }

    @Test
    public void order_by_min_generation() throws IOException {
        documentRevs1 = getDocumentRevsFromFile(TestUtils.loadFixture("fixture/document_revs_only_one_revision.json"));
        documentRevs2 = getDocumentRevsFromFile(TestUtils.loadFixture("fixture/document_revs_only_two_revision.json"));

        documentRevs = new ArrayList<DocumentRevs>();
        documentRevs.add(documentRevs2);
        documentRevs.add(documentRevs1);

        DocumentRevsList documentRevsList = new DocumentRevsList(documentRevs);
        Iterator<DocumentRevs> i = documentRevsList.iterator();

        Assert.assertTrue(i.hasNext());
        Assert.assertEquals(1, i.next().getRevisions().getStart());
        Assert.assertTrue(i.hasNext());
        Assert.assertEquals(3, i.next().getRevisions().getStart());
        Assert.assertFalse(i.hasNext());

        // Assert the original list not modified
        Assert.assertEquals(3, documentRevs.get(0).getRevisions().getStart());
        Assert.assertEquals(1, documentRevs.get(1).getRevisions().getStart());
    }
}
