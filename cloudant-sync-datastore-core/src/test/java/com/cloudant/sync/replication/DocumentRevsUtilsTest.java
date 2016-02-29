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

import com.cloudant.mazha.DocumentRevs;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.datastore.DocumentRevsUtils;
import com.cloudant.sync.util.JSONUtils;
import com.cloudant.sync.util.TestUtils;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DocumentRevsUtilsTest {

    @Test
    public void test_load_data_from_file() throws IOException {
        DocumentRevs documentRevs = getDocumentRevsFromFile(TestUtils.loadFixture
                ("fixture/document_revs_with_everything.json"));
        Assert.assertNotNull(documentRevs);
        Assert.assertEquals("cdb1a2fec33d146fe07a44ea823bf3ae", documentRevs.getId());
        Assert.assertEquals("4-47d7102726fc89914431cb217ab7bace", documentRevs.getRev());
        Assert.assertEquals(4, documentRevs.getRevisions().getStart());
        Assert.assertEquals(4, documentRevs.getRevisions().getIds().size());
        Assert.assertThat(documentRevs.getOthers().keySet(), hasItems("title", "album"));
    }

    @Test(expected = NullPointerException.class)
    public void createRevisionIdHistory_null_exception() {
        DocumentRevsUtils.createRevisionIdHistory(null);
    }

    @Test
    public void createRevisionIdHistory_historyWithFourIds() throws IOException {
        DocumentRevs documentRevs = getDocumentRevsFromFile(TestUtils.loadFixture("fixture/document_revs_with_everything.json"));
        List<String> revisions = DocumentRevsUtils.createRevisionIdHistory(documentRevs);
        Assert.assertEquals(4, revisions.size());
        Assert.assertEquals("1-421ff3d58df47ea6c5e83ca65efb2fa9", revisions.get(0));
        Assert.assertEquals("2-74e0572530e3b4cd4776616d2f591a96", revisions.get(1));
        Assert.assertEquals("3-d8e1fb8127d8dd732d9ae46a6c38ae3c", revisions.get(2));
        Assert.assertEquals("4-47d7102726fc89914431cb217ab7bace", revisions.get(3));
    }

    @Test
    public void createRevisionIdHistory_onlyOneRevision() throws IOException {
        DocumentRevs documentRevs = getDocumentRevsFromFile(TestUtils.loadFixture("fixture/document_revs_only_one_revision.json"));
        List<String> revisions = DocumentRevsUtils.createRevisionIdHistory(documentRevs);
        Assert.assertEquals(1, revisions.size());
        Assert.assertEquals("1-47d7102726fc89914431cb217ab7bace", revisions.get(0));
    }

    private DocumentRevs getDocumentRevsFromFile(File file) throws IOException {
        byte[] data = FileUtils.readFileToByteArray(file);
        return JSONUtils.deserialize(data, DocumentRevs.class);
    }

    @Test
    public void createDocument() throws Exception {

        DocumentRevs documentRevs = getDocumentRevsFromFile(TestUtils.loadFixture("fixture/document_revs_with_everything.json"));
        DocumentRevision documentRevision = DocumentRevsUtils.createDocument(documentRevs);

        Assert.assertEquals("cdb1a2fec33d146fe07a44ea823bf3ae", documentRevision.getId());
        Assert.assertEquals("4-47d7102726fc89914431cb217ab7bace", documentRevision.getRevision());

        Map<String, Object> body = documentRevision.getBody().asMap();
        Assert.assertEquals(2, body.size());
        Assert.assertThat(body.keySet(), hasItems("title", "album"));
        Assert.assertEquals("A Flush Of Blood To My Head", body.get("album"));
        Assert.assertEquals("Trouble Two", body.get("title"));
    }

    @Test(expected = NullPointerException.class)
    public void createDocument_null_exception() {
        DocumentRevsUtils.createDocument(null);
    }

    @Test
    public void createDocument_deletedDocument_documentMarkedAsDeleted() throws Exception {
        DocumentRevs documentRevs = getDocumentRevsFromFile(TestUtils.loadFixture("fixture/document_revs_deleted.json"));
        DocumentRevision documentRevision = DocumentRevsUtils.createDocument(documentRevs);

        Assert.assertTrue(documentRevision.isDeleted());
    }
}
