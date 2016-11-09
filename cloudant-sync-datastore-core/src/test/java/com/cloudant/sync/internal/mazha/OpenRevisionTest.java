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

package com.cloudant.sync.internal.mazha;

import com.cloudant.sync.internal.mazha.DocumentRevs;
import com.cloudant.sync.internal.mazha.MissingOpenRevision;
import com.cloudant.sync.internal.mazha.OkOpenRevision;
import com.cloudant.sync.internal.mazha.OpenRevision;
import com.cloudant.sync.internal.mazha.json.JSONHelper;
import com.cloudant.sync.util.TestUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class OpenRevisionTest {

    JSONHelper jsonHelper;

    @Before
    public void setUp() {
        jsonHelper = new JSONHelper();
    }

    @Test
    public void deserialization_ok() throws IOException {
        String s = FileUtils.readFileToString(TestUtils.loadFixture("fixture/open_revisions_ok.json"));
        List<OpenRevision> openRevisionList = jsonHelper.fromJson(
                new StringReader(s), new TypeReference<List<OpenRevision>>() {});
        Assert.assertThat(openRevisionList, hasSize(1));
        Assert.assertTrue(openRevisionList.get(0) instanceof OkOpenRevision);
        DocumentRevs documentRevs = ((OkOpenRevision)openRevisionList.get(0)).getDocumentRevs();
        Assert.assertEquals("a", documentRevs.getId());
        Assert.assertEquals("1-a", documentRevs.getRev());
    }

    @Test
    public void deserialization_missingRevision() throws IOException {
        String s = FileUtils.readFileToString(TestUtils.loadFixture("fixture/open_revisions_missing.json"));
        List<OpenRevision> openRevisionList = jsonHelper.fromJson(
                new StringReader(s), new TypeReference<List<OpenRevision>>() {
        });
        Assert.assertThat(openRevisionList, hasSize(1));
        Assert.assertTrue(openRevisionList.get(0) instanceof MissingOpenRevision);
        MissingOpenRevision missingOpenRevision = (MissingOpenRevision) openRevisionList.get(0);
        Assert.assertEquals("2-x", missingOpenRevision.getRevision());
    }

    @Test
    public void deserialization_okAndMissingRevision() throws IOException {
        String s = FileUtils.readFileToString(TestUtils.loadFixture("fixture/open_revisions_ok_and_missing.json"));
        List<OpenRevision> openRevisionList = jsonHelper.fromJson(
                new StringReader(s), new TypeReference<List<OpenRevision>>() {
        });
        Assert.assertThat(openRevisionList, hasSize(2));
        Assert.assertTrue(openRevisionList.get(0) instanceof OkOpenRevision);
        DocumentRevs documentRevs = ((OkOpenRevision)openRevisionList.get(0)).getDocumentRevs();
        Assert.assertEquals("a", documentRevs.getId());
        Assert.assertEquals("1-a", documentRevs.getRev());

        Assert.assertTrue(openRevisionList.get(1) instanceof MissingOpenRevision);
        MissingOpenRevision missingOpenRevision = (MissingOpenRevision) openRevisionList.get(1);
        Assert.assertEquals("2-x", missingOpenRevision.getRevision());
    }

    @Test
    public void deserialization_manyOpenRevisions() throws IOException {
        String s = FileUtils.readFileToString(TestUtils.loadFixture("fixture/open_revisions_many.json"));
        List<OpenRevision> openRevisionList = jsonHelper.fromJson(
                new StringReader(s), new TypeReference<List<OpenRevision>>() {});

        Assert.assertEquals(11, openRevisionList.size());
        for(OpenRevision openRevision : openRevisionList) {
            Assert.assertTrue(openRevision instanceof OkOpenRevision);
        }
    }
    @Test
    public void deserialization_empty() throws IOException {
        String s = FileUtils.readFileToString(TestUtils.loadFixture("fixture/open_revisions_empty.json"));
        List<OpenRevision> openRevisionList = jsonHelper.fromJson(
                new StringReader(s), new TypeReference<List<OpenRevision>>() {});
        Assert.assertThat(openRevisionList, hasSize(0));
    }
}
