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

package com.cloudant.mazha;

import com.cloudant.mazha.json.JSONHelper;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class DocumentRevsTest {
    JSONHelper jsonHelper;

    @Before
    public void setUp() {
        jsonHelper = new JSONHelper();
    }

    @Test(expected = RuntimeException.class)
    public void deserialization_unknownSpecialField() throws IOException {
        String s = FileUtils.readFileToString(new File("fixture/document_revs_with_unknown_special_field.json"));
        jsonHelper.fromJson(new StringReader(s), DocumentRevs.class);
    }

    @Test
    public void deserialization_others() throws IOException {
        String s = FileUtils.readFileToString(new File("fixture/document_revs_others.json"));
        DocumentRevs documentRevs = jsonHelper.fromJson(new StringReader(s), DocumentRevs.class);
        Map<String, Object> others = documentRevs.getOthers();
        Assert.assertThat(others.keySet(), hasSize(2));
        Assert.assertThat(others.keySet(), hasItems("a", "b"));
        Assert.assertEquals("A", others.get("a"));
        Assert.assertEquals("B", others.get("b"));
    }

    @Test
    public void deserialization_withAttachments() throws IOException {
        String s = FileUtils.readFileToString(new File("fixture/document_revs_with_attachments_1.json"));
        DocumentRevs documentRevs = jsonHelper.fromJson(new StringReader(s), DocumentRevs.class);

        Assert.assertEquals("Reston", documentRevs.getId());
        Assert.assertEquals("5-9d234010f32f593edafc04620f3cf2bd", documentRevs.getRev());

        Assert.assertThat(documentRevs.getAttachments().keySet(), hasSize(2));
        Assert.assertThat(documentRevs.getAttachments().keySet(), hasItems("Reston-0.2.0.tgz", "Reston-0.1.1.tgz"));

        Assert.assertThat(documentRevs.getOthers().keySet(), hasSize(8));
        Assert.assertThat(documentRevs.getOthers().keySet(), hasItems("name", "description", "versions",
                "maintainers", "time", "author", "repository", "dist-tags"));
    }
}
