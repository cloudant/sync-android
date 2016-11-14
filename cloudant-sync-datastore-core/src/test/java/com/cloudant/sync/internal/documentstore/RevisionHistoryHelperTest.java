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

package com.cloudant.sync.internal.documentstore;

import static org.hamcrest.CoreMatchers.equalTo;

import com.cloudant.sync.internal.mazha.DocumentRevs;
import com.cloudant.sync.internal.mazha.json.JSONHelper;
import com.cloudant.sync.documentstore.DocumentBody;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.documentstore.RevisionHistoryHelper;

import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RevisionHistoryHelperTest {

    private JSONHelper jsonHelper = new JSONHelper();

    @Test
    public void revisionHistoryToJson_oneRevisionInHistory_success() {
        Map m = new HashMap<String, Object>();
        m.put("name", "Tom");

        List<DocumentRevision> d = createDBObjects("Tom", "1-a");
        String json = jsonHelper.toJson(RevisionHistoryHelper.revisionHistoryToJson(d));

        DocumentRevs documentRevs = jsonHelper.fromJson(new StringReader(json), DocumentRevs.class);
        Assert.assertEquals("1", documentRevs.getId());
        Assert.assertEquals("1-a", documentRevs.getRev());
        Assert.assertThat(documentRevs.getOthers().entrySet(), equalTo(m.entrySet()));
        Assert.assertEquals(1, documentRevs.getRevisions().getStart());
        Assert.assertThat(documentRevs.getRevisions().getIds(), equalTo(Arrays.asList("a")));

    }

    private List<DocumentRevision> createDBObjects(String name, String... revisions) {
        List<DocumentRevision> res = new ArrayList<DocumentRevision>();
        for(String revision : revisions) {
            res.add(createDBObject(name, revision));
        }
        return res;
    }

    private DocumentRevision createDBObject(String name, String rev) {
        Map m = new HashMap<String, Object>();
        m.put("name", name);
        DocumentBody body = DocumentBodyImpl.bodyWith(m);

        DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
        builder.setDocId("1");
        builder.setRevId(rev);
        builder.setBody(body);
        return builder.build();
    }

    @Test
    public void revisionHistoryToJson_twoRevisionsInHistory_success() {
        Map m = new HashMap<String, Object>();
        m.put("name", "Tom");

        List<DocumentRevision> d = createDBObjects("Tom", "2-b", "1-a");
        String json = jsonHelper.toJson(RevisionHistoryHelper.revisionHistoryToJson(d));

        DocumentRevs documentRevs = jsonHelper.fromJson(new StringReader(json), DocumentRevs.class);
        Assert.assertEquals("1", documentRevs.getId());
        Assert.assertEquals("2-b", documentRevs.getRev());
        Assert.assertThat(documentRevs.getOthers().entrySet(), equalTo(m.entrySet()));
        Assert.assertEquals(2, documentRevs.getRevisions().getStart());
        Assert.assertThat(documentRevs.getRevisions().getIds(), equalTo(Arrays.asList("b", "a")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void revisionHistoryToJson_historyIsInAscendingOrder_exception() {
        List<DocumentRevision> d = createDBObjects("Tom", "1-a", "2-b");
        RevisionHistoryHelper.revisionHistoryToJson(d);
    }

    @Test(expected = NullPointerException.class)
    public void revisionHistoryToJson_null_exception() {
        RevisionHistoryHelper.revisionHistoryToJson(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void revisionHistoryToJson_zeroLengthHistory_exception() {
        RevisionHistoryHelper.revisionHistoryToJson(new ArrayList<DocumentRevision>());
    }

    @Test
    public void getRevisionPath_oneRevision_success() {
        List<DocumentRevision> d = createDBObjects("Tom", "1-a");
        String json = jsonHelper.toJson(RevisionHistoryHelper.revisionHistoryToJson(d));
        DocumentRevs documentRevs = jsonHelper.fromJson(new StringReader(json), DocumentRevs.class);
        List<String> revisions = RevisionHistoryHelper.getRevisionPath(documentRevs);
        Assert.assertThat(revisions, equalTo(Arrays.asList("1-a")));
    }

    @Test
    public void getRevisionPath_twoRevision_success() {
        List<DocumentRevision> d = createDBObjects("Tom", "2-b", "1-a");
        String json = jsonHelper.toJson(RevisionHistoryHelper.revisionHistoryToJson(d));
        DocumentRevs documentRevs = jsonHelper.fromJson(new StringReader(json), DocumentRevs.class);
        List<String> revisions = RevisionHistoryHelper.getRevisionPath(documentRevs);
        Assert.assertThat(revisions, equalTo(Arrays.asList("2-b", "1-a")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void getRevisionPath_revisionStartIsTooSmall_exception() {
        List<DocumentRevision> d = createDBObjects("Tom", "2-b", "1-a");
        String json = jsonHelper.toJson(RevisionHistoryHelper.revisionHistoryToJson(d));
        DocumentRevs documentRevs = jsonHelper.fromJson(new StringReader(json), DocumentRevs.class);
        addOneMoreIdToRevisions(documentRevs);
        RevisionHistoryHelper.getRevisionPath(documentRevs);
    }

    private void addOneMoreIdToRevisions(DocumentRevs documentRevs) {
        DocumentRevs.Revisions r = documentRevs.getRevisions();
        List<String> ids = r.getIds();
        ids.add("1");
        r.setIds(ids);
    }
}
