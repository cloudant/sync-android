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

import com.cloudant.common.CouchConstants;
import com.cloudant.common.CouchUtils;
import com.cloudant.common.RequireRunningCouchDB;
import com.cloudant.mazha.json.JSONHelper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.core.Is.is;

@Category(RequireRunningCouchDB.class)
public class GetDocumentRevsTest extends CouchClientTestBase {

    /**
     * Example output for get docs with revisions
     * {
     *     "_id" : "05195cd197799bde8af095e9a3185597",
     *     "_rev" : "2-dc16dcd3a3faa8a6b5cdc21b2e16d6a4",
     *     "hello" : "world",
     *     "Here" : "Is Cloudant!",
     *     "_revisions" : {
     *         "start" : 2,
     *         "ids" : [
     *         "dc16dcd3a3faa8a6b5cdc21b2e16d6a4",
     *         "15f65339921e497348be384867bb940f" ]
     *     }
     * }
     */
    @Test
    public void getDocRevisions_idAndRevAndMapClass_revsMustBeReturned() {
        Response[] responses = createAndUpdateDocumentAndReturnRevisions();
        Map<String, Object> revs = client.getDocRevisions(responses[1].getId(), responses[1].getRev(),
                JSONHelper.STRING_MAP_TYPE_DEF);

        Assert.assertThat(revs.keySet(), hasItem(CouchConstants._revisions));

        Map<String, Object> _revisions = (Map<String, Object>) revs.remove(CouchConstants._revisions);
        Assert.assertThat(_revisions.keySet(), hasItems(CouchConstants.start, CouchConstants.ids));
        Assert.assertThat((Integer)_revisions.get(CouchConstants.start), is(equalTo(2)));

        List<String> ids = (List<String>) _revisions.get(CouchConstants.ids);
        Assert.assertThat(ids.size(), is(equalTo(2)));
        Assert.assertThat(ids, hasItems(CouchUtils.getRevisionIdsSuffix(responses[0].getRev(), responses[1].getRev())));
    }

    public Response[] createAndUpdateDocumentAndReturnRevisions() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        Assert.assertThat(res.getRev(), startsWith("1-"));

        Map<String, Object> doc = client.getDocument(res.getId());
        doc.put("Here", "Is Cloudant!");

        Response updatedRes = client.update(res.getId(), doc);
        Assert.assertThat(updatedRes.getRev(), startsWith("2-"));

        return new Response[]{ res, updatedRes };
    }

    @Test
    public void getDocRevisions_idAndRev_revsMustBeReturned() {
        Response[] responses = createAndUpdateDocumentAndReturnRevisions();
        DocumentRevs docRevs = client.getDocRevisions(responses[1].getId(), responses[1].getRev());

        Assert.assertThat(docRevs, is(notNullValue()));
        Assert.assertThat(docRevs.getId(), is(equalTo(responses[1].getId())));
        Assert.assertThat(docRevs.getRev(), is(equalTo(responses[1].getRev())));
        Assert.assertThat(docRevs.getDeleted(), is(equalTo(Boolean.FALSE)));

        Assert.assertThat(docRevs.getOthers().size(), is(equalTo(2)));
        Assert.assertThat(docRevs.getOthers().keySet(), hasItems("hello", "Here"));
        Assert.assertThat((String)docRevs.getOthers().get("hello"), is("world"));
        Assert.assertThat((String)docRevs.getOthers().get("Here"), is("Is Cloudant!"));
    }

    @Test
    public void getDocRevisions_documentDeleted_mustReturnedMarkedAsDeleted() {
        Response[] responses = createAndUpdateDocumentAndReturnRevisions();
        Response response = client.delete(responses[1].getId(), responses[1].getRev());

        DocumentRevs documentRevs = client.getDocRevisions(response.getId(), response.getRev());
        Assert.assertThat(documentRevs.getId(), is(equalTo(response.getId())));
        Assert.assertThat(documentRevs.getRev(), is(equalTo(response.getRev())));
        Assert.assertTrue(documentRevs.getDeleted());
        Assert.assertEquals(3, documentRevs.getRevisions().getStart());
        Assert.assertEquals(3, documentRevs.getRevisions().getIds().size());
        Assert.assertThat(documentRevs.getRevisions().getIds(), hasItems(CouchUtils.getRevisionIdsSuffix(
                responses[0].getRev(), responses[1].getRev(), response.getRev()
        )));
        Assert.assertEquals(0, documentRevs.getOthers().size());
    }

    @Test(expected = NoResourceException.class)
    public void getDocRevisions_docNotExistAndMapClass_exception() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        client.getDocRevisions("bad_id", res.getRev());
    }

    @Test(expected = NoResourceException.class)
    public void getDocRevisions_docNotExist_exception() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        client.getDocRevisions("bad_id", res.getRev());
    }

    @Test(expected = NoResourceException.class)
    public void getDocRevisions_wrongRevIdAndMapClass_exception() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        client.getDocRevisions(res.getId(), res.getRev() + "bad");
    }

    @Test(expected = NoResourceException.class)
    public void getDocRevisions_wrongRevId_exception() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        client.getDocRevisions(res.getId(), res.getRev() + "bad");
    }
}
