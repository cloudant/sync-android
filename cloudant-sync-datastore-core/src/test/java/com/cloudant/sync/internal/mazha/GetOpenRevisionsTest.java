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

import com.cloudant.sync.internal.common.CouchUtils;
import com.cloudant.common.RequireRunningCouchDB;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.core.Is.is;

@Category(RequireRunningCouchDB.class)
@RunWith(Parameterized.class)
public class GetOpenRevisionsTest extends CouchClientTestBase {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {false}, {true},
        });
    }

    @Parameterized.Parameter
    public boolean pullAttachmentsInline;


    @Test(expected = IllegalArgumentException.class)
    public void getDocWithOpenRevisions_zeroOpenRev() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        ArrayList<String> revIds = new ArrayList<String>();
        ArrayList<String> attsSince = new ArrayList<String>();
        client.getDocWithOpenRevisions(res.getId(), revIds, attsSince, pullAttachmentsInline);
    }

    @Test
    public void getDocWithOpenRevisions_onlyOneOpenRev_correctDataMustReturn() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        String[] openRevs = ClientTestUtils.createDocumentWithConflicts(client, res);
        String openRev = openRevs[0];
        ArrayList<String> revIds = new ArrayList<String>();
        revIds.add(openRev);
        ArrayList<String> attsSince = new ArrayList<String>();

        GetOpenRevisionsResponse response =
                new GetOpenRevisionsResponse(client.getDocWithOpenRevisions(res.getId(), revIds, attsSince, pullAttachmentsInline));
        Assert.assertEquals(1, response.getOkRevisionMap().size());
        Assert.assertThat(response.getOkRevisionMap().keySet(), hasItems(openRev));

        List<String> revisionIds = response.findRevisionsIdForOpenRev(openRev);
        Assert.assertThat(revisionIds.size(), is(equalTo(3)));
        Assert.assertThat(revisionIds.get(0), is(equalTo(CouchUtils.getRevisionIdSuffix(openRev))));
        Assert.assertThat(revisionIds.get(2), is(equalTo(CouchUtils.getRevisionIdSuffix(res.getRev()))));
    }

    @Test
    public void getDocWithOpenRevisions_threeOpenRevs_correctDataMustReturn() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        String[] openRevs = ClientTestUtils.createDocumentWithConflicts(client, res);
        ArrayList<String> attsSince = new ArrayList<String>();

        List<OpenRevision> openRevisionList = client.getDocWithOpenRevisions(res.getId(), Arrays.asList(openRevs), attsSince, pullAttachmentsInline);
        GetOpenRevisionsResponse response = new GetOpenRevisionsResponse(openRevisionList);

        Assert.assertEquals(3, response.getOkRevisionMap().size());
        Assert.assertThat(response.getOkRevisionMap().keySet(), hasItems(openRevs));

        assertRevisionIdListAreCorrect(openRevs[0], response.findRevisionsIdForOpenRev(openRevs[0]), 3);
        assertRevisionIdListAreCorrect(openRevs[1], response.findRevisionsIdForOpenRev(openRevs[1]), 2);
        assertRevisionIdListAreCorrect(openRevs[2], response.findRevisionsIdForOpenRev(openRevs[2]), 2);
    }

    @Test
    public void getDocWithOpenRevisions_deletedDocument_correctDataMustReturn() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        String[] openRevs = ClientTestUtils.createDocumentWithConflicts(client, res);

        // Delete the document changes the open revisions
        Response res2 = client.delete(res.getId(), openRevs[0]);
        Assert.assertThat(res2.getRev(), startsWith("4-"));

        ArrayList<String> revIds = new ArrayList<String>();
        ArrayList<String> attsSince = new ArrayList<String>();
        revIds.add(openRevs[1]);
        revIds.add(openRevs[2]);
        revIds.add(res2.getRev());

        List<OpenRevision> openRevisionList = client.getDocWithOpenRevisions(res.getId(), revIds, attsSince, pullAttachmentsInline);
        GetOpenRevisionsResponse response = new GetOpenRevisionsResponse(openRevisionList);

        Assert.assertEquals(3, response.getOkRevisionMap().size());
        Assert.assertThat(response.getOkRevisionMap().keySet(), hasItems(res2.getRev(), openRevs[1], openRevs[2]));

        List<String> revisionIds = response.findRevisionsIdForOpenRev(res2.getRev());
        Assert.assertEquals(4, revisionIds.size());
        Assert.assertEquals(CouchUtils.getRevisionIdSuffix(res2.getRev()), revisionIds.get(0));
        Assert.assertEquals(CouchUtils.getRevisionIdSuffix(openRevs[0]), revisionIds.get(1));
        Assert.assertEquals(CouchUtils.getRevisionIdSuffix(res.getRev()), revisionIds.get(3));
    }

    private void assertRevisionIdListAreCorrect(String rev, List<String> revisionIds, int numberOfRevisionIds) {
        Assert.assertThat("Revisions ids (hashes)", revisionIds, is(notNullValue()));
        Assert.assertThat("Revisions ids (hashes)", revisionIds.size(), is(equalTo(numberOfRevisionIds)));
        Assert.assertThat("Revisions ids (hashes)",
                revisionIds, hasItem(CouchUtils.getRevisionIdSuffix(rev)));
    }

    @Test
    public void getDocWithOpenRevisions_invalidRevision() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        String[] openRevs = ClientTestUtils.createDocumentWithConflicts(client, res);

        {
            String invalidRev = openRevs[2] + "bad";
            ArrayList<String> revIds = new ArrayList<String>();
            ArrayList<String> attsSince = new ArrayList<String>();
            revIds.add(invalidRev);

            GetOpenRevisionsResponse response =
                    new GetOpenRevisionsResponse(client.getDocWithOpenRevisions(res.getId(), revIds, attsSince, pullAttachmentsInline));

            Assert.assertThat("No valid open revision", response.getOkRevisionMap().size(), equalTo(0));
            Assert.assertThat("Invalid open revision number", response.getMissingRevisionsMap().size(), equalTo(1));
            Assert.assertThat("Invalid open revision", response.getMissingRevisionsMap().keySet(), hasItem(invalidRev));
        }

        {
            String invalidRev1 = openRevs[0] + "bad";
            String invalidRev2 = openRevs[1] + "bad";
            ArrayList<String> revIds = new ArrayList<String>();
            ArrayList<String> attsSince = new ArrayList<String>();
            revIds.add(invalidRev1);
            revIds.add(invalidRev2);
            revIds.add(openRevs[0]);
            revIds.add(openRevs[1]);

            GetOpenRevisionsResponse response =
                    new GetOpenRevisionsResponse(client.getDocWithOpenRevisions(res.getId(),
                            revIds, attsSince, pullAttachmentsInline));

            Assert.assertThat("Valid valid revision should return as 'ok'",
                    response.getOkRevisionMap().size(), equalTo(2));
            Assert.assertThat("Valid open revision",
                    response.getOkRevisionMap().keySet(), hasItems(openRevs[0], openRevs[1]));

            Assert.assertThat("Invalid revision should return as 'missing'",
                    response.getMissingRevisionsMap().size(), equalTo(2));
            Assert.assertThat("Invalid revision should return as 'miss'",
                    response.getMissingRevisionsMap().keySet(), hasItems(invalidRev1, invalidRev2));
        }
    }

    @Test
    public void getDocWithOpenRevision_documentForest() {
        Response res = ClientTestUtils.createHelloWorldDoc(client);
        String[] openRevs = ClientTestUtils.createDocumentForest(client, res);
        ArrayList<String> revIds = new ArrayList<String>();
        ArrayList<String> attsSince = new ArrayList<String>();

        List<OpenRevision> openRevisionList = client.getDocWithOpenRevisions(res.getId(), Arrays.asList(openRevs), attsSince, pullAttachmentsInline);

        GetOpenRevisionsResponse response = new GetOpenRevisionsResponse(openRevisionList);
        Assert.assertEquals(2, response.getOkRevisionMap().size());
        Assert.assertEquals(0, response.getMissingRevisionsMap().size());
        Assert.assertThat(response.getOkRevisionMap().keySet(), hasItems(openRevs));

        assertRevisionIdListAreCorrect(openRevs[0], response.findRevisionsIdForOpenRev(openRevs[0]), 3);
        assertRevisionIdListAreCorrect(openRevs[1], response.findRevisionsIdForOpenRev(openRevs[1]), 2);
    }
}
