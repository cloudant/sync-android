/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2015 Cloudant, Inc. All rights reserved.
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

import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.internal.common.ValueListMap;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;

public class DatabaseImplRevsDiffTest extends BasicDatastoreTestBase{

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void revsDiff_emptyInput_returnEmpty() throws Exception
    {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("revisions cannot be empty");
        datastore.revsDiff(new ValueListMap<String, String>());
    }

    @Test
    public void revsDiff_oneDocOneRev_returnNothing() throws Exception {
        DocumentRevision revMut = new DocumentRevision();
        revMut.setBody(bodyOne);
        DocumentRevision rev = datastore.create(revMut);
        ValueListMap<String, String> revs = new ValueListMap<String, String>();
        revs.addValueToKey(rev.getId(), rev.getRevision());
        Map<String, List<String>> missingRevs = datastore.revsDiff(revs);
        Assert.assertEquals(0, missingRevs.size());
    }

    @Test
    public void revsDiff_oneDocOneRev_returnOne() throws Exception {
        DocumentRevision revMut = new DocumentRevision();
        revMut.setBody(bodyOne);
        DocumentRevision rev = datastore.create(revMut);
        ValueListMap<String, String> revs = new ValueListMap<String, String>();
        revs.addValueToKey(rev.getId(), "2-a");
        Map<String, List<String>> missingRevs = datastore.revsDiff(revs);
        Assert.assertEquals(1, missingRevs.size());
        Assert.assertTrue(missingRevs.get(rev.getId()).contains("2-a"));
    }

    @Test
    public void revsDiff_oneDocTwoRevs_returnNothing() throws Exception {
        DocumentRevision revMut1 = new DocumentRevision();
        revMut1.setBody(bodyOne);
        DocumentRevision rev1 = datastore.create(revMut1);
        DocumentRevision rev2Mut = rev1;
        rev2Mut.setBody(bodyTwo);
        DocumentRevision rev2 = datastore.update(rev2Mut);
        ValueListMap<String, String> revs = new ValueListMap<String, String>();
        revs.addValueToKey(rev1.getId(), rev1.getRevision());
        revs.addValueToKey(rev2.getId(), rev2.getRevision());
        Map<String, List<String>> missingRevs = datastore.revsDiff(revs);
        Assert.assertEquals(0, missingRevs.size());
    }

    @Test
    public void revsDiff_twoDoc_returnOneDoc() throws Exception {
        DocumentRevision revMut1 = new DocumentRevision();
        revMut1.setBody(bodyOne);
        DocumentRevision rev1 = datastore.create(revMut1);
        DocumentRevision revMut2 = new DocumentRevision();
        revMut2.setBody(bodyTwo);
        DocumentRevision rev2 = datastore.create(revMut2);
        ValueListMap<String, String> revs = new ValueListMap<String, String>();
        revs.addValueToKey(rev1.getId(), rev1.getRevision());
        revs.addValueToKey(rev1.getId(), "2-a");
        revs.addValueToKey(rev2.getId(), rev2.getRevision());

        Map<String, List<String>> missingRevs = datastore.revsDiff(revs);
        Assert.assertEquals(1, missingRevs.size());
        Assert.assertTrue(missingRevs.get(rev1.getId()).contains("2-a"));
    }

    @Test
    public void revsDiff_twoDoc_returnTwoDocs() throws Exception {
        DocumentRevision revMut1 = new DocumentRevision();
        revMut1.setBody(bodyOne);
        DocumentRevision rev1 = datastore.create(revMut1);
        DocumentRevision revMut2 = new DocumentRevision();
        revMut2.setBody(bodyTwo);
        DocumentRevision rev2 = datastore.create(revMut2);

        ValueListMap<String, String> revs = new ValueListMap<String, String>();
        revs.addValueToKey(rev1.getId(), rev1.getRevision());
        revs.addValueToKey(rev1.getId(), "2-a");
        revs.addValueToKey(rev2.getId(), rev2.getRevision());
        revs.addValueToKey(rev2.getId(), "2-a");

        Map<String, List<String>> missingRevs = datastore.revsDiff(revs);
        Assert.assertEquals(2, missingRevs.size());
        Assert.assertTrue(missingRevs.get(rev1.getId()).contains("2-a"));
        Assert.assertTrue(missingRevs.get(rev2.getId()).contains("2-a"));
    }

    @Test
    public void revsDiff_oneDocWithManyRevisions_onlyNonExistingRevisionsReturned() throws Exception {
        DocumentRevision revMut1 = new DocumentRevision();
        revMut1.setBody(bodyOne);
        DocumentRevision rev1 = datastore.create(revMut1);
        DocumentRevision revMut2 = new DocumentRevision();
        revMut2.setBody(bodyTwo);
        DocumentRevision rev2 = datastore.create(revMut2);

        ValueListMap<String, String> revs = new ValueListMap<String, String>();
        // Add two existing revisions first, and then add many
        // revisions that do not exist yet. The two existing
        // revisions should not return in the api result.
        revs.addValueToKey(rev1.getId(), rev1.getRevision());
        revs.addValueToKey(rev2.getId(), rev2.getRevision());
        for(int i = 1 ; i < 100000 ; i ++) {
            revs.addValueToKey(rev1.getId(), i + "-a");
        }
        Map<String, List<String>> missing = datastore.revsDiff(revs);
        Assert.assertEquals(1, missing.size());
        Assert.assertEquals(99999, missing.get(rev1.getId()).size());
        Assert.assertTrue(missing.get(rev1.getId()).contains("1-a"));
        Assert.assertTrue(missing.get(rev1.getId()).contains("499-a"));
        Assert.assertTrue(missing.get(rev1.getId()).contains("99999-a"));
        Assert.assertFalse(missing.get(rev1.getId()).contains(rev1.getRevision()));
    }

}
