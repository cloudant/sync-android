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

package com.cloudant.sync.internal.replication;

import com.cloudant.sync.internal.mazha.ChangesResult;
import com.cloudant.sync.internal.util.JSONUtils;
import com.cloudant.sync.util.TestUtils;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ChangesResultWrapperTest {

    @Before
    public void setUp() throws IOException {
    }

    @Test
    public void load_data_from_file_with_really_long_last_sequence() throws Exception {
        String expectedLastSeq = "7-g1AAAADfeJzLYWBgYMlgTmGQS0lKzi9KdUhJMjTUyyrNSS3QS87JL01JzCvRy0styQGqY0pkSLL___9_ViIbmg5jXDqSHIBkUj1YEwOx1uSxAEmGBiAF1LcfU6MJfo0HIBqBNjJmAQBtaklG";
        ChangesResult changes = getChangeResultFromFile(TestUtils.loadFixture("fixture/change_feed_0.json"));
        Assert.assertNotNull(changes);
        Assert.assertEquals(3, changes.size());
        Assert.assertEquals(expectedLastSeq, changes.getLastSeq());
    }

    @Test
    public void test_load_data_from_file() throws Exception {
        ChangesResult changes = getChangeResultFromFile(TestUtils.loadFixture("fixture/change_feed_1.json"));
        Assert.assertNotNull(changes);
        Assert.assertEquals(2, changes.size());
    }

    @Test
    public void openRevisions_twoRows() throws Exception {
        ChangesResult data = getChangeResultFromFile(TestUtils.loadFixture("fixture/change_feed_0.json"));
        ChangesResultWrapper changes = new ChangesResultWrapper(data);

        Map<String, List<String>> openRevisionsList1 = changes.openRevisions(0, 1);
        Assert.assertEquals(1, openRevisionsList1.size());

        Map<String, List<String>> openRevisionsList2 = changes.openRevisions(0, 2);
        Assert.assertEquals(2, openRevisionsList2.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void openRevisions_invalidIndexMinusOne_exception() throws Exception {
        ChangesResult data = getChangeResultFromFile(TestUtils.loadFixture("fixture/change_feed_0.json"));
        ChangesResultWrapper changes = new ChangesResultWrapper(data);
        changes.openRevisions(-1, 2);
    }

    private ChangesResult getChangeResultFromFile(File file) throws IOException {
        byte[] data = FileUtils.readFileToByteArray(file);
        return JSONUtils.deserialize(data, ChangesResult.class);
    }

    @Test
    public void size() throws IOException {
        ChangesResult data = getChangeResultFromFile(TestUtils.loadFixture("fixture/change_feed_1.json"));
        ChangesResultWrapper changes = new ChangesResultWrapper(data);
        Assert.assertEquals(2, changes.size());
    }

    @Test
    public void tenK_changesRow() throws IOException {
        ChangesResult data = getChangeResultFromFile(TestUtils.loadFixture("fixture/10K_changes_feeds.json"));
        ChangesResultWrapper changes = new ChangesResultWrapper(data);
        Assert.assertEquals(10000, changes.size());
    }
}
