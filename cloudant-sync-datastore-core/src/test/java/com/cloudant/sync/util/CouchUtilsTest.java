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

package com.cloudant.sync.util;

import com.cloudant.sync.internal.common.CouchUtils;

import org.junit.Assert;
import org.junit.Test;

public class CouchUtilsTest {

    @Test
    public void getFirstLocalDocRevisionId() {
        String fistLocalRevisionId = CouchUtils.getFirstLocalDocRevisionId();
        Assert.assertEquals("1-local", fistLocalRevisionId);
    }

    @Test
    public void getFirstRevisionId() {
        String revisionId = CouchUtils.getFirstRevisionId();
        Assert.assertTrue(revisionId.startsWith("1-"));
        CouchUtils.validateRevisionId(revisionId);
    }

    @Test
    public void generateNextRevisionID() {
        String revisionId = "10-ladlanaj09njn";
        String newRevisionId = CouchUtils.generateNextRevisionId(revisionId);
        Assert.assertNotSame("11-ladlanaj09njn", newRevisionId);
        Assert.assertTrue(newRevisionId.startsWith("11-"));
    }

    @Test
    public void generationFromRevID() {
        String revisionId = "100001-ladlanaj09njn";
        Assert.assertEquals(100001, CouchUtils.generationFromRevId(revisionId));
    }

    @Test
    public void validateRevisionId_success() {
        String revisionId = "10-ladlanaj09njn";
        CouchUtils.validateRevisionId(revisionId);
    }

    @Test
    public void validateRevisionId_stringWithTwoDash_success() {
        String revisionId = "10-ladlanaj09n-jn";
        CouchUtils.validateRevisionId(revisionId);
    }

    @Test(expected = IllegalArgumentException.class)
    public void validateRevisionId_exception() {
        String revisionId = "-ladlanaj09njn";
        CouchUtils.validateRevisionId(revisionId);
    }

    @Test(expected = IllegalArgumentException.class)
    public void validateRevisionId_wrong_generation() {
        String revisionId = "ahah-ladlanaj09njn";
        CouchUtils.validateRevisionId(revisionId);
    }

}

