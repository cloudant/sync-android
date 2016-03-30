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

package com.cloudant.sync.datastore;

import com.cloudant.sync.util.JSONUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class BasicDBObjectTest {

    private static final String DOCUMENT_ID = "hahaha";
    private static final String REVISION_ID = "1-testrevision";
    private static final String LOCAL_REVISION_ID = "2-local";
    private static final byte[] JSON_BODY = "{\"a\":\"test data\"}".getBytes();
    private static final long SEQUENCE = 123456789L;
    private static final long INTERNAL_ID = 987654321L;

    DocumentRevisionBuilder builder = null;

    @Before
    public void setUp() {
        this.builder = new DocumentRevisionBuilder();
    }

    @Test
    public void constructor_fullObject_objectShouldBeCorrectlyCreated() {
        this.builder.setDocId(DOCUMENT_ID);
        this.builder.setRevId(REVISION_ID);
        this.builder.setBody(new BasicDocumentBody(JSON_BODY));
        this.builder.setSequence(SEQUENCE);
        this.builder.setInternalId(INTERNAL_ID);
        this.builder.setDeleted(Boolean.TRUE);

        DocumentRevision td = this.builder.build();
        Assert.assertEquals(DOCUMENT_ID, td.getId());
        Assert.assertEquals(REVISION_ID, td.getRevision());
        Assert.assertTrue(td.isDeleted());
        Assert.assertFalse(td.isCurrent());
        Assert.assertEquals("test data", (String) td.getBody().asMap().get("a"));
    }

    @Test
    public void constructor_stubObject_stubObjectShouldBeCreated() {
        this.builder.setDocId(DOCUMENT_ID);
        this.builder.setRevId(REVISION_ID);

        DocumentRevision td = this.builder.buildStub();
        Assert.assertEquals(DOCUMENT_ID, td.getId());
        Assert.assertEquals(REVISION_ID, td.getRevision());
        Arrays.equals(JSONUtils.emptyJSONObjectAsBytes(), td.getBody().asBytes());
    }
}
