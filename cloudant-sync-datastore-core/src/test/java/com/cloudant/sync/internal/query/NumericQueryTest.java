/*
 * Copyright © 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.internal.query;

import com.cloudant.sync.documentstore.DocumentBodyFactory;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.query.FieldSort;
import com.cloudant.sync.query.QueryException;
import com.cloudant.sync.query.QueryResult;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NumericQueryTest extends AbstractQueryTestBase {

    int defaultInt = 42;
    float defaultFloat = 1.01f;
    double defaultDouble = 1.01d;


    @Before
    public void before() throws Exception {
        super.setUp();
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("integer", defaultInt);
        data.put("float", defaultFloat);
        data.put("double", defaultDouble);
        DocumentRevision documentRevision = new DocumentRevision();
        documentRevision.setBody(DocumentBodyFactory.create(data));
        ds.create(documentRevision);
        List<FieldSort> fs = new ArrayList<FieldSort>();
        fs.add(new FieldSort("integer"));
        fs.add(new FieldSort("float"));
        fs.add(new FieldSort("double"));
        im.ensureIndexed(fs);
    }

    @After
    public void after() {
        ;
    }

    // query two integers are equal to each other
    @Test
    public void testIntegerEqual() throws QueryException {
        Map<String, Object> q = new HashMap<String, Object>();
        q.put("integer", defaultInt);
        QueryResult qr = im.find(q);
        Assert.assertEquals(1, qr.size());
    }

    // can query an int with a double - value in query is truncated
    @Test
    public void testIntegerEqualDoubleInQuery() throws QueryException {
        Map<String, Object> q = new HashMap<String, Object>();
        q.put("integer", 42.0d);
        QueryResult qr = im.find(q);
        Assert.assertEquals(1, qr.size());
    }

    // throws NPE due to validateNotAFloat failing
    @Test(expected = NullPointerException.class)
    public void testFloatEqual() throws QueryException {
        Map<String, Object> q = new HashMap<String, Object>();
        q.put("float", defaultFloat);
        QueryResult qr = im.find(q);
        Assert.assertEquals(1, qr.size());
    }

    // can query a float with a double
    @Test
    public void testFloatEqualDoubleInQuery() throws QueryException {
        Map<String, Object> q = new HashMap<String, Object>();
        q.put("float", defaultDouble);
        QueryResult qr = im.find(q);
        Assert.assertEquals(1, qr.size());
    }

    // query two doubles are equal to each other
    @Test
    public void testDoubleEqual() throws QueryException {
        Map<String, Object> q = new HashMap<String, Object>();
        q.put("double", defaultDouble);
        QueryResult qr = im.find(q);
        Assert.assertEquals(1, qr.size());
    }

}