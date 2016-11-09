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

package com.cloudant.sync.internal.replication;

import com.cloudant.sync.internal.mazha.CouchClient;
import com.cloudant.sync.internal.mazha.Response;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * <code>CouchClientWrapper</code> is a wrapper <code>CouchClient</code>, and a couple of its method just simple
 * delegate to <code>CouchClient</code>. This test suite is to ensure those methods are simply passing parameter to
 *  a <code>CouchClient</code> mockup.
 */
public class CouchClientWrapperMockTest {

    public static final String DB_NAME = "defaultDb";
    CouchClient mockedClient;
    CouchClientWrapper wrapper;

    @Before
    public void setup() throws IOException {
        mockedClient = mock(CouchClient.class);
        wrapper = new CouchClientWrapper(mockedClient);
    }

    @Test
    public void create_obj() {
        Object s = new HashMap();
        wrapper.create(s);

        verify(mockedClient).create(s);
    }

    @Test
    public void update_idAndObj() {
        Object s = new HashMap();
        wrapper.update("id", s);

        verify(mockedClient).update("id", s);
    }

    @Test
    public void delete_idAndRev() {
        Object s = new HashMap();
        wrapper.delete("id", "rev");

        verify(mockedClient).delete("id", "rev");
    }

    @Test
    public void get_classAndId() {
        Object s = new HashMap();
        wrapper.get(Bar.class, "id");

        verify(mockedClient).getDocument("id", Bar.class);
    }

    @Test
    public void createDb_dbName() {
        wrapper.createDatabase();

        verify(mockedClient).createDb();
    }

    @Test
    public void deleteDb_dbName() {
        wrapper.deleteDatabase();

        verify(mockedClient).deleteDb();
    }

    @Test
    public void bulkSerializedDocs() {
        List<String> docs = new ArrayList<String>();
        docs.add("{}");
        wrapper.bulkCreateSerializedDocs(docs);

        verify(mockedClient).bulkCreateSerializedDocs(docs);
    }

    @Test(expected = RuntimeException.class)
    public void bulkSerializedDocs_unknownServerError() {
        // Prepare
        List<String> docs = new ArrayList<String>();
        docs.add("{}");

        when(mockedClient.bulkCreateSerializedDocs(anyList())).thenReturn(Arrays.asList(new Response()));

        // exec
        wrapper.bulkCreateSerializedDocs(docs);
    }
}
