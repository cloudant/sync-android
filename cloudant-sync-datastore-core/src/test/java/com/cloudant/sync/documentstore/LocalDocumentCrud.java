/*
 * Copyright © 2018 IBM Corp. All rights reserved.
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

package com.cloudant.sync.documentstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.cloudant.common.DocumentStoreTestBase;
import com.cloudant.sync.event.Subscribe;
import com.cloudant.sync.event.notifications.DocumentCreated;
import com.cloudant.sync.event.notifications.DocumentDeleted;
import com.cloudant.sync.event.notifications.DocumentModified;
import com.cloudant.sync.event.notifications.DocumentUpdated;
import com.cloudant.sync.internal.common.CouchConstants;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * CRUD tests for local (non-replicating) documents using the public API
 */

public class LocalDocumentCrud extends DocumentStoreTestBase {

    private static final String id = "foo";
    private static final String idPrefixed = CouchConstants._local_prefix + id;

    private Deque<DocumentModified> events;

    @Before
    public void setup() {
        events = new ArrayDeque<DocumentModified>();
        documentStore.database().getEventBus().register(this);
    }

    /**
     * Create a local rev and read it back
     * @throws Exception
     */
    @Test
    public void testCreateAndRead() throws Exception {
        DocumentRevision rev = new DocumentRevision(idPrefixed);
        rev.setBody(DocumentBodyFactory.create("{\"hello\":\"world\"}".getBytes()));
        this.documentStore.database().create(rev);

        // read the document back, it should have the id including the _local/ prefix
        DocumentRevision rev2 = this.documentStore.database().read(idPrefixed);
        assertEquals(idPrefixed, rev2.id);

        // check the body
        assertEquals("world", rev2.getBody().asMap().get("hello"));

        // check events
        DocumentModified dc = events.remove();
        assertEquals(idPrefixed, dc.newDocument.id);
        assertEquals("world", dc.newDocument.body.asMap().get("hello"));
    }

    /**
     * Create a local rev twice with the same doc id.
     * For local revs this is just an overwrite since "create" is really an upsert.
     * @throws Exception
     */
    @Test
    public void testCreateTwiceAndRead() throws Exception {
        // create first rev
        DocumentRevision rev = new DocumentRevision(idPrefixed);
        rev.setBody(DocumentBodyFactory.create("{\"hello\":\"world\"}".getBytes()));
        this.documentStore.database().create(rev);

        // create second rev
        DocumentRevision rev2 = new DocumentRevision(idPrefixed);
        rev2.setBody(DocumentBodyFactory.create("{\"hello\":\"universe\"}".getBytes()));
        this.documentStore.database().create(rev2);

        // read the document back, it should have the id including the _local/ prefix
        DocumentRevision rev3 = this.documentStore.database().read(idPrefixed);
        assertEquals(idPrefixed, rev3.id);

        // check the body
        assertEquals("universe", rev3.getBody().asMap().get("hello"));

        // check events
        DocumentModified dc = events.remove();
        assertEquals(idPrefixed, dc.newDocument.id);
        assertEquals("world", dc.newDocument.body.asMap().get("hello"));
        dc = events.remove();
        assertEquals(idPrefixed, dc.newDocument.id);
        assertEquals("universe", dc.newDocument.body.asMap().get("hello"));
    }

    /**
     * Create a local rev and check that contains() returns true for it
     * @throws Exception
     */
    @Test
    public void testCreateAndContains() throws Exception {
        DocumentRevision rev = new DocumentRevision(idPrefixed);
        rev.setBody(DocumentBodyFactory.create("{\"hello\":\"world\"}".getBytes()));
        this.documentStore.database().create(rev);

        // check that contains() returns true for the local doc
        boolean contains = this.documentStore.database().contains(idPrefixed);
        assertTrue(contains);
    }

    /**
     * Create a local rev and assert that an exception is thrown if we try to set the rev id
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateThrowsForNonNullRevisionId() throws Exception {
        // try to create a local document with a non-null revision id
        DocumentRevision rev = new DocumentRevision(idPrefixed, "1-a");
        rev.setBody(DocumentBodyFactory.create("{\"hello\":\"world\"}".getBytes()));
        this.documentStore.database().create(rev);
    }

    /**
     * Create then delete local rev and check that calling read() on it raises an exception
     * @throws Exception
     */
    @Test
    public void testDeleteAndRead() throws Exception {
        DocumentRevision rev = new DocumentRevision(idPrefixed);
        rev.setBody(DocumentBodyFactory.create("{\"hello\":\"world\"}".getBytes()));
        this.documentStore.database().create(rev);
        DocumentRevision deleted = this.documentStore.database().delete(rev);

        // delete returns null for local documents since local documents don't have tombstones
        assertNull(deleted);
        boolean caught = false;
        try {
            this.documentStore.database().read(idPrefixed);
        } catch (DocumentNotFoundException dnfe) {
            caught = true;
        }
        assertTrue(caught);

        // check events
        DocumentModified dc = events.remove();
        assertEquals(idPrefixed, dc.newDocument.id);
        assertEquals("world", dc.newDocument.body.asMap().get("hello"));
        // after deletion, "new" document is null
        dc = events.remove();
        assertEquals(idPrefixed, dc.prevDocument.id);
        assertNull(dc.newDocument);
    }

    /**
     * Create a local rev and attempt to update it.
     * This will fail because the correct usage is to call create() which is really an upsert.
     *
     * @throws Exception
     */
    @Test
    public void testUpdateThrows() throws Exception {
        // create first rev
        DocumentRevision rev = new DocumentRevision(idPrefixed);
        rev.setBody(DocumentBodyFactory.create("{\"hello\":\"world\"}".getBytes()));
        this.documentStore.database().create(rev);
        // create second rev
        DocumentRevision rev2 = new DocumentRevision(idPrefixed);
        rev2.setBody(DocumentBodyFactory.create("{\"hello\":\"universe\"}".getBytes()));
        boolean caught = false;
        try {
            this.documentStore.database().update(rev2);
        } catch (IllegalArgumentException iae) {
            caught = true;
        }
        assertTrue(caught);
        // read the document back, it should have the id including the _local/ prefix
        DocumentRevision rev3 = this.documentStore.database().read(idPrefixed);
        assertEquals(idPrefixed, rev3.id);
        // check the body, it should still be the old body because the update failed
        assertEquals("world", rev3.getBody().asMap().get("hello"));
    }

    /**
     * Create then delete local rev and check that contains() returns false for it
     * @throws Exception
     */
    @Test
    public void testDeleteAndContains() throws Exception {
        DocumentRevision rev = new DocumentRevision(idPrefixed);
        rev.setBody(DocumentBodyFactory.create("{\"hello\":\"world\"}".getBytes()));
        this.documentStore.database().create(rev);
        DocumentRevision deleted = this.documentStore.database().delete(rev);
        // delete returns null for local documents since local documents don't have tombstones
        assertNull(deleted);
        // check that contains() returns false for the local doc
        boolean contains = this.documentStore.database().contains(idPrefixed);
        assertFalse(contains);
    }

    /**
     * Create a local rev and assert that an exception is thrown if we try to delete it with rev id
     * set to a non-null value
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testDeleteThrowsForNonNullRevisionId() throws Exception {
        DocumentRevision rev = new DocumentRevision(idPrefixed);
        rev.setBody(DocumentBodyFactory.create("{\"hello\":\"world\"}".getBytes()));
        this.documentStore.database().create(rev);
        DocumentRevision toDelete = new DocumentRevision(idPrefixed, "1-a");
        this.documentStore.database().delete(toDelete);
    }

    /**
     * Create a local rev and assert that an exception is thrown if we try to delete it using the
     * String argument type instead of the DocumentRevision argument type
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testDeleteThrowsForIncorrectArgumentType() throws Exception {
        DocumentRevision rev = new DocumentRevision(idPrefixed);
        this.documentStore.database().delete(idPrefixed);
    }

    @Subscribe
    public void onDocumentCreated(DocumentCreated dc) throws Exception {
        events.add(dc);
    }

    @Subscribe
    public void onDocumentUpdated(DocumentUpdated du) {
        events.add(du);
    }

    @Subscribe
    public void onDocumentDeleted(DocumentDeleted dd) {
        events.add(dd);
    }


}
