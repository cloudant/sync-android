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

import com.cloudant.sync.datastore.*;
import com.cloudant.sync.replication.Foo;
import com.cloudant.sync.sqlite.SQLDatabase;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TypedDatastoreTest {

    public static final String ORIGINAL_FOO_STRING = "haha";
    public static final String UPDATED_FOO_STRING = "hehe";

    String database_dir;
    String database_file = "touchdb_impl_test";

    SQLDatabase sqldb = null;
    DatastoreExtended core = null;
    private TypedDatastore<Foo> fooTypedDatastore;

    @Before
    public void setUp() throws Exception {
        this.database_dir = TestUtils.createTempTestingDir(TypedDatastoreTest.class.getName());

        DatastoreManager manager = new DatastoreManager(this.database_dir);
        this.core = (DatastoreExtended) manager.openDatastore(this.database_file);

        //this.sqldb = DatastoreTestUtils.createDatabase(database_dir, database_file);
        //this.core = DatastoreFactory.create(sqldb);

        this.fooTypedDatastore = new TypedDatastore<Foo>(
                Foo.class,
                this.core
        );
    }

    @After
    public void tearDown() throws Exception {
        TestUtils.deleteDatabaseQuietly(this.core.getSQLDatabase());
        TestUtils.deleteTempTestingDir(database_dir);
    }

    @Test
    public void create_foo_shouldBeCreated() {
        Foo foo = new Foo();
        Foo savedFoo = this.fooTypedDatastore.createDocument(foo);
        Assert.assertNotNull(savedFoo.getId());
        Assert.assertNotNull(savedFoo.getRevision());
        assertFooEquals(savedFoo, foo);
    }

    @Test
    public void save_newFoo_shouldBeSaved() throws ConflictException {
        Foo foo = new Foo();
        Foo savedFoo = this.fooTypedDatastore.saveDocument(foo);
        Assert.assertNotNull(savedFoo.getId());
        Assert.assertNotNull(savedFoo.getRevision());
        assertFooEquals(savedFoo, foo);
    }

    @Test
    public void update_na_shouldBeUpdated() throws ConflictException {
        Foo savedFoo = createFoo();

        Foo updatedFoo = updateFoo(savedFoo);
        Assert.assertEquals(UPDATED_FOO_STRING, updatedFoo.getFoo());
    }

    public void assertFooEquals(Foo exp, Foo act) {
        Assert.assertEquals(exp.getFoo(), act.getFoo());
    }

    @Test(expected = IllegalArgumentException.class)
    public void update_noDocumentId_exception() throws ConflictException {
        Foo foo = new Foo();
        this.fooTypedDatastore.updateDocument(foo);
    }

    @Test(expected = ConflictException.class)
    public void update_notCurrentRevision_exception() throws ConflictException {
        Foo savedFoo = createFoo();
        Foo updatedFoo = updateFoo(savedFoo);
        Assert.assertEquals(UPDATED_FOO_STRING, updatedFoo.getFoo());

        this.fooTypedDatastore.updateDocument(savedFoo);
    }

    @Test
    public void delete_document_documentShouldBeMarkedAsDeleted() throws ConflictException {
        Foo savedFoo = createFoo();

        this.fooTypedDatastore.deleteDocument(savedFoo);

        BasicDocumentRevision deletedObj = core.getDocument(savedFoo.getId());
        Assert.assertTrue(deletedObj.isDeleted());
    }

    @Test
    public void delete_documentId_documentShouldBeMarkedAsDeleted() throws ConflictException {
        Foo savedFoo = createFoo();

        this.fooTypedDatastore.deleteDocument(
                savedFoo.getId(),
                savedFoo.getRevision()
        );

        BasicDocumentRevision deletedObj = core.getDocument(savedFoo.getId());
        Assert.assertTrue(deletedObj.isDeleted());
    }

    @Test
    public void get_documentId_documentReturned() {
        Foo savedFoo = createFoo();

        Foo getFoo = this.fooTypedDatastore.getDocument(savedFoo.getId());
        Assert.assertNotNull(getFoo);
    }

    @Test
    public void get_documentIdAndRev_documentReturned() throws ConflictException {
        Foo foo = createFoo();
        Foo updatedFoo = updateFoo(foo);
        Assert.assertEquals(UPDATED_FOO_STRING, updatedFoo.getFoo());

        Foo oldVersionFoo = this.fooTypedDatastore.getDocument(
                foo.getId(),
                foo.getRevision()
        );
        Assert.assertEquals(ORIGINAL_FOO_STRING, oldVersionFoo.getFoo());
    }


    @Test(expected = DocumentNotFoundException.class)
    public void get_badDocumentId_exception() throws ConflictException {
        this.fooTypedDatastore.getDocument("-1");
    }

    @Test(expected = DocumentDeletedException.class)
    public void get_deletedDoc_exception() throws ConflictException {
        Foo foo = createFoo();
        Foo updatedFoo = updateFoo(foo);
        Assert.assertEquals("hehe", updatedFoo.getFoo());

        this.fooTypedDatastore.deleteDocument(updatedFoo);

        this.fooTypedDatastore.getDocument(updatedFoo.getId());
    }

    @Test
    public void contains_document_true() {
        Foo foo = createFoo();
        Assert.assertTrue(this.fooTypedDatastore.containsDocument(foo.getId()));
    }

    @Test
    public void contains_badDocumentId_false() {
        Assert.assertFalse(this.fooTypedDatastore.containsDocument("-1"));
    }


    private Foo createFoo() {
        Foo foo = new Foo();
        foo.setFoo(ORIGINAL_FOO_STRING);

        Foo savedFoo = this.fooTypedDatastore.createDocument(foo);

        Assert.assertNotNull(savedFoo.getId());
        Assert.assertNotNull(savedFoo.getRevision());
        return savedFoo;
    }

    private Foo updateFoo(Foo foo) throws ConflictException {
        foo.setFoo(UPDATED_FOO_STRING);
        return this.fooTypedDatastore.updateDocument(foo);
    }
}
