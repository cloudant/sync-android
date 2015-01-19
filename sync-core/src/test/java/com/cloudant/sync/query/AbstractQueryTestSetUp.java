//  Copyright (c) 2014 Cloudant. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.cloudant.sync.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.MutableDocumentRevision;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.TestUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 *  The purpose of this abstract class is to provide setup methods that include
 *  the loading of test data and the creation of indexes for use by the
 *  {@link com.cloudant.sync.query.AbstractQueryTestBase} class and the
 *  {@link com.cloudant.sync.query.AbstractQueryExtendedTestBase} class test methods.
 *
 *  @see com.cloudant.sync.query.AbstractQueryTestBase
 *  @see com.cloudant.sync.query.AbstractQueryExtendedTestBase
 */
public abstract class AbstractQueryTestSetUp {

    String factoryPath = null;
    DatastoreManager factory = null;
    DatastoreExtended ds = null;
    IndexManager im = null;
    SQLDatabase db = null;

    @Before
    public void setUp() throws SQLException {
        factoryPath = TestUtils.createTempTestingDir(AbstractQueryTestSetUp.class.getName());
        assertThat(factoryPath, is(notNullValue()));
        factory = new DatastoreManager(factoryPath);
        assertThat(factory, is(notNullValue()));
        String datastoreName = AbstractQueryTestSetUp.class.getSimpleName();
        ds = (DatastoreExtended) factory.openDatastore(datastoreName);
        assertThat(ds, is(notNullValue()));
    }

    @After
    public void tearDown() {
        im.close();
        assertThat(im.getQueue().isShutdown(), is(true));
        ds.close();
        TestUtils.deleteDatabaseQuietly(db);
        TestUtils.deleteTempTestingDir(factoryPath);

        im = null;
        ds = null;
        factory = null;
        factoryPath = null;
    }

    // Used to setup document data testing:
    // - When executing AND queries
    // - When limiting and offsetting results
    // - When querying using _id
    // - When querying using _rev
    // - When querying using $not operator
    // - When querying using $exists operator
    public void setUpBasicQueryData() {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "mike12";
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "mike34";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "dog");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "mike72";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 72);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "fred34";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "fred12";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        assertThat(im.ensureIndexed(Arrays.<Object>asList("name", "age"), "basic"), is("basic"));
        assertThat(im.ensureIndexed(Arrays.<Object>asList("name", "pet"), "pet"), is("pet"));
    }

    // Used to setup document data testing:
    // - When using dotted notation
    public void setUpDottedQueryData() {
        setUpSharedDocs();

        assertThat(im.ensureIndexed(Arrays.<Object>asList("age", "pet.name", "pet.species"), "pet"),
                is("pet"));
        assertThat(im.ensureIndexed(Arrays.<Object>asList("name", "pet.name.first"), "firstname"),
                is("firstname"));
    }

    // Used to setup document data testing:
    // - When using non-ascii text
    public void setUpNonAsciiQueryData() {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "mike12";
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "mike34";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "dog");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "mike72";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 72);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "اسم34";
        bodyMap.clear();
        bodyMap.put("name", "اسم");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "fred12";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "fredarabic";
        bodyMap.clear();
        bodyMap.put("اسم", "fred");
        bodyMap.put("age", 12);
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "freddatatype";
        bodyMap.clear();
        bodyMap.put("datatype", "fred");
        bodyMap.put("age", 12);
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }
    }

    // Used to setup document data testing:
    // - When using OR queries
    public void setUpOrQueryData() {
        setUpSharedDocs();

        assertThat(im.ensureIndexed(Arrays.<Object>asList("age", "pet", "name"), "basic"),
                is("basic"));
        assertThat(im.ensureIndexed(Arrays.<Object>asList("age", "pet.name", "pet.species"), "pet"),
                is("pet"));
        assertThat(im.ensureIndexed(Arrays.<Object>asList("age", "pet.name.first"), "firstname"),
                is("firstname"));
    }

    // Used to setup document data testing:
    // - When using nested queries
    public void setUpNestedQueryData() {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "mike12";
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }
        rev.docId = "mike23";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 23);
        bodyMap.put("pet", "parrot");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "mike34";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "dog");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }
        rev.docId = "john34";
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "fish");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "fred43";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 43);
        bodyMap.put("pet", "snake");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "fred12";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        assertThat(im.ensureIndexed(Arrays.<Object>asList("age", "pet", "name"), "basic"),
                is("basic"));
    }

    // Used to setup document data testing:
    // - When indexing array fields
    public void setUpArrayIndexingData() {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "mike12";
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", Arrays.<Object>asList("cat", "dog"));
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }
        rev.docId = "fred34";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "parrot");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        assertThat(im.ensureIndexed(Arrays.<Object>asList("name", "pet", "age"), "pet"),
                is("pet"));
    }

    // Used to setup document data testing:
    // - When there is a large result set
    public void setUpLargeResultSetQueryData() {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        for (int i = 0; i < 150; i++) {
            rev.docId = String.format("d%d", i);
            Map<String, Object> bodyMap = new HashMap<String, Object>();
            bodyMap.put("large_field", "cat");
            bodyMap.put("idx", i);
            rev.body = DocumentBodyFactory.create(bodyMap);
            try {
                ds.createDocumentFromRevision(rev);
            } catch (IOException e) {
                e.printStackTrace();
                Assert.fail("Failed to create document revision");
            }
        }
        assertThat(im.ensureIndexed(Arrays.<Object>asList("large_field", "idx"), "large"),
                is("large"));
    }

    // Used to setup document data testing for queries without covering indexes:
    // - When executing queries
    public void setUpWithoutCoveringIndexesQueryData() {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "mike12";
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "mike34";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "dog");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "mike72";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 72);
        bodyMap.put("pet", "cat");
        bodyMap.put("town", "bristol");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "fred34";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "fred12";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        bodyMap.put("town", "bristol");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        assertThat(im.ensureIndexed(Arrays.<Object>asList("name", "age"), "basic"), is("basic"));
        assertThat(im.ensureIndexed(Arrays.<Object>asList("name", "pet"), "pet"), is("pet"));
    }

    private void setUpSharedDocs() {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "mike12";
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        Map<String, Object> petMap = new HashMap<String, Object>();
        petMap.put("species", "cat");
        petMap.put("name", "mike");
        bodyMap.put("pet", petMap);
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "mike23";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 23);
        petMap.clear();
        petMap.put("species", "cat");
        Map<String, Object> petNameMap = new HashMap<String, Object>();
        petNameMap.put("first", "mike");
        petMap.put("name", petNameMap);
        bodyMap.put("pet", petMap);
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "mike34";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        petMap.clear();
        petMap.put("species", "cat");
        petMap.put("name", "mike");
        bodyMap.put("pet", petMap);
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "mike72";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 72);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "fred34";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }

        rev.docId = "fred12";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        rev.body = DocumentBodyFactory.create(bodyMap);
        try {
            ds.createDocumentFromRevision(rev);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Failed to create document revision");
        }
    }

}