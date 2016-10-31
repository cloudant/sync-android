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

import com.cloudant.sync.datastore.DatabaseImpl;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.datastore.DocumentStore;
import com.cloudant.sync.sqlite.SQLDatabaseQueue;
import com.cloudant.sync.util.TestUtils;

import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *  The purpose of this abstract class is to provide setup methods that include
 *  the loading of test data and the creation of indexes for use by the
 *  {@link QueryCoveringIndexesTest} class and the
 *  {@link QueryWithoutCoveringIndexesTest} class test methods.
 *
 *  @see QueryCoveringIndexesTest
 *  @see QueryWithoutCoveringIndexesTest
 */
public abstract class AbstractQueryTestBase {

    String factoryPath = null;
    DatabaseImpl ds = null;
    IndexManagerImpl im = null;
    SQLDatabaseQueue indexManagerDatabaseQueue;

    @Before
    public void setUp() throws Exception {
        factoryPath = TestUtils.createTempTestingDir(AbstractQueryTestBase.class.getName());
        assertThat(factoryPath, is(notNullValue()));
        String datastoreName = AbstractQueryTestBase.class.getSimpleName();
        DocumentStore documentStore = DocumentStore.getInstance(new File(factoryPath));
        ds = (DatabaseImpl) documentStore.database;
        im = (IndexManagerImpl) documentStore.query;
        assertThat(ds, is(notNullValue()));
    }

    @After
    public void tearDown() throws Exception {
        im.close();
        assertThat(indexManagerDatabaseQueue.isShutdown(), is(true));
        ds.close();
        TestUtils.deleteTempTestingDir(factoryPath);

        im = null;
        ds = null;
        factoryPath = null;
    }

    // Used to setup document data testing:
    // - When executing AND queries
    // - When limiting and offsetting results
    // - When querying using _id
    // - When querying using _rev
    // - When querying using $not operator
    // - When querying using $exists operator
    public void setUpBasicQueryData() throws Exception {
        DocumentRevision rev = new DocumentRevision("mike12");
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);


        rev = new DocumentRevision("mike34");
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "dog");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);


        rev = new DocumentRevision("mike72");
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 72);
        bodyMap.put("pet", "cat");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);


        rev = new DocumentRevision("fred34");
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "cat");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);


        rev = new DocumentRevision("fred12");
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);


        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "basic"), is("basic"));
        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("pet")), "pet"), is("pet"));
    }

    // Used to setup document data testing:
    // - When using dotted notation
    public void setUpDottedQueryData() throws Exception {
        setUpSharedDocs();

        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("age"), new FieldSort("pet.name"), new FieldSort("pet.species")), "pet"),
                is("pet"));
        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("pet.name.first")), "firstname"),
                is("firstname"));
    }

    // Used to setup document data testing:
    // - When using non-ascii text
    public void setUpNonAsciiQueryData() throws Exception {
        DocumentRevision rev = new DocumentRevision("mike12");
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        rev.setBody(DocumentBodyFactory.create(bodyMap));

        ds.createDocumentFromRevision(rev);


        rev = new DocumentRevision("mike34");
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "dog");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);


        rev = new DocumentRevision("mike72");
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 72);
        bodyMap.put("pet", "cat");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);


        rev = new DocumentRevision("اسم34");
        bodyMap.clear();
        bodyMap.put("name", "اسم");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "cat");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fred12");
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fredarabic");
        bodyMap.clear();
        bodyMap.put("اسم", "fred");
        bodyMap.put("age", 12);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);


        rev = new DocumentRevision("freddatatype");
        bodyMap.clear();
        bodyMap.put("datatype", "fred");
        bodyMap.put("age", 12);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

    }

    // Used to setup document data testing:
    // - When using OR queries
    public void setUpOrQueryData() throws Exception {
        setUpSharedDocs();

        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("age"), new FieldSort("pet"), new FieldSort("name")), "basic"),
                is("basic"));
        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("age"), new FieldSort("pet.name"), new FieldSort("pet.species")), "pet"),
                is("pet"));
        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("age"), new FieldSort("pet.name.first")), "firstname"),
                is("firstname"));
    }

    // Used to setup document data testing:
    // - When using nested queries
    public void setUpNestedQueryData() throws Exception {
        DocumentRevision rev = new DocumentRevision("mike12");
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("mike23");
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 23);
        bodyMap.put("pet", "parrot");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("mike34");
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "dog");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("john34");
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "fish");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fred43");
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 43);
        bodyMap.put("pet", "snake");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fred12");
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("age"), new FieldSort("pet"), new FieldSort("name")), "basic"),
                is("basic"));
    }

    // Used to setup document data testing:
    // - When indexing array fields
    // - When querying using $in operator
    public void setUpArrayIndexingData() throws Exception {
        DocumentRevision rev = new DocumentRevision("mike12");
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", Arrays.<String>asList("cat", "dog"));
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fred34");
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "parrot");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("mike34");
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        bodyMap.put("pet", Arrays.<String>asList("cat", "dog", "fish"));
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fred12");
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("john44");
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("age", 44);
        bodyMap.put("pet", Arrays.<String>asList("hamster", "snake"));
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("john22");
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("age", 22);
        bodyMap.put("pet", "cat");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("pet"), new FieldSort("age")), "pet"),
                is("pet"));
    }

    // Used to setup document data testing for queries with mathematical operations.
    // - When querying using $mod operator
    public void setUpNumericOperationsQueryData() throws Exception {
        DocumentRevision rev = new DocumentRevision("mike31");
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("score", 31);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fred11");
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("score", 11);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("john15");
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("score", 15);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("john-15");
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("score", -15);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("john15.2");
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("score", 15.2);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("john15.6");
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("score", 15.6);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("john0");
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("score", 0);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("john0.0");
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("score", 0.0);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("john0.6");
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("score", 0.6);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("john-0.6");
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("score", -0.6);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("score")), "name_score"),
                                    is("name_score"));
    }

    // Used to setup document data testing:
    // - When there is a large result set
    public void setUpLargeResultSetQueryData() throws Exception {
       for (int i = 0; i < 150; i++) {
            DocumentRevision rev = new DocumentRevision(String.format("d%d", i));
            Map<String, Object> bodyMap = new HashMap<String, Object>();
            bodyMap.put("large_field", "cat");
            bodyMap.put("idx", i);
            rev.setBody(DocumentBodyFactory.create(bodyMap));
            ds.createDocumentFromRevision(rev);
        }
        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("large_field"), new FieldSort("idx")), "large"),
                is("large"));
    }

    // Used to setup document data testing for queries without covering indexes:
    // - When executing AND queries
    // - When executing OR queries
    public void setUpWithoutCoveringIndexesQueryData() throws Exception {
        DocumentRevision rev = new DocumentRevision("mike12");
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("mike34");
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "dog");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("mike72");
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 72);
        bodyMap.put("pet", "cat");
        bodyMap.put("town", "bristol");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fred34");
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "cat");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fred12");
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        bodyMap.put("town", "bristol");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "basic"), is("basic"));
        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("pet")), "pet"), is("pet"));
    }

    // Used to setup document data testing for queries containing a $size operator:
    // - When executing queries containing $size operator
    public void setUpSizeOperatorQueryData() throws Exception {
        DocumentRevision rev = new DocumentRevision("mike24");
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 24);
        bodyMap.put("pet", Collections.singletonList("cat"));
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("mike12");
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", Arrays.asList("cat", "dog"));
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fred34");
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", Arrays.asList("cat", "dog"));
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("john44");
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("age", 44);
        bodyMap.put("pet", "cat");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fred72");
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 72);
        bodyMap.put("pet", Collections.singletonList("dog"));
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("john12");
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("age", 12);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("bill34");
        bodyMap.clear();
        bodyMap.put("name", "bill");
        bodyMap.put("age", 34);
        bodyMap.put("pet", Arrays.asList("cat", "parrot"));
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fred11");
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 11);
        bodyMap.put("pet", new ArrayList<Object>());
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("pet"), new FieldSort("age")), "basic"), is("basic"));
    }

    // Used to setup document data testing for sorting:
    // - When sorting
    public void setUpSortingQueryData() throws Exception {
        DocumentRevision rev = new DocumentRevision("mike12");
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("age", Arrays.<FieldSort>asList(new FieldSort("cat"), new FieldSort("dog")));
        bodyMap.put("same", "all");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fred34");
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "parrot");
        bodyMap.put("same", "all");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fred11");
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 11);
        bodyMap.put("pet", "fish");
        bodyMap.put("same", "all");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("pet"), new FieldSort("age"), new FieldSort("same")), "pet"),
                is("pet"));
    }

    private void setUpSharedDocs() throws Exception {
        DocumentRevision rev = new DocumentRevision("mike12");
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        Map<String, Object> petMap = new HashMap<String, Object>();
        petMap.put("species", "cat");
        petMap.put("name", "mike");
        bodyMap.put("pet", petMap);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("mike23");
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 23);
        petMap.clear();
        petMap.put("species", "cat");
        Map<String, Object> petNameMap = new HashMap<String, Object>();
        petNameMap.put("first", "mike");
        petMap.put("name", petNameMap);
        bodyMap.put("pet", petMap);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("mike34");
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        petMap.clear();
        petMap.put("species", "cat");
        petMap.put("name", "mike");
        bodyMap.put("pet", petMap);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("mike72");
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 72);
        bodyMap.put("pet", "cat");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fred34");
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "cat");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fred12");
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);
    }

}
