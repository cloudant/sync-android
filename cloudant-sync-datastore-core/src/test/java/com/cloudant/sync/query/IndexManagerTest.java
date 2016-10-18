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

import static com.cloudant.sync.query.IndexMatcherHelpers.getIndexNameMatcher;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentRevision;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class IndexManagerTest extends AbstractIndexTestBase {

    @Test
    public void enusureIndexedGeneratesIndexName() throws QueryException {
        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"))), is(notNullValue()));
    }

    @Test
    public void deleteFailOnNoIndexName() throws QueryException {
        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("address")), "basic");
        assertThat(im.listIndexes(), contains(getIndexNameMatcher("basic")));

        try {
            im.deleteIndex(null);
            Assert.fail("Expected deleteIndex to throw a QueryException");
        } catch (QueryException qe) {
            ;
        }
        assertThat(im.listIndexes(), contains(getIndexNameMatcher("basic")));

        try {
            im.deleteIndex("");
            Assert.fail("Expected deleteIndex to throw a QueryException");
        } catch (QueryException qe) {
            ;
        }

        assertThat(im.listIndexes(), contains(getIndexNameMatcher("basic")));
    }

    @Test
    public void deleteFailOnInvalidIndexName() throws QueryException {
        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("address")), "basic");
        assertThat(im.listIndexes(), contains(getIndexNameMatcher("basic")));

        try {
            im.deleteIndex("invalid");
            Assert.fail("Expected deleteIndex to throw a QueryException");
        } catch (QueryException qe) {
            ;
        }
        assertThat(im.listIndexes(), contains(getIndexNameMatcher("basic")));
    }

    @Test
    public void createIndexWithSpaceInName() throws QueryException {
        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("address")), "basic index");
        assertThat(im.listIndexes(), contains(getIndexNameMatcher("basic index")));
    }

    @Test
         public void createIndexWithSingleQuoteInName() throws QueryException {
        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("address")), "basic'index");
        assertThat(im.listIndexes(), contains(getIndexNameMatcher("basic'index")));
    }

    @Test
    public void createIndexWithSemiColonQuoteInName() throws QueryException {
        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("address")), "basic;index");
        assertThat(im.listIndexes(), contains(getIndexNameMatcher("basic;index")));
    }

    @Test
    public void createIndexWithBracketsInName() throws QueryException {
        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("address")), "basic(index)");
        assertThat(im.listIndexes(), contains(getIndexNameMatcher("basic(index)")));
    }

    @Test
    public void createIndexWithKeyWordName() throws QueryException {
        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("address")), "INSERT INDEX");
        assertThat(im.listIndexes(), contains(getIndexNameMatcher("INSERT INDEX")));
    }



    @Test
     public void deleteEmptyIndex() throws QueryException {
        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("address")), "basic");
        assertThat(im.listIndexes(), contains(getIndexNameMatcher("basic")));

        im.deleteIndex("basic");
        assertThat(im.listIndexes().isEmpty(), is(true));
    }

    @Test
    public void deleteTheCorrectEmptyIndex() throws QueryException {
        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("address")), "basic");
        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "basic2");
        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name")), "basic3");
        assertThat(im.listIndexes().size(), is(3));
        assertThat(im.listIndexes(), containsInAnyOrder(getIndexNameMatcher("basic"), getIndexNameMatcher("basic2"), getIndexNameMatcher("basic3")));

        im.deleteIndex("basic2");
        assertThat(im.listIndexes().size(), is(2));
        assertThat(im.listIndexes(), containsInAnyOrder(getIndexNameMatcher("basic"), getIndexNameMatcher("basic3")));
    }

    @Test
    public void deleteNonEmptyIndex() throws Exception {
        for (int i = 0; i < 4; i++) {
            DocumentRevision rev = new DocumentRevision();
            Map<String, Object> bodyMap = new HashMap<String, Object>();
            bodyMap.put("name", "mike");
            bodyMap.put("age", 12);
            Map<String, Object> petMap = new HashMap<String, Object>();
            petMap.put("species", "cat");
            petMap.put("name", "mike");
            bodyMap.put("pet", petMap);
            rev.setBody(DocumentBodyFactory.create(bodyMap));
            ds.createDocumentFromRevision(rev);
        }

        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("address")), "basic");
        assertThat(im.listIndexes(), contains(getIndexNameMatcher("basic")));
        im.deleteIndex("basic");
        assertThat(im.listIndexes().isEmpty(), is(true));
    }

    @Test
    public void deleteTheCorrectNonEmptyIndex() throws Exception {
        for (int i = 0; i < 4; i++) {
            DocumentRevision rev = new DocumentRevision();
            Map<String, Object> bodyMap = new HashMap<String, Object>();
            bodyMap.put("name", "mike");
            bodyMap.put("age", 12);
            Map<String, Object> petMap = new HashMap<String, Object>();
            petMap.put("species", "cat");
            petMap.put("name", "mike");
            bodyMap.put("pet", petMap);
            rev.setBody(DocumentBodyFactory.create(bodyMap));
            ds.createDocumentFromRevision(rev);
        }

        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("address")), "basic");
        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "basic2");
        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name")), "basic3");
        assertThat(im.listIndexes().size(), is(3));
        assertThat(im.listIndexes(), containsInAnyOrder(getIndexNameMatcher("basic"), getIndexNameMatcher("basic2"), getIndexNameMatcher("basic3")));

        im.deleteIndex("basic2");
        assertThat(im.listIndexes().size(), is(2));
        assertThat(im.listIndexes(), containsInAnyOrder(getIndexNameMatcher("basic"), getIndexNameMatcher("basic3")));
    }

    @Test
    public void deleteATextIndex() throws Exception {
        for (int i = 0; i < 4; i++) {
            DocumentRevision rev = new DocumentRevision();
            Map<String, Object> bodyMap = new HashMap<String, Object>();
            bodyMap.put("name", "mike");
            bodyMap.put("age", 12);
            Map<String, Object> petMap = new HashMap<String, Object>();
            petMap.put("species", "cat");
            petMap.put("name", "mike");
            bodyMap.put("pet", petMap);
            rev.setBody(DocumentBodyFactory.create(bodyMap));
            ds.createDocumentFromRevision(rev);
        }

        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("address")), "basic", IndexType.TEXT);
        assertThat(im.listIndexes(), contains(getIndexNameMatcher("basic")));

        im.deleteIndex("basic");
        assertThat(im.listIndexes().isEmpty(), is(true));
    }

    @Test
    public void validateTextSearchIsAvailable() throws Exception {
        assertThat(im.isTextSearchEnabled(), is(true));
    }

}
