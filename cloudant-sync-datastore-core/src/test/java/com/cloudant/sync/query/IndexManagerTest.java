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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentRevision;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class IndexManagerTest extends AbstractIndexTestBase {

    @Test
    public void enusureIndexedGeneratesIndexName() throws Exception {
        assertThat(fd.ensureIndexed(Arrays.<Object>asList("name")), is(notNullValue()));
    }

    @Test
    public void deleteFailOnNoIndexName() throws Exception {
        fd.ensureIndexed(Arrays.<Object>asList("name", "address"), "basic");
        assertThat(fd.listIndexes().keySet(), contains("basic"));

        assertThat(fd.deleteIndexNamed(null), is(false));
        assertThat(fd.listIndexes().keySet(), contains("basic"));

        assertThat(fd.deleteIndexNamed(""), is(false));
        assertThat(fd.listIndexes().keySet(), contains("basic"));
    }

    @Test
    public void deleteFailOnInvalidIndexName() throws Exception {
        fd.ensureIndexed(Arrays.<Object>asList("name", "address"), "basic");
        assertThat(fd.listIndexes().keySet(), contains("basic"));

        assertThat(fd.deleteIndexNamed("invalid"), is(false));
        assertThat(fd.listIndexes().keySet(), contains("basic"));
    }

    @Test
    public void createIndexWithSpaceInName() throws Exception {
        fd.ensureIndexed(Arrays.<Object>asList("name", "address"), "basic index");
        assertThat(fd.listIndexes().keySet(), contains("basic index"));
    }

    @Test
         public void createIndexWithSingleQuoteInName() throws Exception {
        fd.ensureIndexed(Arrays.<Object>asList("name", "address"), "basic'index");
        assertThat(fd.listIndexes().keySet(), contains("basic'index"));
    }

    @Test
    public void createIndexWithSemiColonQuoteInName() throws Exception {
        fd.ensureIndexed(Arrays.<Object>asList("name", "address"), "basic;index");
        assertThat(fd.listIndexes().keySet(), contains("basic;index"));
    }

    @Test
    public void createIndexWithBracketsInName() throws Exception {
        fd.ensureIndexed(Arrays.<Object>asList("name", "address"), "basic(index)");
        assertThat(fd.listIndexes().keySet(), contains("basic(index)"));
    }

    @Test
    public void createIndexWithKeyWordName() throws Exception {
        fd.ensureIndexed(Arrays.<Object>asList("name", "address"), "INSERT INDEX");
        assertThat(fd.listIndexes().keySet(), contains("INSERT INDEX"));
    }



    @Test
     public void deleteEmptyIndex() throws Exception {
        fd.ensureIndexed(Arrays.<Object>asList("name", "address"), "basic");
        assertThat(fd.listIndexes().keySet(), contains("basic"));

        assertThat(fd.deleteIndexNamed("basic"), is(true));
        assertThat(fd.listIndexes().isEmpty(), is(true));
    }

    @Test
    public void deleteTheCorrectEmptyIndex() throws Exception {
        fd.ensureIndexed(Arrays.<Object>asList("name", "address"), "basic");
        fd.ensureIndexed(Arrays.<Object>asList("name", "age"), "basic2");
        fd.ensureIndexed(Arrays.<Object>asList("name"), "basic3");
        assertThat(fd.listIndexes().keySet(), containsInAnyOrder("basic", "basic2", "basic3"));

        assertThat(fd.deleteIndexNamed("basic2"), is(true));
        assertThat(fd.listIndexes().keySet(), containsInAnyOrder("basic", "basic3"));
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

        fd.ensureIndexed(Arrays.<Object>asList("name", "address"), "basic");
        assertThat(fd.listIndexes().keySet(), contains("basic"));
        fd.deleteIndexNamed("basic");
        assertThat(fd.listIndexes().isEmpty(), is(true));
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

        fd.ensureIndexed(Arrays.<Object>asList("name", "address"), "basic");
        fd.ensureIndexed(Arrays.<Object>asList("name", "age"), "basic2");
        fd.ensureIndexed(Arrays.<Object>asList("name"), "basic3");
        assertThat(fd.listIndexes().keySet(), containsInAnyOrder("basic", "basic2", "basic3"));

        assertThat(fd.deleteIndexNamed("basic2"), is(true));
        assertThat(fd.listIndexes().keySet(), containsInAnyOrder("basic", "basic3"));
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

        fd.ensureIndexed(Arrays.<Object>asList("name", "address"), "basic", IndexType.TEXT);
        assertThat(fd.listIndexes().keySet(), contains("basic"));

        assertThat(fd.deleteIndexNamed("basic"), is(true));
        assertThat(fd.listIndexes().isEmpty(), is(true));
    }

    @Test
    public void validateTextSearchIsAvailable() throws Exception {
        assertThat(fd.isTextSearchEnabled(), is(true));
    }

}
