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
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentRevision;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class IndexManagerTest extends AbstractIndexTestBase {

    @Test
    public void unimplementedEnsureIndexed() {
        assertThat(im.ensureIndexed(Arrays.<Object>asList("name")), is(nullValue()));
    }

    @Test
    public void deleteFailOnNoIndexName() {
        im.ensureIndexed(Arrays.<Object>asList("name", "address"), "basic");
        assertThat(im.listIndexes().keySet(), contains("basic"));

        assertThat(im.deleteIndexNamed(null), is(false));
        assertThat(im.listIndexes().keySet(), contains("basic"));

        assertThat(im.deleteIndexNamed(""), is(false));
        assertThat(im.listIndexes().keySet(), contains("basic"));
    }

    @Test
    public void deleteFailOnInvalidIndexName() {
        im.ensureIndexed(Arrays.<Object>asList("name", "address"), "basic");
        assertThat(im.listIndexes().keySet(), contains("basic"));

        assertThat(im.deleteIndexNamed("invalid"), is(false));
        assertThat(im.listIndexes().keySet(), contains("basic"));
    }

    @Test
     public void deleteEmptyIndex() {
        im.ensureIndexed(Arrays.<Object>asList("name", "address"), "basic");
        assertThat(im.listIndexes().keySet(), contains("basic"));

        assertThat(im.deleteIndexNamed("basic"), is(true));
        assertThat(im.listIndexes().isEmpty(), is(true));
    }

    @Test
    public void deleteTheCorrectEmptyIndex() {
        im.ensureIndexed(Arrays.<Object>asList("name", "address"), "basic");
        im.ensureIndexed(Arrays.<Object>asList("name", "age"), "basic2");
        im.ensureIndexed(Arrays.<Object>asList("name"), "basic3");
        assertThat(im.listIndexes().keySet(), containsInAnyOrder("basic", "basic2", "basic3"));

        assertThat(im.deleteIndexNamed("basic2"), is(true));
        assertThat(im.listIndexes().keySet(), containsInAnyOrder("basic", "basic3"));
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

        im.ensureIndexed(Arrays.<Object>asList("name", "address"), "basic");
        assertThat(im.listIndexes().keySet(), contains("basic"));
        im.deleteIndexNamed("basic");
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

        im.ensureIndexed(Arrays.<Object>asList("name", "address"), "basic");
        im.ensureIndexed(Arrays.<Object>asList("name", "age"), "basic2");
        im.ensureIndexed(Arrays.<Object>asList("name"), "basic3");
        assertThat(im.listIndexes().keySet(), containsInAnyOrder("basic", "basic2", "basic3"));

        assertThat(im.deleteIndexNamed("basic2"), is(true));
        assertThat(im.listIndexes().keySet(), containsInAnyOrder("basic", "basic3"));
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

        im.ensureIndexed(Arrays.<Object>asList("name", "address"), "basic", "text");
        assertThat(im.listIndexes().keySet(), contains("basic"));

        assertThat(im.deleteIndexNamed("basic"), is(true));
        assertThat(im.listIndexes().isEmpty(), is(true));
    }

    @Test
    public void validateTextSearchIsAvailable() throws Exception {
        assertThat(im.isTextSearchEnabled(), is(true));
    }

}
