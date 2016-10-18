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

import static com.cloudant.sync.query.IndexMatcherHelpers.hasFieldsInAnyOrder;
import static com.cloudant.sync.query.IndexMatcherHelpers.getIndexNameMatcher;
import static com.cloudant.sync.query.IndexMatcherHelpers.getIndexNamed;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IndexCreatorTest extends AbstractIndexTestBase {

    @Test
    public void emptyIndexList() {
        List<Index> indexes = im.listIndexes();
        assertThat(indexes, is(notNullValue()));
        assertThat(indexes.isEmpty(), is(true));
    }

    @Test(expected = NullPointerException.class)
    public void preconditionsToCreatingIndexesNullFields() throws QueryException {
        // doesn't create an index on null fields
        im.ensureIndexed(null, "basic");
        Assert.fail("Expected ensureIndexed to throw a NullPointerException");
    }

    @Test(expected = IllegalArgumentException.class)
    public void preconditionsToCreatingIndexesNoFields() throws QueryException {
        List<FieldSort> fieldNames;
        // doesn't create an index on no fields
        fieldNames = new ArrayList<FieldSort>();
        im.ensureIndexed(fieldNames, "basic");
        Assert.fail("Expected ensureIndexed to throw a IllegalArgumentException");
    }

    @Test(expected = IllegalArgumentException.class)
    public void preconditionsToCreatingIndexesNoName() throws QueryException {
        List<FieldSort> fieldNames = null;
        // doesn't create an index without a name
        fieldNames = new ArrayList<FieldSort>();
        im.ensureIndexed(fieldNames, "");
        Assert.fail("Expected ensureIndexed to throw a IllegalArgumentException");
    }

    @Test(expected = NullPointerException.class)
    public void preconditionsToCreatingIndexesNullType() throws QueryException {
        List<FieldSort> fieldNames = null;
        // doesn't create an index on null index type
        im.ensureIndexed(fieldNames, "basic", null);
        Assert.fail("Expected ensureIndexed to throw a NullPointerException");
    }

    @Test(expected = IllegalArgumentException.class)
    public void preconditionsToCreatingIndexesDuplicateFields() throws QueryException {
        List<FieldSort> fieldNames;
        // doesn't create an index if duplicate fields
        fieldNames = Arrays.<FieldSort>asList(new FieldSort("age"), new FieldSort("pet"), new
                FieldSort("age"));
        im.ensureIndexed(fieldNames, "basic");
        Assert.fail("Expected ensureIndexed to throw a IllegalArgumentException");
    }

    @Test
    public void createIndexOverOneField() throws QueryException {
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name")), "basic");
        assertThat(indexName, is("basic"));

        List<Index> indexes = im.listIndexes();
        assertThat(indexes, contains(getIndexNameMatcher("basic")));

        assertThat(getIndexNamed("basic", indexes), hasFieldsInAnyOrder("_id", "_rev", "name"));
    }

    @Test
    public void createIndexOverTwoFields() throws QueryException {
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "basic");
        assertThat(indexName, is("basic"));

        List<Index> indexes = im.listIndexes();
        assertThat(indexes, contains(getIndexNameMatcher("basic")));

        assertThat(getIndexNamed("basic", indexes), hasFieldsInAnyOrder("_id", "_rev", "name", "age"));
    }

    @Test
    public void createIndexUsingDottedNotation() throws QueryException {
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name.first"), new FieldSort("age.years")),
                                            "basic");
        assertThat(indexName, is("basic"));

        List<Index> indexes = im.listIndexes();
        assertThat(indexes, contains(getIndexNameMatcher("basic")));

        assertThat(getIndexNamed("basic", indexes), hasFieldsInAnyOrder("_id", "_rev", "name.first", "age.years"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void createMultipleIndexes() throws QueryException {
        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "basic");
        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "another");
        im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("cat")), "petname");


        List<Index> indexes = im.listIndexes();
        assertThat(indexes, containsInAnyOrder(getIndexNameMatcher("basic"),
                getIndexNameMatcher("another"),
                getIndexNameMatcher("petname")));

        assertThat(getIndexNamed("basic", indexes), hasFieldsInAnyOrder("_id", "_rev", "name", "age"));
        assertThat(getIndexNamed("another", indexes), hasFieldsInAnyOrder("_id", "_rev", "name", "age"));
        assertThat(getIndexNamed("petname", indexes), hasFieldsInAnyOrder("_id", "_rev", "cat"));
    }

    @Test
    public void createIndexSpecifiedWithAscOrDesc() throws QueryException {
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(
                new FieldSort("name", FieldSort.Direction.ASCENDING),
                new FieldSort("age", FieldSort.Direction.DESCENDING)), "basic");
        assertThat(indexName, is("basic"));

        List<Index> indexes = im.listIndexes();
        assertThat(indexes, contains(getIndexNameMatcher("basic")));

        assertThat(getIndexNamed("basic", indexes), hasFieldsInAnyOrder("_id", "_rev", "name", "age"));
    }

    @Test
    public void createIndexWhenIndexNameExistsIdxDefinitionSame() throws QueryException {
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(
                new FieldSort("name", FieldSort.Direction.ASCENDING),
                new FieldSort("age", FieldSort.Direction.DESCENDING)), "basic");
        assertThat(indexName, is("basic"));

        // succeeds when the index definition is the same
        indexName = im.ensureIndexed(Arrays.<FieldSort>asList(
                new FieldSort("name", FieldSort.Direction.ASCENDING),
                new FieldSort("age", FieldSort.Direction.DESCENDING)), "basic");
        assertThat(indexName, is("basic"));
    }

    @Test
    public void createIndexWhenIndexNameExistsIdxDefinitionDifferent() throws QueryException {
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(
                new FieldSort("name", FieldSort.Direction.ASCENDING),
                new FieldSort("age", FieldSort.Direction.DESCENDING)), "basic");
        assertThat(indexName, is("basic"));

        // fails when the index definition is different
        try {
            indexName = im.ensureIndexed(Arrays.<FieldSort>asList(
                    new FieldSort("name", FieldSort.Direction.ASCENDING),
                    new FieldSort("pet", FieldSort.Direction.DESCENDING)), "basic");
            Assert.fail("ensureIndexed should throw QueryException");
        } catch (QueryException qe) {
            ;
        }
    }

    @Test
    public void createIndexWithJsonType() throws QueryException {
        // supports using the json type
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(
                new FieldSort("name", FieldSort.Direction.ASCENDING),
                new FieldSort("age", FieldSort.Direction.DESCENDING)),
                                            "basic",
                                            IndexType.JSON);
        assertThat(indexName, is("basic"));
        List<Index> indexes = im.listIndexes();
        assertThat(indexes, contains(getIndexNameMatcher("basic")));
        Index i = getIndexNamed("basic", indexes);
        assertThat(i.indexType, is(IndexType.JSON));
        assertThat(i.tokenize, is(nullValue()));
    }

    @Test
    public void createIndexWithTextType() throws QueryException {
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(
                new FieldSort("name", FieldSort.Direction.ASCENDING),
                new FieldSort("age", FieldSort.Direction.DESCENDING)),
                                            "basic",
                                            IndexType.TEXT);
        assertThat(indexName, is("basic"));
        List<Index> indexes = im.listIndexes();
        Index i = getIndexNamed("basic", indexes);
        assertThat(i.indexType, is(IndexType.TEXT));
        assertThat(i.tokenize, is("simple"));
    }

    @Test
    public void createIndexWithTextTypeAndTokenizeSetting() throws QueryException {
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")),
                "basic",
                IndexType.TEXT,
                "porter");
        assertThat(indexName, is("basic"));
        List<Index> indexes = im.listIndexes();
        Index i = getIndexNamed("basic", indexes);
        assertThat(i.indexType, is(IndexType.TEXT));
        assertThat(i.tokenize, is("porter"));
    }

    @Test
    public void indexAndTextIndexCanCoexist() throws QueryException {
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")),
                                            "textIndex",
                                            IndexType.TEXT,
                                            "porter");
        assertThat(indexName, is("textIndex"));
        indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "jsonIndex");
        assertThat(indexName, is("jsonIndex"));
        List<Index> indexes = im.listIndexes();
        assertThat(indexes, containsInAnyOrder(getIndexNameMatcher("textIndex"),
                getIndexNameMatcher("jsonIndex")));
    }

    @Test(expected = QueryException.class)
    public void correctlyLimitsTextIndexesToOne() throws QueryException {
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "basic", IndexType.TEXT);
        assertThat(indexName, is("basic"));
        indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "anotherIndex", IndexType.TEXT);
    }

    @Test
    public void createIndexUsingNonAsciiText() throws QueryException {
        // can create indexes successfully
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("اسم"), new FieldSort("datatype"), new FieldSort("ages")),
                                            "basic");
        assertThat(indexName, is("basic"));
    }

    @Test
    public void createIndexWhereFieldNameContainsDollarSign() throws QueryException {

        // rejects indexes with $ at start
        try {
            im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("$name"), new FieldSort("datatype")), "basic");
            Assert.fail("Expected ensureIndexed to throw a IllegalArgumentException");
        } catch (IllegalArgumentException qe) {
            ;
        }

        // creates indexes with $ not at start
        String indexName = im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("na$me"), new FieldSort("datatype$")), "basic");
        assertThat(indexName, is("basic"));
    }

    @Test
    public void validateFieldNames() {
        // allows single fields
        assertThat(IndexCreator.validFieldName("name"), is(true));

        // allows dotted notation fields
        assertThat(IndexCreator.validFieldName("name.first"), is(true));
        assertThat(IndexCreator.validFieldName("name.first.prefix"), is(true));

        // allows dollars in positions other than first letter of a part
        assertThat(IndexCreator.validFieldName("na$me"), is(true));
        assertThat(IndexCreator.validFieldName("name.fir$t"), is(true));
        assertThat(IndexCreator.validFieldName("name.fir$t.pref$x"), is(true));

        // rejects dollars in first letter of a part
        assertThat(IndexCreator.validFieldName("$name"), is(false));
        assertThat(IndexCreator.validFieldName("name.$first"), is(false));
        assertThat(IndexCreator.validFieldName("name.$first.$prefix"), is(false));
        assertThat(IndexCreator.validFieldName("name.first.$prefix"), is(false));
        assertThat(IndexCreator.validFieldName("name.first.$pr$efix"), is(false));
        assertThat(IndexCreator.validFieldName("name.$$$$.prefix"), is(false));
    }

}
