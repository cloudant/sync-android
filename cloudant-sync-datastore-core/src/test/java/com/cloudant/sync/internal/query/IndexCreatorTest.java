/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2014 Cloudant, Inc. All rights reserved.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import com.cloudant.sync.query.FieldSort;
import com.cloudant.sync.query.Index;
import com.cloudant.sync.query.IndexType;
import com.cloudant.sync.query.QueryException;
import com.cloudant.sync.query.Tokenizer;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IndexCreatorTest extends AbstractIndexTestBase {

    @Test
    public void emptyIndexList() throws QueryException {
        List<Index> indexes = im.listIndexes();
        assertThat(indexes, is(notNullValue()));
        assertThat(indexes.isEmpty(), is(true));
    }

    @Test(expected = NullPointerException.class)
    public void preconditionsToCreatingIndexesNullFields() throws QueryException {
        // doesn't create an index on null fields
        im.ensureIndexedJson(null, "basic");
        Assert.fail("Expected ensureIndexedJson to throw a NullPointerException");
    }

    @Test(expected = IllegalArgumentException.class)
    public void preconditionsToCreatingIndexesNoFields() throws QueryException {
        List<FieldSort> fieldNames;
        // doesn't create an index on no fields
        fieldNames = new ArrayList<FieldSort>();
        im.ensureIndexedJson(fieldNames, "basic");
        Assert.fail("Expected ensureIndexedJson to throw a IllegalArgumentException");
    }

    @Test(expected = IllegalArgumentException.class)
    public void preconditionsToCreatingIndexesNoName() throws QueryException {
        List<FieldSort> fieldNames = null;
        // doesn't create an index without a name
        fieldNames = new ArrayList<FieldSort>();
        im.ensureIndexedJson(fieldNames, "");
        Assert.fail("Expected ensureIndexedJson to throw a IllegalArgumentException");
    }

    @Test(expected = NullPointerException.class)
    public void preconditionsToCreatingIndexesNullType() throws QueryException {
        List<FieldSort> fieldNames = null;
        // doesn't create an index on null index type
        im.ensureIndexedJson(fieldNames, "basic");
        Assert.fail("Expected ensureIndexedJson to throw a NullPointerException");
    }

    @Test(expected = IllegalArgumentException.class)
    public void preconditionsToCreatingIndexesDuplicateFields() throws QueryException {
        List<FieldSort> fieldNames;
        // doesn't create an index if duplicate fields
        fieldNames = Arrays.<FieldSort>asList(new FieldSort("age"), new FieldSort("pet"), new
                FieldSort("age"));
        im.ensureIndexedJson(fieldNames, "basic");
        Assert.fail("Expected ensureIndexedJson to throw a IllegalArgumentException");
    }

    @Test
    public void createIndexOverOneField() throws QueryException {
        String indexName = im.ensureIndexedJson(Arrays.<FieldSort>asList(new FieldSort("name")), "basic").indexName;
        assertThat(indexName, is("basic"));

        List<Index> indexes = im.listIndexes();
        assertThat(indexes, Matchers.contains(IndexMatcherHelpers.getIndexNameMatcher("basic")));

        assertThat(IndexMatcherHelpers.getIndexNamed("basic", indexes), IndexMatcherHelpers.hasFieldsInAnyOrder("_id", "_rev", "name"));
    }

    @Test
    public void createIndexOverTwoFields() throws QueryException {
        String indexName = im.ensureIndexedJson(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "basic").indexName;
        assertThat(indexName, is("basic"));

        List<Index> indexes = im.listIndexes();
        assertThat(indexes, Matchers.contains(IndexMatcherHelpers.getIndexNameMatcher("basic")));

        assertThat(IndexMatcherHelpers.getIndexNamed("basic", indexes), IndexMatcherHelpers.hasFieldsInAnyOrder("_id", "_rev", "name", "age"));
    }

    @Test
    public void createIndexUsingDottedNotation() throws QueryException {
        String indexName = im.ensureIndexedJson(Arrays.<FieldSort>asList(new FieldSort("name.first"), new FieldSort("age.years")),
                                            "basic").indexName;
        assertThat(indexName, is("basic"));

        List<Index> indexes = im.listIndexes();
        assertThat(indexes, Matchers.contains(IndexMatcherHelpers.getIndexNameMatcher("basic")));

        assertThat(IndexMatcherHelpers.getIndexNamed("basic", indexes), IndexMatcherHelpers.hasFieldsInAnyOrder("_id", "_rev", "name.first", "age.years"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void createMultipleIndexes() throws QueryException {

        im.ensureIndexedJson(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "basic");
        im.ensureIndexedJson(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "another");
        im.ensureIndexedJson(Arrays.<FieldSort>asList(new FieldSort("cat")), "petname");

        // basic has the same definition as another so another won't appear in the output
        List<Index> indexes = im.listIndexes();
        assertThat(indexes, containsInAnyOrder(IndexMatcherHelpers.getIndexNameMatcher("basic"),
                IndexMatcherHelpers.getIndexNameMatcher("petname")));

        assertThat(IndexMatcherHelpers.getIndexNamed("basic", indexes), IndexMatcherHelpers.hasFieldsInAnyOrder("_id", "_rev", "name", "age"));
        assertThat(IndexMatcherHelpers.getIndexNamed("petname", indexes), IndexMatcherHelpers.hasFieldsInAnyOrder("_id", "_rev", "cat"));
    }

    @Test
    public void createIndexSpecifiedWithAscOrDesc() throws QueryException {
        String indexName = im.ensureIndexedJson(Arrays.<FieldSort>asList(
                new FieldSort("name"),
                new FieldSort("age")), "basic").indexName;
        assertThat(indexName, is("basic"));

        List<Index> indexes = im.listIndexes();
        assertThat(indexes, Matchers.contains(IndexMatcherHelpers.getIndexNameMatcher("basic")));

        assertThat(IndexMatcherHelpers.getIndexNamed("basic", indexes), IndexMatcherHelpers.hasFieldsInAnyOrder("_id", "_rev", "name", "age"));
    }

    // both indexes have same names, definitions same - return the first
    @Test
    public void createIndexWhenIndexNameExistsIdxDefinitionSame() throws QueryException {
        String indexName = im.ensureIndexedJson(Arrays.<FieldSort>asList(
                new FieldSort("name"),
                new FieldSort("age")), "basic").indexName;
        assertThat(indexName, is("basic"));

        // succeeds when the index definition is the same
        indexName = im.ensureIndexedJson(Arrays.<FieldSort>asList(
                new FieldSort("name"),
                new FieldSort("age")), "basic").indexName;
        assertThat(indexName, is("basic"));
        assertThat(im.listIndexes(), hasSize(1));
    }

    // both indexes have generated names, definitions same - return the first
    @Test
    public void createIndexWhenIndexNamesGeneratedIdxDefinitionSame() throws QueryException {
        String indexName = im.ensureIndexedJson(Arrays.<FieldSort>asList(
                new FieldSort("name"),
                new FieldSort("age")), null).indexName;

        // succeeds when the index definition is the same
        String indexName2 = im.ensureIndexedJson(Arrays.<FieldSort>asList(
                new FieldSort("name"),
                new FieldSort("age")), null).indexName;
        assertThat(indexName, is(indexName2));
        assertThat(im.listIndexes(), hasSize(1));
    }

    // indexes have different names but the same definitions - return the first
    @Test
    public void createIndexWhenIndexNamesDifferentIdxDefinitionSame() throws QueryException {
        String indexName = im.ensureIndexedJson(Arrays.<FieldSort>asList(
                new FieldSort("name"),
                new FieldSort("age")), "basic").indexName;

        // creates second index
        String indexName2 = im.ensureIndexedJson(Arrays.<FieldSort>asList(
                new FieldSort("name"),
                new FieldSort("age")), "basic2").indexName;
        assertThat(indexName, is(indexName2));
        assertThat(im.listIndexes(), hasSize(1));
    }

    @Test
    public void createIndexWhenIndexNameExistsIdxDefinitionDifferent() throws QueryException {
        String indexName = im.ensureIndexedJson(Arrays.<FieldSort>asList(
                new FieldSort("name"),
                new FieldSort("age")), "basic").indexName;
        assertThat(indexName, is("basic"));

        // fails when the index definition is different
        try {
            indexName = im.ensureIndexedJson(Arrays.<FieldSort>asList(
                    new FieldSort("name"),
                    new FieldSort("pet")), "basic").indexName;
            Assert.fail("ensureIndexedJson should throw QueryException");
        } catch (QueryException qe) {
            ;
        }
    }

    @Test
    public void createIndexWithJsonType() throws QueryException {
        // supports using the json type
        String indexName = im.ensureIndexedJson(Arrays.<FieldSort>asList(
                new FieldSort("name"),
                new FieldSort("age")),
                                            "basic").indexName;
        assertThat(indexName, is("basic"));
        List<Index> indexes = im.listIndexes();
        assertThat(indexes, Matchers.contains(IndexMatcherHelpers.getIndexNameMatcher("basic")));
        Index i = IndexMatcherHelpers.getIndexNamed("basic", indexes);
        assertThat(i.indexType, is(IndexType.JSON));
        assertThat(i.tokenizer, is(nullValue()));
    }

    @Test
    public void createIndexWithTextTypeTokenizerNull() throws QueryException {
        String indexName = im.ensureIndexedText(Arrays.<FieldSort>asList(
                new FieldSort("name"),
                new FieldSort("age")),
                                            "basic",
                                            null).indexName;
        assertThat(indexName, is("basic"));
        List<Index> indexes = im.listIndexes();
        Index i = IndexMatcherHelpers.getIndexNamed("basic", indexes);
        assertThat(i.indexType, is(IndexType.TEXT));
        assertThat(i.tokenizer.tokenizerName, is("simple"));
    }

    @Test
    public void createIndexWithTextTypeTokenizerDefault() throws QueryException {
        String indexName = im.ensureIndexedText(Arrays.<FieldSort>asList(
                new FieldSort("name"),
                new FieldSort("age")),
                "basic",
                Tokenizer.DEFAULT).indexName;
        assertThat(indexName, is("basic"));
        List<Index> indexes = im.listIndexes();
        Index i = IndexMatcherHelpers.getIndexNamed("basic", indexes);
        assertThat(i.indexType, is(IndexType.TEXT));
        assertThat(i.tokenizer.tokenizerName, is("simple"));
        assertThat(i.tokenizer.tokenizerArguments, nullValue());
    }

    @Test
    public void createIndexWithTextTypeAndTokenizeSetting() throws QueryException {
        String indexName = im.ensureIndexedText(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")),
                "basic",
                new Tokenizer("porter")).indexName;
        assertThat(indexName, is("basic"));
        List<Index> indexes = im.listIndexes();
        Index i = IndexMatcherHelpers.getIndexNamed("basic", indexes);
        assertThat(i.indexType, is(IndexType.TEXT));
        assertThat(i.tokenizer.tokenizerName, is("porter"));
        assertThat(i.tokenizer.tokenizerArguments, nullValue());
    }

    @Test
    public void createIndexWithTextTypeAndTokenizeSettingWithArgs() throws QueryException {
        String indexName = im.ensureIndexedText(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")),
                "basic",
                new Tokenizer("porter", "unicode61 remove_diacritics 1")).indexName;
        assertThat(indexName, is("basic"));
        List<Index> indexes = im.listIndexes();
        Index i = IndexMatcherHelpers.getIndexNamed("basic", indexes);
        assertThat(i.indexType, is(IndexType.TEXT));
        assertThat(i.tokenizer.tokenizerName, is("porter"));
        assertThat(i.tokenizer.tokenizerArguments, is("unicode61 remove_diacritics 1"));
    }

    // as above, but ask for the index name to be generated
    @Test
    public void createIndexWithTextTypeAndTokenizeSettingWithArgsDefaultName() throws QueryException {
        String indexName = im.ensureIndexedText(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")),
                null,
                new Tokenizer("porter", "unicode61 remove_diacritics 1")).indexName;
        assertThat(indexName, startsWith("com.cloudant.sync.query.GeneratedIndexName.Index"));
        List<Index> indexes = im.listIndexes();
        Index i = IndexMatcherHelpers.getIndexNamed(indexName, indexes);
        assertThat(i.indexType, is(IndexType.TEXT));
        assertThat(i.indexName, startsWith("com.cloudant.sync.query.GeneratedIndexName.Index"));
        assertThat(i.tokenizer.tokenizerName, is("porter"));
        assertThat(i.tokenizer.tokenizerArguments, is("unicode61 remove_diacritics 1"));
    }

    // as above but check values returned from ensureIndexedText
    @Test
    public void checkEnsureIndexedReturnsCorrectValues() throws QueryException {
        Index i = im.ensureIndexedText(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")),
                null,
                new Tokenizer("porter", "unicode61 remove_diacritics 1"));
        assertThat(i.indexType, is(IndexType.TEXT));
        assertThat(i.indexName, startsWith("com.cloudant.sync.query.GeneratedIndexName.Index"));
        assertThat(i.tokenizer.tokenizerName, is("porter"));
        assertThat(i.tokenizer.tokenizerArguments, is("unicode61 remove_diacritics 1"));
    }

    @Test
    public void indexAndTextIndexCanCoexist() throws QueryException {
        String indexName = im.ensureIndexedText(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")),
                                            "textIndex",
                                            new Tokenizer("porter")).indexName;
        assertThat(indexName, is("textIndex"));
        indexName = im.ensureIndexedJson(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "jsonIndex").indexName;
        assertThat(indexName, is("jsonIndex"));
        List<Index> indexes = im.listIndexes();
        assertThat(indexes, containsInAnyOrder(IndexMatcherHelpers.getIndexNameMatcher("textIndex"),
                IndexMatcherHelpers.getIndexNameMatcher("jsonIndex")));
    }

    @Test(expected = QueryException.class)
    public void correctlyLimitsTextIndexesToOne() throws QueryException {
        String indexName = im.ensureIndexedText(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age")), "basic", null).indexName;
        assertThat(indexName, is("basic"));
        indexName = im.ensureIndexedText(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("donuts")), "anotherIndex", null).indexName;
    }

    @Test
    public void createIndexUsingNonAsciiText() throws QueryException {
        // can create indexes successfully
        String indexName = im.ensureIndexedJson(Arrays.<FieldSort>asList(new FieldSort("اسم"), new FieldSort("datatype"), new FieldSort("ages")),
                                            "basic").indexName;
        assertThat(indexName, is("basic"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void createIndexWhereFieldNameContainsDollarSignAtStart() throws QueryException {
        // rejects indexes with $ at start
       im.ensureIndexedJson(Arrays.<FieldSort>asList(new FieldSort("$name"), new FieldSort("datatype")), "basic");
    }
    @Test
    public void createIndexWhereFieldNameContainsDollarSignNotAtStart() throws QueryException {
        // creates indexes with $ not at start
        String indexName = im.ensureIndexedJson(Arrays.<FieldSort>asList(new FieldSort("na$me"), new FieldSort("datatype$")), "basic").indexName;
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

    // we don't support defining indexes with descending order
    @Test(expected = UnsupportedOperationException.class)
    public void fieldSortDescendingNotSupported() throws QueryException {
        im.ensureIndexedJson(Arrays.<FieldSort>asList(new FieldSort("name", FieldSort.Direction
                .DESCENDING), new FieldSort("age")), null);
    }


}
