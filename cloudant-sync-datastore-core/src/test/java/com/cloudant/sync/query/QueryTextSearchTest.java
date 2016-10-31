//  Copyright (c) 2015 IBM Cloudant. All rights reserved.
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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.util.SQLDatabaseTestUtils;
import com.cloudant.sync.util.TestUtils;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class QueryTextSearchTest extends AbstractQueryTestBase {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        indexManagerDatabaseQueue = TestUtils.getDBQueue(im);
        assertThat(im, is(notNullValue()));
        assertThat(indexManagerDatabaseQueue, is(notNullValue()));
        String[] metadataTableList = new String[] { IndexManagerImpl.INDEX_METADATA_TABLE_NAME };
        SQLDatabaseTestUtils.assertTablesExist(indexManagerDatabaseQueue, metadataTableList);

        DocumentRevision rev = new DocumentRevision("mike12");
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        bodyMap.put("comment", "He lives in Bristol, UK and his best friend is Fred.");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("mike34");
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "dog");
        bodyMap.put("comment", "He lives in a van down by the river in Bristol.");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("mike72");
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 72);
        bodyMap.put("pet", "cat");
        bodyMap.put("comment",
                    "He's retired and has memories of spending time with his cat Remus.");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fred34");
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "cat");
        bodyMap.put("comment",
                    "He lives next door to Mike and his cat Romulus is brother to Remus.");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("fred12");
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        bodyMap.put("comment", "He lives in Bristol, UK and his best friend is Mike.");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);

        rev = new DocumentRevision("john34");
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "cat");
        bodyMap.put("comment", "وهو يعيش في بريستول، المملكة المتحدة، وأفضل صديق له هو مايك.");
        rev.setBody(DocumentBodyFactory.create(bodyMap));
        ds.createDocumentFromRevision(rev);
    }

    @Test
    public void canMakeAQueryConsistingOfASingleTextSearch() throws QueryException {
        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("comment")), "basic_text", IndexType.TEXT),
                                    is("basic_text"));

        // query - { "$text" : { "$search" : "lives in Bristol" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "lives in Bristol");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "fred12"));
    }

    @Test
    public void canMakeAPhraseSearch() throws QueryException {
        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("comment")), "basic_text", IndexType.TEXT),
                                    is("basic_text"));

        // query - { "$text" : { "$search" : "\"lives in Bristol\"" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "\"lives in Bristol\"");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred12"));
    }

    @Test
    public void canMakeAQueryTextSearchContainingAnApostrophe() throws QueryException {
        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("comment")), "basic_text", IndexType.TEXT),
                                    is("basic_text"));

        // query - { "$text" : { "$search" : "He's retired" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "He's retired");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("mike72"));
    }

    @Test
    public void canMakeAQueryConsistingOfASingleTextSearchWithASort() throws QueryException {
        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("comment")), "basic_text", IndexType.TEXT),
                                    is("basic_text"));

        // query - { "$text" : { "$search" : "best friend" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "best friend");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query, 0, 0, null, Arrays.asList(new FieldSort("name", FieldSort.Direction.ASCENDING)));
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred12"));
        assertThat(queryResult.documentIds().get(0), is("fred12"));
        assertThat(queryResult.documentIds().get(1), is("mike12"));
    }

    @Test
    public void canMakeANDCompoundQueryWithATextSearch() throws QueryException {
        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name")), "basic"), is("basic"));
        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("comment")), "basic_text", IndexType.TEXT),
                   is("basic_text"));

        // query - { "name" : "mike", "$text" : { "$search" : "best friend" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "best friend");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    @Test
    public void canMakeORCompoundQueryWithATextSearch() throws QueryException {
        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name")), "basic"), is("basic"));
        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("comment")), "basic_text", IndexType.TEXT),
                   is("basic_text"));

        // query - { "$or" : [ { "name" : "mike" }, { "$text" : { "$search" : "best friend" } } ] }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "best friend");
        Map<String, Object> text = new HashMap<String, Object>();
        text.put("$text", search);
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(name, text));

        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike34",
                                                                 "mike72",
                                                                 "fred12"));
    }

    @Test
    public void nullForTextSearchQueryWithoutATextIndex() throws QueryException {
        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name")), "basic"), is("basic"));

        // query - { "name" : "mike", "$text" : { "$search" : "best friend" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "best friend");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        query.put("$text", search);
        assertThat(im.find(query), is(nullValue()));
    }

    @Test
    public void nullForTextSearchQueryWhenAJsonIndexIsMissing() throws QueryException {
        // All fields in a TEXT index only apply to the text search portion of any query.
        // So even though "name" exists in the text index, the clause that { "name" : "mike" }
        // expects a JSON index that contains the "name" field.  Since, this query includes a
        // text search clause then all clauses of the query must be satisfied by existing indexes.
        assertThat(im.ensureIndexed(Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("comment")), "basic_text", IndexType.TEXT),
                   is("basic_text"));

        // query - { "$or" : [ { "name" : "mike" }, { "$text" : { "$search" : "best friend" } } ] }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "best friend");
        Map<String, Object> text = new HashMap<String, Object>();
        text.put("$text", search);
        Map<String, Object> name = new HashMap<String, Object>();
        name.put("name", "mike");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$or", Arrays.<Object>asList(name, text));

        assertThat(im.find(query), is(nullValue()));
    }

    @Test
    public void canMakeATextSearchUsingNonAsciiValues() throws QueryException {
        List<FieldSort> fields = Collections.<FieldSort>singletonList(new FieldSort("comment"));
        assertThat(im.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "\"صديق له هو\"" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "\"صديق له هو\"");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("john34"));
    }

    @Test
    public void returnsEmptyResultSetForUnmatchedPhraseSearch() throws QueryException {
        List<FieldSort> fields = Collections.<FieldSort>singletonList(new FieldSort("comment"));
        assertThat(im.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "\"Remus Romulus\"" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "\"Remus Romulus\"");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void returnsCorrectResultSetForNonContiguousWordSearch() throws QueryException {
        List<FieldSort> fields = Collections.<FieldSort>singletonList(new FieldSort("comment"));
        assertThat(im.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "Remus Romulus" } }
        // - The search predicate "Remus Romulus" normalizes to "Remus AND Romulus" in SQLite
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "Remus Romulus");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("fred34"));
    }

    @Test
    public void canQueryUsingEnhancedQuerySyntaxOR() throws QueryException {
        List<FieldSort> fields = Collections.<FieldSort>singletonList(new FieldSort("comment"));
        assertThat(im.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "Remus OR Romulus" } }
        // - Enhanced query Syntax - logical operators must be uppercase otherwise they will
        //   be treated as a search token
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "Remus OR Romulus");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred34", "mike72"));
    }

    @Test
    public void canQueryUsingEnhancedQuerySyntaxNOT() throws Exception{
        // Only execute this test if SQLite enhanced query syntax is enabled
        Set<String> compileOptions = SQLDatabaseTestUtils.getCompileOptions(indexManagerDatabaseQueue);
        if (compileOptions.containsAll(Arrays.asList("ENABLE_FTS3", "ENABLE_FTS3_PARENTHESIS"))) {
            List<FieldSort> fields = Collections.<FieldSort>singletonList(new FieldSort("comment"));
            assertThat(im.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

            // query - { "$text" : { "$search" : "Remus NOT Romulus" } }
            // - Enhanced query Syntax - logical operators must be uppercase otherwise they will
            //   be treated as a search token
            // - NOT operator only works between tokens as in (token1 NOT token2)
            Map<String, Object> search = new HashMap<String, Object>();
            search.put("$search", "Remus NOT Romulus");
            Map<String, Object> query = new HashMap<String, Object>();
            query.put("$text", search);
            QueryResult queryResult = im.find(query);
            assertThat(queryResult.documentIds(), contains("mike72"));
        }
    }

    @Test
    public void canQueryUsingEnhancedQuerySyntaxParentheses() throws Exception{
        // Only execute this test if SQLite enhanced query syntax is enabled
        Set<String> compileOptions = SQLDatabaseTestUtils.getCompileOptions(indexManagerDatabaseQueue);
        if (compileOptions.containsAll(Arrays.asList("ENABLE_FTS3", "ENABLE_FTS3_PARENTHESIS"))) {
            List<FieldSort> fields = Collections.<FieldSort>singletonList(new FieldSort("comment"));
            assertThat(im.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

            // query - { "$text" : { "$search" : "(Remus OR Romulus) AND \"lives next door\"" } }
            // - Parentheses are used to override SQLite enhanced query syntax operator precedence
            // - Operator precedence is NOT -> AND -> OR
            Map<String, Object> search = new HashMap<String, Object>();
            search.put("$search", "(Remus OR Romulus) AND \"lives next door\"");
            Map<String, Object> query = new HashMap<String, Object>();
            query.put("$text", search);
            QueryResult queryResult = im.find(query);
            assertThat(queryResult.documentIds(), contains("fred34"));
        }
    }

    @Test
    public void canQueryUsingNEAR() throws QueryException {
        List<FieldSort> fields = Collections.<FieldSort>singletonList(new FieldSort("comment"));
        assertThat(im.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "\"he lives\" NEAR/2 Bristol" } }
        // - NEAR provides the ability to search for terms/phrases in proximity to each other
        // - By specifying a value for NEAR as in NEAR/2 you can define the range of proximity.
        //   If left out it defaults to 10
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "\"he lives\" NEAR/2 Bristol");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred12"));
    }

    @Test
    public void ignoresCapitalizationUsingDefaultTokenizer() throws QueryException {
        List<FieldSort> fields = Collections.<FieldSort>singletonList(new FieldSort("comment"));
        assertThat(im.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "rEmUs RoMuLuS" } }
        // - Search is generally case-insensitive unless a custom tokenizer is provided
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "rEmUs RoMuLuS");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("fred34"));
    }

    @Test
    public void queriesNonStringFieldAsAString() throws QueryException {
        // Text index on age field
        List<FieldSort> fields = Collections.<FieldSort>singletonList(new FieldSort("age"));
        assertThat(im.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "12" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "12");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred12"));
    }

    @Test
    public void returnsNullWhenSearchCriteriaNotAString() throws QueryException {
        // Text index on age field
        List<FieldSort> fields = Collections.<FieldSort>singletonList(new FieldSort("age"));
        assertThat(im.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : 12 } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult, is(nullValue()));
    }

    @Test
    public void canQueryAcrossMultipleFields() throws QueryException {
        // Text index on name and comment fields
        List<FieldSort> fields = Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("comment"));
        assertThat(im.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "Fred" } }
        //       - Will find both fred12 and fred34 as well as mike12 since Fred is mentioned
        //         in mike12's comment
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "Fred");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred12", "fred34"));
    }

    @Test
    public void canQueryTargetingSpecificFields() throws QueryException {
        // Text index on name and comment fields
        List<FieldSort> fields = Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("comment"));
        assertThat(im.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "name:fred comment:lives in Bristol" } }
        //       - Will only find fred12 since he is the only named fred who's comment
        //         states that he "lives in Bristol"
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "name:fred comment:lives in Bristol");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("fred12"));
    }

    @Test
    public void canQueryUsingPrefixSearches() throws QueryException {
        List<FieldSort> fields = Collections.<FieldSort>singletonList(new FieldSort("comment"));
        assertThat(im.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "liv* riv*" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "liv* riv*");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("mike34"));
    }

    @Test
    public void returnsEmptyResultSetWhenPrefixSearchesMissingWildcards() throws QueryException {
        List<FieldSort> fields = Collections.<FieldSort>singletonList(new FieldSort("comment"));
        assertThat(im.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "liv riv" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "liv riv");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void canQueryUsingID() throws QueryException {
        List<FieldSort> fields = Collections.<FieldSort>singletonList(new FieldSort("comment"));
        assertThat(im.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "_id:mike*" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "_id:mike*");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "mike72"));
    }

    @Test
    public void canQueryUsingPorterTokenizerStemmer() throws QueryException {
        List<FieldSort> fields = Collections.<FieldSort>singletonList(new FieldSort("comment"));
        Map<String, String> indexSettings = new HashMap<String, String>();
        assertThat(im.ensureIndexed(fields, "basic_text", IndexType.TEXT, "porter"), is("basic_text"));

        // query - { "$text" : { "$search" : "live" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "retire memory");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("mike72"));
    }

    @Test
    public void returnsEmptyResultSetUsingDefaultTokenizerStemmer() throws QueryException {
        List<FieldSort> fields = Collections.<FieldSort>singletonList(new FieldSort("comment"));
        assertThat(im.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "live" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "retire memory");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void canQueryUsingAnApostrophe() throws QueryException {
        List<FieldSort> fields = Collections.<FieldSort>singletonList(new FieldSort("comment"));
        assertThat(im.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "He's retired" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "He's retired");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = im.find(query);
        assertThat(queryResult.documentIds(), contains("mike72"));
    }

}
