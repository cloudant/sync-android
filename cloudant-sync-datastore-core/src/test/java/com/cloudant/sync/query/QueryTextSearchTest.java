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
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLQueueCallable;
import com.cloudant.sync.util.SQLDatabaseTestUtils;
import com.cloudant.sync.util.TestUtils;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
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
        fd = ds;
        assertThat(fd, is(notNullValue()));
        final String[] metadataTableList = new String[] { QueryConstants.INDEX_METADATA_TABLE_NAME };
        dbq.submit(new SQLQueueCallable<Void>() {
            @Override
            public Void call(SQLDatabase db) throws Exception {
                SQLDatabaseTestUtils.assertTablesExist(db, metadataTableList);
                return null;
            }
        }).get();

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
    public void canMakeAQueryConsistingOfASingleTextSearch() throws Exception {
        assertThat(fd.ensureIndexed(Arrays.<Object>asList("comment"), "basic_text", IndexType.TEXT),
                                    is("basic_text"));

        // query - { "$text" : { "$search" : "lives in Bristol" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "lives in Bristol");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "fred12"));
    }

    @Test
    public void canMakeAPhraseSearch() throws Exception {
        assertThat(fd.ensureIndexed(Arrays.<Object>asList("comment"), "basic_text", IndexType.TEXT),
                                    is("basic_text"));

        // query - { "$text" : { "$search" : "\"lives in Bristol\"" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "\"lives in Bristol\"");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred12"));
    }

    @Test
    public void canMakeAQueryTextSearchContainingAnApostrophe() throws Exception {
        assertThat(fd.ensureIndexed(Arrays.<Object>asList("comment"), "basic_text", IndexType.TEXT),
                                    is("basic_text"));

        // query - { "$text" : { "$search" : "He's retired" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "He's retired");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), contains("mike72"));
    }

    @Test
    public void canMakeAQueryConsistingOfASingleTextSearchWithASort() throws Exception {
        assertThat(fd.ensureIndexed(Arrays.<Object>asList("name", "comment"), "basic_text", IndexType.TEXT),
                                    is("basic_text"));

        // query - { "$text" : { "$search" : "best friend" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "best friend");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        List<Map<String, String>> sortDocument = new ArrayList<Map<String, String>>();
        Map<String, String> sortByName = new HashMap<String, String>();
        sortByName.put("name", "asc");
        sortDocument.add(sortByName);
        QueryResult queryResult = fd.find(query, 0, 0, null, sortDocument);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred12"));
        assertThat(queryResult.documentIds().get(0), is("fred12"));
        assertThat(queryResult.documentIds().get(1), is("mike12"));
    }

    @Test
    public void canMakeANDCompoundQueryWithATextSearch() throws Exception {
        assertThat(fd.ensureIndexed(Arrays.<Object>asList("name"), "basic"), is("basic"));
        assertThat(fd.ensureIndexed(Arrays.<Object>asList("name", "comment"), "basic_text", IndexType.TEXT),
                   is("basic_text"));

        // query - { "name" : "mike", "$text" : { "$search" : "best friend" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "best friend");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), contains("mike12"));
    }

    @Test
    public void canMakeORCompoundQueryWithATextSearch() throws Exception {
        assertThat(fd.ensureIndexed(Arrays.<Object>asList("name"), "basic"), is("basic"));
        assertThat(fd.ensureIndexed(Arrays.<Object>asList("comment"), "basic_text", IndexType.TEXT),
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

        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12",
                                                                 "mike34",
                                                                 "mike72",
                                                                 "fred12"));
    }

    @Test
    public void nullForTextSearchQueryWithoutATextIndex() throws Exception {
        assertThat(fd.ensureIndexed(Arrays.<Object>asList("name"), "basic"), is("basic"));

        // query - { "name" : "mike", "$text" : { "$search" : "best friend" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "best friend");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("name", "mike");
        query.put("$text", search);
        assertThat(fd.find(query), is(nullValue()));
    }

    @Test
    public void nullForTextSearchQueryWhenAJsonIndexIsMissing() throws Exception {
        // All fields in a TEXT index only apply to the text search portion of any query.
        // So even though "name" exists in the text index, the clause that { "name" : "mike" }
        // expects a JSON index that contains the "name" field.  Since, this query includes a
        // text search clause then all clauses of the query must be satisfied by existing indexes.
        assertThat(fd.ensureIndexed(Arrays.<Object>asList("name", "comment"), "basic_text", IndexType.TEXT),
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

        assertThat(fd.find(query), is(nullValue()));
    }

    @Test
    public void canMakeATextSearchUsingNonAsciiValues() throws Exception {
        List<Object> fields = Collections.<Object>singletonList("comment");
        assertThat(fd.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "\"صديق له هو\"" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "\"صديق له هو\"");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), contains("john34"));
    }

    @Test
    public void returnsEmptyResultSetForUnmatchedPhraseSearch() throws Exception {
        List<Object> fields = Collections.<Object>singletonList("comment");
        assertThat(fd.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "\"Remus Romulus\"" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "\"Remus Romulus\"");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void returnsCorrectResultSetForNonContiguousWordSearch() throws Exception {
        List<Object> fields = Collections.<Object>singletonList("comment");
        assertThat(fd.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "Remus Romulus" } }
        // - The search predicate "Remus Romulus" normalizes to "Remus AND Romulus" in SQLite
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "Remus Romulus");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), contains("fred34"));
    }

    @Test
    public void canQueryUsingEnhancedQuerySyntaxOR() throws Exception {
        List<Object> fields = Collections.<Object>singletonList("comment");
        assertThat(fd.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "Remus OR Romulus" } }
        // - Enhanced Query Syntax - logical operators must be uppercase otherwise they will
        //   be treated as a search token
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "Remus OR Romulus");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("fred34", "mike72"));
    }

    @Test
    public void canQueryUsingEnhancedQuerySyntaxNOT() throws Exception{

        Boolean ftsEnabled = dbq.submit(new SQLQueueCallable<Boolean>() {
            @Override
            public Boolean call(SQLDatabase db) throws Exception {
                Set<String> compileOptions = SQLDatabaseTestUtils.getCompileOptions(db);
                // Only execute this test if SQLite enhanced query syntax is enabled
                return compileOptions.containsAll(Arrays.asList("ENABLE_FTS3", "ENABLE_FTS3_PARENTHESIS"));

            }
        }).get();

                if (ftsEnabled) {
                    List<Object> fields = Collections.<Object>singletonList("comment");
                    assertThat(fd.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

                    // query - { "$text" : { "$search" : "Remus NOT Romulus" } }
                    // - Enhanced Query Syntax - logical operators must be uppercase otherwise they will
                    //   be treated as a search token
                    // - NOT operator only works between tokens as in (token1 NOT token2)
                    Map<String, Object> search = new HashMap<String, Object>();
                    search.put("$search", "Remus NOT Romulus");
                    Map<String, Object> query = new HashMap<String, Object>();
                    query.put("$text", search);
                    QueryResult queryResult = fd.find(query);
                    assertThat(queryResult.documentIds(), contains("mike72"));
                }
    }

    @Test
    public void canQueryUsingEnhancedQuerySyntaxParentheses() throws Exception {
        Boolean ftsEnabled = dbq.submit(new SQLQueueCallable<Boolean>() {
            @Override
            public Boolean call(SQLDatabase db) throws Exception {
                Set<String> compileOptions = SQLDatabaseTestUtils.getCompileOptions(db);
                // Only execute this test if SQLite enhanced query syntax is enabled
                return compileOptions.containsAll(Arrays.asList("ENABLE_FTS3", "ENABLE_FTS3_PARENTHESIS"));

            }
        }).get();
                if (ftsEnabled) {
                    List<Object> fields = Collections.<Object>singletonList("comment");
                    assertThat(fd.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

                    // query - { "$text" : { "$search" : "(Remus OR Romulus) AND \"lives next door\"" } }
                    // - Parentheses are used to override SQLite enhanced query syntax operator precedence
                    // - Operator precedence is NOT -> AND -> OR
                    Map<String, Object> search = new HashMap<String, Object>();
                    search.put("$search", "(Remus OR Romulus) AND \"lives next door\"");
                    Map<String, Object> query = new HashMap<String, Object>();
                    query.put("$text", search);
                    QueryResult queryResult = fd.find(query);
                    assertThat(queryResult.documentIds(), contains("fred34"));
                }

    }

    @Test
    public void canQueryUsingNEAR() throws Exception {
        List<Object> fields = Collections.<Object>singletonList("comment");
        assertThat(fd.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "\"he lives\" NEAR/2 Bristol" } }
        // - NEAR provides the ability to search for terms/phrases in proximity to each other
        // - By specifying a value for NEAR as in NEAR/2 you can define the range of proximity.
        //   If left out it defaults to 10
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "\"he lives\" NEAR/2 Bristol");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred12"));
    }

    @Test
    public void ignoresCapitalizationUsingDefaultTokenizer() throws Exception {
        List<Object> fields = Collections.<Object>singletonList("comment");
        assertThat(fd.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "rEmUs RoMuLuS" } }
        // - Search is generally case-insensitive unless a custom tokenizer is provided
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "rEmUs RoMuLuS");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), contains("fred34"));
    }

    @Test
    public void queriesNonStringFieldAsAString() throws Exception {
        // Text index on age field
        List<Object> fields = Collections.<Object>singletonList("age");
        assertThat(fd.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "12" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "12");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred12"));
    }

    @Test
    public void returnsNullWhenSearchCriteriaNotAString() throws Exception {
        // Text index on age field
        List<Object> fields = Collections.<Object>singletonList("age");
        assertThat(fd.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : 12 } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", 12);
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult, is(nullValue()));
    }

    @Test
    public void canQueryAcrossMultipleFields() throws Exception {
        // Text index on name and comment fields
        List<Object> fields = Arrays.<Object>asList("name", "comment");
        assertThat(fd.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "Fred" } }
        //       - Will find both fred12 and fred34 as well as mike12 since Fred is mentioned
        //         in mike12's comment
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "Fred");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "fred12", "fred34"));
    }

    @Test
    public void canQueryTargetingSpecificFields() throws Exception {
        // Text index on name and comment fields
        List<Object> fields = Arrays.<Object>asList("name", "comment");
        assertThat(fd.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "name:fred comment:lives in Bristol" } }
        //       - Will only find fred12 since he is the only named fred who's comment
        //         states that he "lives in Bristol"
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "name:fred comment:lives in Bristol");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), contains("fred12"));
    }

    @Test
    public void canQueryUsingPrefixSearches() throws Exception {
        List<Object> fields = Collections.<Object>singletonList("comment");
        assertThat(fd.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "liv* riv*" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "liv* riv*");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), contains("mike34"));
    }

    @Test
    public void returnsEmptyResultSetWhenPrefixSearchesMissingWildcards() throws Exception {
        List<Object> fields = Collections.<Object>singletonList("comment");
        assertThat(fd.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "liv riv" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "liv riv");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void canQueryUsingID() throws Exception {
        List<Object> fields = Collections.<Object>singletonList("comment");
        assertThat(fd.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "_id:mike*" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "_id:mike*");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), containsInAnyOrder("mike12", "mike34", "mike72"));
    }

    @Test
    public void canQueryUsingPorterTokenizerStemmer() throws Exception {
        List<Object> fields = Collections.<Object>singletonList("comment");
        Map<String, String> indexSettings = new HashMap<String, String>();
        indexSettings.put("tokenize", "porter");
        assertThat(fd.ensureIndexed(fields, "basic_text", IndexType.TEXT, indexSettings), is("basic_text"));

        // query - { "$text" : { "$search" : "live" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "retire memory");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), contains("mike72"));
    }

    @Test
    public void returnsEmptyResultSetUsingDefaultTokenizerStemmer() throws Exception {
        List<Object> fields = Collections.<Object>singletonList("comment");
        assertThat(fd.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "live" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "retire memory");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), is(empty()));
    }

    @Test
    public void canQueryUsingAnApostrophe() throws Exception {
        List<Object> fields = Collections.<Object>singletonList("comment");
        assertThat(fd.ensureIndexed(fields, "basic_text", IndexType.TEXT), is("basic_text"));

        // query - { "$text" : { "$search" : "He's retired" } }
        Map<String, Object> search = new HashMap<String, Object>();
        search.put("$search", "He's retired");
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("$text", search);
        QueryResult queryResult = fd.find(query);
        assertThat(queryResult.documentIds(), contains("mike72"));
    }

}
