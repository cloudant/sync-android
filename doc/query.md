# Cloudant Query - Android

Cloudant Query - Android is a Java implementation of [Cloudant Query][1] for Android mobile devices. It is included as part of the sync-core jar file.

Cloudant Query is inspired by MongoDB's query implementation, so users of MongoDB should feel at home using Cloudant Query in their mobile applications.

The aim is that the query you use on our cloud-based database works for your mobile application.

[1]: https://docs.cloudant.com/api/cloudant-query.html
[2]: https://github.com/cloudant/sync-android

## Usage

These notes assume familiarity with Cloudant Sync Datastore.

Cloudant Query uses indexes explicitly defined over the fields in the document. Multiple indexes can be created for use in different queries, the same field may end up indexed in more than one index.

Querying is carried out by supplying a query in the form of a map which describes the query.

For the following examples, assume two things.

Firstly, we set up an `IndexManager` object, `im`, as follows:

```java
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.query.IndexManager;

File path = getApplicationContext().getDir("datastores");
DatastoreManager manager = new DatastoreManager(path.getAbsolutePath());
Datastore ds = manager.openDatastore("my_datastore");
IndexManager im = new IndexManager(ds);
```

The `IndexManager` object provides the ability to manage query indexes and execute queries.

Secondly, these documents are in the datastore:

```java
{ "name": "mike", 
  "age": 12, 
  "pet": {"species": "cat"} };

{ "name": "mike", 
  "age": 34, 
  "pet": {"species": "dog"} };

{ "name": "fred", 
  "age": 23, 
  "pet": {"species": "cat"} };
```

### Creating indexes

In order to query documents, indexes need to be created over
the fields to be queried against.

Use `ensureIndexed(List<Object> fieldNames, String indexName)` to create indexes. These indexes are persistent across application restarts as they are saved to disk. They are kept up to date documents change; there's no need to call `ensureIndexed(List<Object> fieldNames, String indexName)` each time your applications starts, though there is no harm in doing so.

The first argument to `ensureIndexed(List<Object> fieldNames, String indexName)` is a list of fields to put into this index. The second argument is a name for the index. This is used to delete indexes at a later stage and appears when you list the indexes in the database.

A field can appear in more than one index. The query engine will select an appropriate index to use for a given query. However, the more indexes you have, the more disk space they will use and the greater overhead in keeping them up to date.

To index values in sub-documents, use _dotted notation_. This notation puts the field names in the path to a particular value into a single string, separated by dots. Therefore, to index the `species` field of the `pet` sub-document in the examples above, use `pet.species`.

```java
// Create an index over the name, age, and species fields.
String name = im.ensureIndexed(Arrays.<Object>asList("name", "age", "pet.species"), 
                               "basic");
if (name == null) {
    // there was an error creating the index
}
```

`ensureIndexed(List<Object> fieldNames, String indexName)` returns the name of the index if it is successful, otherwise it returns `null`.

If an index needs to be changed, first delete the existing index by calling `deleteIndexNamed(String indexName)` where the argument is the index name, then call  `ensureIndexed(List<Object> fieldNames, String indexName)` with the new definition.

#### Indexing document metadata (_id and _rev)

The document ID and revision ID are automatically indexed under `_id` and `_rev` 
respectively. If you need to query on document ID or document revision ID,
use these field names.

#### Indexing array fields

Indexing of array fields is supported. See "Array fields" below for the indexing and
querying semantics.

### Querying syntax

Query documents using `Map` objects. These use the [Cloudant Query `selector`][sel]
syntax. Several features of Cloudant Query are not yet supported in this implementation.
See below for more details.

[sel]: https://docs.cloudant.com/api/cloudant-query.html#selector-syntax

#### Equality and comparisons

To query for all documents where `pet.species` is `cat`:

```java
// query: { "pet.species": "cat" }
Map<String, Object> query = new HashMap<String, Object>();
query.put("pet.species", "cat");
```

If you don't specify a condition for the clause, equality (`$eq`) is used. To use other conditions, supply them explicitly in the clause.

To query for documents where `age` is greater than twelve use the `$gt` condition:

```java
// query: { "age": { "$gt": 12 } }
Map<String, Object> query = new HashMap<String, Object>();
Map<String, Object> gt12 = new HashMap<String, Object>();
gt12.put("$gt", 12);
query.put("age", gt12);
```

See below for supported operators (Selections -> Conditions).

#### Compound queries

Compound queries allow selection of documents based on more than one criteria.  If you specify several clauses, they are implicitly joined by AND.

To find all people named `fred` with a `cat` use:

```java
// query: { "name": "fred", "pet.species": "cat" }
Map<String, Object> query = new HashMap<String, Object>();
query.put("name", "fred");
query.put("pet.species", "cat");
```

##### Using OR to join clauses

Use `$or` to find documents where just one of the clauses match.

To find all people with a `dog` who are under thirty:

```java
// query: { "$or": [ { "pet.species": { "$eq": "dog" } }, 
//                   { "age": { "$lt": 30 } }
//                 ]}
Map<String, Object> query = new HashMap<String, Object>();
Map<String, Object> petClause = new HashMap<String, Object>();
Map<String, Object> eqDog = new HashMap<String, Object>();
eqDog.put("$eq", "dog");
petClause.put("pet.species", eqDog);
Map<String, Object> ageClause = new HashMap<String, Object>();
Map<String, Object> lt30 = new HashMap<String, Object>();
lt30.put("$lt", 30);
ageClause.put("age", lt30);
query.put("$or", Arrays.<Object>asList(eqDog, lt30));
```

#### Using AND and OR in queries

Using a combination of AND and OR allows the specification of complex queries.

This selects documents where _either_ the person has a pet `dog` _or_ they are
both over thirty _and_ named `mike`:

```java
// query: { "$or": [ { "pet.species": { "$eq": "dog" } }, 
//                   { "$and": [ { "age": { "$gt": 30 } },
//                               { "name": { "$eq": "mike" } }
//                             ] }
//                 ]}
Map<String, Object> query = new HashMap<String, Object>();
Map<String, Object> petClause = new HashMap<String, Object>();
Map<String, Object> eqDog = new HashMap<String, Object>();
eqDog.put("$eq", "dog");
petClause.put("pet.species", eqDog);
Map<String, Object> andClause = new HashMap<String, Object>();
Map<String, Object> ageClause = new HashMap<String, Object>();
Map<String, Object> nameClause = new HashMap<String, Object>();
Map<String, Object> gt30 = new HashMap<String, Object>();
gt30.put("$gt", 30);
ageClause.put("age", gt30);
Map<String, Object> eqMike = new HashMap<String, Object>();
eqMike.put("$eq", "mike");
nameClause.put("name", eqMike);
andClause.put("$and", Arrays.<Object>asList(ageClause, nameClause));
query.put("$or", Arrays.<Object>asList(petClause, andClause));
```

### Executing queries

To find documents matching a query, use the `IndexManager` object's `find(Map<String, Object> query)` method. This returns an object that can be used in `for ( : )` loops to enumerate over the results.

```java
QueryResult result = im.find(query);
for (DocumentRevision rev : result) {
    // The returned revision object contains all fields for
    // the object. You cannot project certain fields in the
    // current implementation.
}
```

There is an extended version of the `find` method which supports:

- Sorting results.
- Projecting fields from documents rather than returning whole documents.
- Skipping results.
- Limiting the number of results returned.

For any of these, use 

```java
find(Map<String, Object> query,
     long skip,
     long limit,
     List<String> fields,
     List<Map<String, String>> sortDocument)
```

#### Sorting

Provide a sort document to the extended version of the `find` method to sort the results of a query. 

The sort document is a list of fields to sort by. Each field is represented by a map specifying the name of the field to sort by and the direction to sort.

The sort document must use fields from a single index.

As yet, you can't leave out the sort direction. The sort direction can be `asc` (ascending) or `desc` (descending).

```java
// sort document: [ { "name": "asc" },
//                  { "age": "desc" } ]
List<Map<String, String>> sortDocument = new ArrayList<Map<String, String>>();
Map<String, String> sortByName = new HashMap<String, String>();
Map<String, String> sortByAge = new HashMap<String, String>();
sortByName.put("name", "asc");
sortByAge.put("age", "desc");
sortDocument.add(sortName);
sortDocument.add(sortAge);
QueryResult queryResult = im.find(query, 0, 0, null, sortDocument);
```

Pass `null` as the `sort` argument to disable sorting.

#### Projecting fields

Projecting fields is useful when you have a large document and only need to use a
subset of the fields for a given view.

To project certain fields from the documents included in the results, pass a list of field names to the `fields` argument. These field names:

- Must be top level fields in the document.
- Cannot use dotted notation to access sub-documents.

For example, in the following document the `name`, `age` and `pet` fields could be projected, but the `species` field inside `pet` cannot:

```json
{
    "name": "mike",
    "age": 12,
    "pet": { "species": "cat" }
}
```

To project the `name` and `age` fields of the above document:

```java
List<String> fields = Arrays.asList("name", "age");
QueryResult queryResult = im.find(query, 0, 0, fields, null);
```

Pass `null` as the `fields` argument to disable projection.

#### Skip and limit

Skip and limit allow retrieving subsets of the results. Amongst other things, this is useful in pagination.

* `skip` skips over a number of results from the result set.
* `limit` defines the maximum number of results to return for the query.

To display the twenty-first to thirtieth results:

```java
QueryResult result = im.find(query, 20, 10, fields, null);
```

To disable:

- `skip`, pass `0` as the `skip` argument.
- `limit`, pass `0` as the `limit` argument.

### Array fields

Indexing and querying over array fields is supported in Cloudant Query Android, with some caveats.

Take this document as an example:

```
{
  _id: mike32
  pet: [ cat, dog, parrot ],
  name: mike,
  age: 32
}
```

You can create an index over the `pet` field:

```java
String name = im.ensureIndexed(Arrays.<Object>asList("name", "age", "pet"), 
                               "basic");
```

Each value of the array is treated as a separate entry in the index. This means that a query such as:

```
{ pet: { $eq: cat } }
```

Will return the document `mike32`. Negation may be slightly confusing:

```
{ pet: { $not: { $eq: cat } } }
```

Will also return `mike32` because there are values in the array that are not `cat`. That is,
this operator is matching for "any values that do not equal 'cat'".

#### Restrictions

Only one field in a given index may be an array. This is because each entry in each array
requires an entry in the index, causing a Cartesian explosion in index size. Taking the
above example, this document wouldn't be indexed because the `name` and `pet` fields are
both indexed in a single index:


```
{
  _id: mike32
  pet: [ cat, dog, parrot ],
  name: [ mike, rhodes ],
  age: 32
}
```

If this happens, an error will be emitted into the log but the indexing process will be
successful.

However, if there was one index with `pet` in and another with `name` in, like this:

```java
String indexOne = im.ensureIndexed(Arrays.<Object>asList("name", "age"), 
                                   "index_one");
String indexTwo = im.ensureIndexed(Arrays.<Object>asList("age", "pet"),
                                   "index_two");
```

The document _would_ be indexed in both of these indexes: each index only contains one of
the array fields.

Also see "Unsupported features", below.


### Errors

Error reporting is somewhat lacking right now. Presently a `null` return value from the `find` methods or the `ensureIndexed(List<Object> fieldNames, String indexName)` method indicates that something went wrong. Any errors that are encountered are logged but exceptions are not thrown as of yet.

## Supported Cloudant Query features

Right now the list of supported features is:

- Create compound indexes using dotted notation that index JSON fields.
- Delete index by name.
- Execute nested queries.
- Limiting returned results.
- Skipping results.
- Queries can include unindexed fields.
      
Selectors -> combination

- `$and`
- `$or`

Selectors -> Conditions -> Equalities

- `$lt`
- `$lte`
- `$eq`
- `$gte`
- `$gt`
- `$ne`

Selectors -> combination

- `$not`

Selectors -> Condition -> Objects

- `$exists`

Implicit operators

- Implicit `$and`.
- Implicit `$eq`.

Arrays

- Indexing individual values in an array.
- Querying for individual values in an array.

## Unsupported Cloudant Query features

As this is an early version of Query on this platform, some features are
not supported yet. We're actively working to support features -- check
the commit log :)

### Query

Overall restrictions:

- Cannot use covering indexes with projection (`fields`) to avoid loading 
  documents from the datastore.

#### Query syntax

- Using non-dotted notation to query sub-documents.
    - That is, `{"pet": { "species": {"$eq": "cat"} } }` is unsupported,
      you must use `{"pet.species": {"$eq": "cat"}}`.
- Cannot use multiple conditions in a single clause, `{ field: { $gt: 7, $lt: 14 } }`.

Selectors -> combination

- `$nor` #10
- `$all` (unplanned)
- `$elemMatch` (unplanned, waiting on arrays support for query)

Selectors -> Condition -> Objects

- `$type` (unplanned)

Selectors -> Condition -> Array

- `$in` (waiting on arrays support)
- `$nin` (waiting on arrays support)
- `$size` (waiting on arrays support)

Selectors -> Condition -> Misc

- `$mod` (unplanned, waiting on filtering)
- `$regex` (unplanned, waiting on filtering)


Arrays

- Dotted notation to index or query sub-documents in arrays.
- Querying for exact array match, `{ field: [ 1, 3, 7 ] }`.
- Querying to match a specific array element using dotted notation, `{ field.0: 1 }`.
- Querying using `$all`.
- Querying using `$elemMatch`.


## Performance

### Indexing

Not carried out yet.


## Grammar

To help, I've tried to write a grammar/schema for the Query language.

Here:

* Bold is used for the JSON formatting (or to indicate the representation of the use of Map, List etc. in Java).
* Italic is variables in the grammar-like thing.
* Quotes enclose literal string values.

<pre>
<em>query</em> := 
    <strong>{ }</strong>
    <strong>{</strong> <em>many-expressions</em> <strong>}</strong>

<em>many-expressions</em> := <em>expression</em> (&quot;,&quot; <em>expression</em>)*

<em>expression</em> := 
    <em>compound-expression</em>
    <em>comparison-expression</em>

<em>compound-expression</em> := 
    <strong>{</strong> (&quot;$and&quot; | &quot;$nor&quot; | &quot;$or&quot;) <strong>:</strong> <strong>[</strong> <em>many-expressions</em> <strong>] }</strong>  // nor not implemented
    
<em>comparison-expression</em> :=
    <strong>{</strong> <em>field</em> <strong>:</strong> <strong>{</strong> <em>operator-expression</em> <strong>} }</strong>

<em>negation-expression</em> := 
    <strong>{</strong> &quot;$not&quot; <strong>:</strong> <strong>{</strong> <em>operator-expression</em> <strong>} }</strong>

<em>operator-expression</em> := 
    <em>negation-expression</em>
    <strong>{</strong> <em>operator</em> <strong>:</strong> <em>simple-value</em> <strong>}</strong>
    <strong>{</strong> &quot;$regex&quot; <strong>:</strong> <em>Pattern</em> <strong>}</strong>  // not implemented
    <strong>{</strong> &quot;$mod&quot; <strong>:</strong> <strong>[</strong> <em>divisor, remainder</em> <strong>] }</strong>  // not implemented
    <strong>{</strong> &quot;$elemMatch&quot; <strong>: {</strong> <em>many-expressions</em> <strong>} }</strong>  // not implemented
    <strong>{</strong> &quot;$size&quot; <strong>:</strong> <em>positive-integer</em> <strong>}</strong>  // not implemented
    <strong>{</strong> &quot;$all&quot; <strong>:</strong> <em>array-value</em> <strong>}</strong>  // not implemented
    <strong>{</strong> &quot;$in&quot; <strong>:</strong> <em>array-value</em> <strong>}</strong>  // not implemented
    <strong>{</strong> &quot;$nin&quot; <strong>:</strong> <em>array-value</em> <strong>}</strong>  // not implemented
    <strong>{</strong> &quot;$exists&quot; <strong>:</strong> <em>boolean</em> <strong>}</strong>
    <strong>{</strong> &quot;$type&quot; <strong>:</strong> <em>type</em> <strong>}</strong>  // not implemented

<em>operator</em> := &quot;$gt&quot; | &quot;$gte&quot; | &quot;$lt&quot; | &quot;$lte&quot; | &quot;$eq&quot; | &quot;$neq&quot;

// Obviously List, but easier to express like this
<em>array-value</em> := <strong>[</strong> simple-value (&quot;,&quot; simple-value)+ <strong>]</strong>

// Java mappings of basic types

<em>field</em> := <em>String</em>  // a field name

<em>simple-value</em> := <em>String</em> | <em>Number</em>

<em>positive-integer</em> := <em>Integer</em>

<em>boolean</em> := <em>Boolean</em>

<em>type</em> := <em>Class</em>
</pre>
