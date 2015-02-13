## Finding documents within Cloudant Sync

**_The functionality detailed below is being replaced by the new Cloudant Query - Android code base.  Please consult the Cloudant Query - Android [documentation][1] as it now supersedes this document._**

[1]: https://github.com/cloudant/sync-android/blob/master/doc/query.md

_This functionality is available in versions 0.3.0 and up._

Cloudant Sync provides simple and powerful ways to index and query
your documents, allowing your applications to make the most of the
data they store.

For those familiar with Cloudant, these indexes are closer to Cloudant's
Search feature than its Views feature. This is because they allow 
you to define indexes and execute ad hoc queries across those indexes.
It's important to note, however, that the indexing is _not_ based on
Lucene so lacks powerful full text search (though we're looking into 
that!).

Currently indexing and querying operate on full terms. That means that
if you index "Cloudant Sync", your query must be on "Cloudant Sync", that
is a precise match. We're working on improving this for free text
search scenarios and wildcard matching.

### Indexing

A datastore can have several indexes defined on it. Each index stores
a particular set of values for each document in the datastore and
allows fast look up of document IDs by those values (via a query).

The values that an index contains are specified by passing each document
through an _indexing function_, which take a document and return an
array of values. The returned array's values are indexed verbatim. Cloudant
Sync provides a number of prebuilt indexing functions or you can 
define your own, leading to powerful ways to index documents.

An index is typed to aid in querying. Currently there are two types:

- String, which allows queries for exactly matching values.
- Integer, which allows queries for matching values and values within a range.

#### Defining an index

The `FieldIndexFunction` class allows indexing a document by a top-level
field (those existing at the root of the JSON document).

We'll use this document as our example:

```json
{
    "firstname": "John",
    "lastname": "Doe", 
    "age": 29
}
```

Indexes for a datastore are managed by the `IndexManager` class. This
class allows creating, deleting and modifying indexes. You'd normally
create a single IndexManager object for each datastore. The `IndexManager`
class is also used for queries. It's simple to create:

```java
IndexManager indexManager = new IndexManager(datastore);
```

To create an index on the `firstname` field using the `FieldIndexFunction`,
we define the index using:

```java
indexManager.ensureIndexed("default", "firstname");
```

The `ensureIndexed` method indexes all existing documents in the datastore
before returning, so for datastore with existing documents it may be 
run on a background thread.

The `ensureIndexed` function must be run every time an `IndexManager` is
created so that the manager object recognises that index. The indexes 
themselves are persisted to disk and updated incrementally -- the 
`IndexManager` just needs to be told about them at startup time.

`ensureIndexed(String name, String field)` is a convienience method to
create an index using a `FieldIndexFunction` on the defined field that
is of type `IndexType.STRING`. A longer form is used for using your
own indexing functions, see below.

##### Deleting an Index

To remove the index we just created, ask the manager object to delete
the index:

```java
indexManager.deleteIndex("default")
```

##### Redefining an Index

To redefine an index, you need to delete and recreate the index:

```java
// Before starting, "default" is a field index on "firstname"
indexManager.deleteIndex("default");
indexManager.ensureIndexed("default", "lastname");
```

### Querying

Once one or more indexes have been created, you can query them using
the `IndexManager` object. The process is:

- Use a `QueryBuilder` object to construct a query `Map`.
- Pass the query to `IndexManager#query(Map)`.

Concretely:

```java
QueryBuilder query = new QueryBuilder();

// add a query on the `default` index:
query.index("default").equalTo("John");

// Run the query
QueryResult result = indexManager.query(query.build());
```

A query can use more than one index:

```java
QueryBuilder query = new QueryBuilder();

query.index("default").equalTo("John");
query.index("age").greaterThanOrEqual(26);

// Run the query
QueryResult result = indexManager.query(query.build());
```

The query result can then be iterated over to retrieve the documents:

```java
for (DocumentRevision revision : result) {
    // do something
}
```

The queries currently supported are:

* `lessThanOrEqual(value)`: index <= value 
* `equalTo(value)`: index == value
* `greaterThanOrEqual(value)`: index >= value
* `greaterThanOrEqual(value1).lessThanOrEqual(value2)`: value1 <= index <= value2
* `oneOf([value_0,...,value_n])`: index == value_0 || ... || index == value_n

### Query options

It is possible to specify additional options with the `QueryBuilder` class.
These options affect the results which the query returns.

`sortBy` is used to order the results according to the value of the index given:

```java
QueryBuilder query = new QueryBuilder();

query.index("default").equalTo("John");
query.index("age").greaterThanOrEqual(26);
query.sortBy("age", SortDirection.Descending);

// Run the query
QueryResult result = indexManager.query(query.build());
```

As in the example above, this can be combined with the parameter `SortDirection.Ascending` or
`SortDirection.Descending` to sort ascending or descending. If the direction is not given, then the
default is ascending.

The ordering is determined by the underlying SQL type of the index. 

`offset` and `limit` can be used to page through results, which can be
useful when presenting information in a GUI. In this example we present 10 results at a time:

```java
QueryBuilder query = new QueryBuilder();

query.index("default").equalTo("John");
query.index("age").greaterThanOrEqual(26);

QueryResult result;

int i=0;
int pageSize=10;
do {
    result = indexManager.query(query.offset(i).limit(pageSize).build());
    i+=pageSize;
    // display results
} while (result.documentIds().size() > 0);
```

Note that the current implementation does not use a cursor, so the results are likely to be
incorrect if the data changes during paging.

### Unique Values

Another useful feature for displaying results in a GUI is the `uniqueValues` method.
Suppose each document represents a blog article and we want to display an index showing each
all of the categories for the articles:

```java
List result = indexManager.uniqueValues("category");
```


### Index Functions

As noted above, an index uses an `IndexFunction` instance to map a
document to the values that should be indexed for the document. The
`IndexFunction` interface defines a single function:

```java
public List<Object> indexedValues(String indexName, Map map);
```

For example, the included `FieldIndexFunction` used earlier is
defined as:

```java
public class FieldIndexFunction implements IndexFunction<Object> {

    private String fieldName;

    public FieldIndexFunction(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public List<Object> indexedValues(String indexName, Map map) {
        if (map.containsKey(this.fieldName)) {
            Object value = map.get(this.fieldName);
            return Arrays.asList(value);
        }
        return null;
    }
}
```

The longer form of the `ensureIndexed` function allows you to provide
your own index function:

```java
public void ensureIndexed(String indexName, IndexType type, IndexFunction indexFunction)
            throws IndexExistsException
```

For example, to use this long form to define the field index on `firstname`
used earlier:

```java
FieldIndexFunction f = new FieldIndexFunction("firstname");
indexManager.ensureIndexed("default", IndexType.STRING, f);
```

As before, `ensureIndexed` must be called each time the `IndexManager` object
is created, but of course the indexed values are persisted to disk, also
as you'd expect.

### Extended example

This example uses Cloudant Sync's indexing function to display
collection of documents, in this case songs from particular albums.

Assume all the songs are in the following format:

```json
{
    "name": "Life in Technicolor",
    "album": "Viva la Vida",
    "artist": "Coldplay",
    ...
}

{
    "name": "Viva la Vida",
    "album": "Viva la Vida",
    "artist": "Coldplay",
    ...
}


{
    "name": "Square One",
    "album": "X&Y",
    "artist": "Coldplay",
    ...
}

{
    "name": "What If",
    "album": "X&Y",
    "artist": "Coldplay",
    ...
}

```

First build the indexes on "album" and "artist" using the `FieldIndexFunction`:


```java
IndexManager indexManager = new IndexManager(datastore);
Index albumIdx = indexManager.ensureIndexed("album", "album")
Index artistIdx = indexManager.ensureIndexed("artist", "artist")
```

Then you can get the songs from Viva la Vida:

```java
Map query = new QueryBuilder()
    .index("artist").equalTo("Coldplay")
    .index("album").equalTo("Viva la Vida")
    .build();
QueryResult result = indexManager.query(query);
for (DocumentRevision revision : result) {
    System.out.println(revision.asMap["name"]);
}

// prints:
//   Life in Technicolor
//   Viva la Vida
```

Note that `FieldIndexFunction` doesn't transform the values, so queries
need to use the exact term and case (e.g., you can't use "coldplay" or
"cold").
