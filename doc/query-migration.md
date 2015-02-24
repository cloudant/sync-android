# Migrate from legacy indexing to Cloudant Query

Our original indexing-query solution has now been replaced by [Cloudant Query - Android][1].  The content within details the differences between the two query implementations and how an existing user of the legacy indexing-query solution can migrate to using the new Cloudant Query implementation.

[1]: https://github.com/cloudant/sync-android/blob/master/doc/query.md

## Differences

For users of the legacy indexing-query solution it is first important to understand the differences between the new and old versions of Query in order to understand the proper path to take with regards to migration.

### Index Management

- It is no longer necessary to invoke the `ensureIndexed` method each time a new instance of the `IndexManager` is created.  Although there is no harm in doing so.  Indexes now are persistent across application restarts.  So they need to only be created one time in order to be used by queries.
- The signature of the `ensureIndexed` method has also changed somewhat, in that now to create an index on a field or fields you would simply pass a `List` of field(s) to the method rather than an individual field name.
- The notion of index functions no longer exists.
- Another minor difference but one worth noting is that the `ensureIndexed` argument order has been flipped.  So now field names are the first argument and index name is the second.
- The processes for deleting and re-defining an index are largely unchanged.

So in the past for indexes created like this:

```
import com.cloudant.sync.indexing.IndexManager;
...
IndexManager indexManager = new IndexManager(datastore);
indexManager.ensureIndexed("default", "name");
indexManager.ensureIndexed("age", "age");
```

You could now create a single index like this:

```
import com.cloudant.sync.query.IndexManager;
...
IndexManager indexManager = new IndexManager(datastore);
indexManager.ensureIndexed(Arrays.<Object>asList("name", "age"), "default");
```

Creating multiple indexes is obviously still possible.

### Querying

We have taken the lead from both [Cloudant Query][2] and [MongoDB Query][3] with regards to how we handle querying in this mobile implementation of Cloudant Query.

- The `QueryBuilder` no longer exists.  Instead you now construct your query programmatically as a `Map<String, Object>` where each map value `Object` can be another `Map<String, Object>`, or a `List<Object>` or simply a `String`, `Number`, or even `Boolean` in some cases.
- Operators such as `$eq`, `$lt`, `$gt`, etc. are now used to define conditions within the query as you would in [Cloudant Query][2] or [MongoDB Query][3].
- When writing your query it is no longer necessary to specify the index(es) that the query must use to process its results.  The new Query engine will find the appropriate index(es) for your query if they exist.  In the case where index(es) do not exist the Query engine will process the results programmatically albeit in a much slower manner.

So in the past for a query defined like this:

```
import com.cloudant.sync.indexing.QueryBuilder;
...
QueryBuilder query = new QueryBuilder();

query.index("default").equalTo("mike");
query.index("age").greaterThanOrEqual(34);
```

You would now define it like this:

```
Map<String, Object> query = new HashMap<String, Object>() {
    { put("name", "mike");
      put("age", new HashMap<String, Object>() { { put("$gte", 34); } }); }
};
```

- Query execution is now handled through the `find` method much in the same way that it is handled in [Cloudant Query][2] and [MongoDB Query][3].
- Iterating over documents returned as part of a query result is largely unchanged.

So executing a query changes from:

```
import com.cloudant.sync.indexing.QueryResult;
...
QueryResult result = indexManager.query(query.build());
```

To:

```
import com.cloudant.sync.query.QueryResult;
...
QueryResult result = indexManager.find(query);
```

- Query options like `offset`, `limit`, and `sort` are now handled by the overloaded `find` method of the IndexManager.  `offset` is now known as `skip` and a new query option to perform field projection is now also available.

[2]: https://docs.cloudant.com/api.html#cloudant-query
[3]: http://docs.mongodb.org/manual/tutorial/query-documents/

## Migration Path

Given the differences detailed above, your migration path should be as follows:

1. Consult the [Cloudant Query - Android][1] documentation to get familiar with the new implementation and all of the supported operators.
2. Change all `com.cloudant.sync.indexing.*` import statements to `com.cloudant.sync.query.*` import statements.
3. Refactor your [index creation][4] code to use the new IndexManager `ensureIndexed` method signature.
4. Update your [query construction][5] code to build queries by populating Maps directly.
5. Modify your execution code to use the `find` method to execute your query.
6. If necessary, refactor code that uses old query options like `offset`, `limit` and `sort` to use the new IndexManager overloaded `find` method with the following signature:

```
QueryResult find(Map<String, Object> query,
                 long skip,
                 long limit,
                 List<String> fields,
                 List<Map<String, String>> sortDocument)
```

[4]: https://github.com/cloudant/sync-android/blob/master/doc/query.md#creating-indexes
[5]: https://github.com/cloudant/sync-android/blob/master/doc/query.md#querying-syntax
