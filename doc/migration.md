# Datastore -> DocumentStore

The DatastoreManager class has been removed - TODO example of how to get an instance vs previously

"CRUD" (create, read, update, delete) functionality has been migrated to the new `Database` class. To ...

* `getDocument` has been renamed `read`
* `getAllDocuments` has been renamed `read`
* `getDocumentsWithIds` has been renamed `read`
* `containsDocument` has been renamed `contains`
* `getAllDocumentIds` has been renamed `getIds`
* `getConflictedDocumentIds` has been renamed `getConflictedIds`
* `resolveConflictsForDocument` has been renamed `resolveConflicts`
* `createDocumentFromRevision` has been renamed `create`
* `updateDocumentFromRevision` has been renamed `update`
* `deleteDocumentFromRevision` has been renamed `delete`
* `deleteDocument` has been renamed `delete`

The `throws` clauses on `Database` methods are different to those on their counterparts in `Datastore`. Code which calls these methods may have to be adjusted to catch different exceptions.

# Notifications/Events - namespaces
com.cloudant.sync.notifications -> com.cloudant.sync.event.notifications

# IndexManager -> Query

The `ensureIndexed` methods have been replaced by `createJsonIndex` and `createTextIndex`

The first argument to createJsonIndex and createTextIndex is of type `List<FieldSort>`.

To specify the fields "name" and "age", replace instances of

```java
Arrays.<Object>asList("name", "age")
```

with

```java
Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age"))
```

The second argument to `createJsonIndex` and `createTextIndex` is the index name. This is required; to automatically generate the index name, replace instances of `ensureIndexed(fields)` with `createJsonIndex(fields, null)` or `createTextIndex(fields, null)`.

The FTS tokenizer for text indexes is specified via the new `Tokenizer` class. To use a non-default tokenizer, replace instances of

```java
Map<String, String> settings = new HashMap<String, String>();		
settings.put("tokenize", "porter");		
String indexName = im.ensureIndexed(Arrays.<Object>asList(
        "name", "age"),		
    "textIndex",		
    IndexType.TEXT,		
    settings);
```

with

```java
String indexName = im.createTextIndex(Arrays.<FieldSort>asList(
        new FieldSort("name"), 
        new FieldSort("age")),
    "textIndex",
    new Tokenizer("porter")).indexName;
```

`createJsonIndex` and `createTextIndex` now return the full `Index` object. To obtain the index name, use the `indexName` property of the returned index.

`updateAllIndexes` has been renamed `updateAllIndexes`.

Methods on the `Query` interface throw checked exceptions rather than throwing `null` to indicate an error condition. To check for error conditions, enclose your existing code in `try/catch` block instead of checking the return value for `null`.

The `close` method has been removed. The native resources used by the indexes database are released when the owning `DocumentStore` has `close` called on it.

Existing index selection algorithm changed? TODO

# namespaces - internal

# javadoc - don't show internal


