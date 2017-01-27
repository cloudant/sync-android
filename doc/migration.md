# Version 2.0 API Migration Guide

There are breaking changes in version 2.0 of the library. This
document summarises the changes that users of the library will need to
make in order to migrate their existing code which uses version 1.x of
the library.

The focus is on practical suggestions with code samples. See also
the [the CHANGES document](../CHANGES.md) which gives a summary of all
fixes and new features in each version.

## Package changes

Classes in the library have been re-organised to simplify the packaging.

All classes are now in the `com.cloudant.sync` package:

* All private API classes are in the `com.cloudant.sync.internal`
  package. API users should not use these classes as fields, method
  signatures, and implementation details may be subject to
  change. Directly using the classes in these packages or calling
  their methods is not supported. These classes have the same status
  as those marked "API Status: Private" in the javadoc for the 1.x
  versions of the library.
* Everything else under `com.cloudant.sync` is public API and subject
  to the usual versioning and deprecation practices. API users can
  expect this to be a stable API.
* Classes in the `com.cloudant.sync.internal` package are not visible
  in javadoc.

API users will have to adjust their `import` statements
appropriately. Furthermore, some class names and methods have also
changed (see below).

## The `Datastore` has been replaced with the `DocumentStore` class

The `DatastoreManager` class has been removed. Replace instances of

```java
File path = getApplicationContext().getDir("datastores"); // Android-specific
DatastoreManager manager = DatastoreManager.getInstance(path.getAbsolutePath());
Datastore ds = manager.openDatastore("my_datastore");
// read a doc
DocumentRevision dr = ds.getDocument("my-document-id");
// call close to release resources
ds.close();
```

with

```java
File path = getApplicationContext().getDir("datastores"); // Android-specific
DocumentStore ds = DocumentStore.getInstance(new File(path, "my_datastore"));
// read a doc
DocumentRevision dr = ds.database().read("my-document-id");
// call close to release resources
ds.close();
```

The `getInstance` method will try to create all necessary
sub-directories in order to construct the path represented by the
`File` argument. This differs from the behaviour of the 1.x versions
of the library which would only attempt to create one level of
directories.

As the above example shows, "CRUD" (create, read, update, delete)
functionality has been migrated to the new `Database` class. Obtain an
instance of the `Database` class managed by the `DocumentStore` by calling
the `database()` getter method.

The method names on the `Database` class are different to their
counterparts on `Datastore`:

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

The `getConflictedIds` method now returns `Iterable<String>`. This
allows the result to be used in an enhanced `for` loop.

The `throws` clauses on `Database` methods are different to those on
their counterparts in `Datastore`. Code which calls these methods may
have to be adjusted to catch different exceptions. See
also [this section on exceptions](#changes-to-exceptions) for more
details on other changes to exception handling.

## The `IndexManager` class has been replaced with the `Query` class

To obtain a reference to the `Query` object, replace instances of

```java
Datastore ds; // Datastore instance previously obtained
IndexManager im = new IndexManager(ds);
// create an index
String name = im.ensureIndexed(Arrays.<Object>asList(
        "name", "age", "pet.species"),
    "basic");
// perform queries etc
// ...
// call close to release resources
im.close();
ds.close();
```

with

```java
DocumentStore ds; // DocumentStore instance previously obtained
// create an index
Index i = ds.query().createJsonIndex(Arrays.<FieldSort>asList(
        new FieldSort("name"),
        new FieldSort("age"),
        new FieldSort("pet.species")),
    "basic");
// perform queries etc
// ...
// call close to release resources
ds.close();
```

The `ensureIndexed` methods have been replaced by `createJsonIndex`
and `createTextIndex`.

The first argument to `createJsonIndex` and `createTextIndex` is of type
`List<FieldSort>`.

To specify the fields "name" and "age", replace instances of

```java
Arrays.<Object>asList("name", "age")
```

with

```java
Arrays.<FieldSort>asList(new FieldSort("name"), new FieldSort("age"))
```

The second argument to `createJsonIndex` and `createTextIndex` is the
index name. This is required; to automatically generate the index
name, replace instances of `ensureIndexed(fields)` with
`createJsonIndex(fields, null)` or `createTextIndex(fields, null)`.

The FTS tokenizer for text indexes is specified via the new
`Tokenizer` class. To use a non-default tokenizer, replace instances
of

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

`createJsonIndex` and `createTextIndex` now return the full `Index`
object. To obtain the index name, use the `indexName` property of the
returned index.

`updateAllIndexes` has been renamed `refreshAllIndexes`.

Methods on the `Query` interface throw checked exceptions rather than
returning `null` to indicate an error condition. To check for error
conditions, enclose your existing code in a `try/catch` block instead of
checking the return value for `null`. See
also [this section on exceptions](#changes-to-exceptions) for more
details on other changes to exception handling.

`createJsonIndex` and `createTextIndex` will return an existing
equivalent index if it exists, regardless of the requested index
name. See the javadoc for these methods for more details.

The `close` method has been removed. The native resources used by the
indexes database are released when the owning `DocumentStore` has
`close` called on it.

## Changes to the Notifications and Events packages

Events which were previously in the `com.cloudant.sync.notifications`
package have moved to the `com.cloudant.sync.event.notifications`
package.

All events now implement the `Notification` marker interface (this
means it is possible to subscribe to all events with one method, if
required).

Some event names have changed:

* `DatabaseClosed` has been renamed to `DocumentStoreClosed`
* `DatabaseCreated` has been renamed to `DocumentStoreCreated`
* `DatabaseDeleted` has been renamed to `DocumentStoreDeleted`
* `DatabaseModified` has been renamed to `DocumentStoreModified`
* `DatabaseOpened` has been renamed to `DocumentStoreOpened`

## Changes to the Replicator and `ReplicatorBuilder`

The `batchLimitPerRun` property has been removed from the Pull and
Push replicator builders. There is no limit to the number of batches
in a replicator run - the replicator will run to completion unless an
error occurs.

## Changes to Exceptions

Almost all API methods will throw checked exceptions instead of
runtime exceptions. The `throws` clause in the javadoc for each method
specifies what exceptions are thrown and under what
circumstances. Additionally there is javadoc documentation for all of
the custom exceptions.

This differs from the behaviour of the 1.x versions of the library
which were much more likely to throw runtime exceptions which the
developer could not anticipate.

Situations for which runtime (unchecked) exceptions may be thrown include:

* Incorrect or `null` arguments passed to an API method: in these
  cases `NullPointerException` or `IllegalArgumentException` can be
  thrown. The developer can defend against these by ensuring that
  correct and non-`null` arguments are passed to these methods. The
  documentation will provide guidance as to what form arguments should
  take and when it is valid for them to be `null`.

* Situations which are hard to anticipate and/or recover from. This
  covers events like out of memory or out of disk space.

## Changes to Replication Policies

The `IntervalTimerReplicationPolicyManager` was moved into the `cloudant-sync-datastore-javase`
module since it was not suitable for running on Android anyway.

## Changes to `Changes`

Removed `size()` and `getIds()` methods. The behaviour duplicated what was
already available on the `List` returned from `getResults()`.

Before:
```java
getChanges().size();
getChanges().getIds();
```
After:
```java
getChanges().getResults().size();
// Get a list of IDs using a collector (Java 1.8)
getChanges().getResults().stream().map(DocumentRevision::getId).collect(Collectors.toList());
// Alternatively, for older APIs, just iterate the DocumentRevisions
for (DocumentRevision rev : getChanges().getResults()) {
  // Do something with the ID...
  rev.getId();
}
```
