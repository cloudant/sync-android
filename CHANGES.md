# 2.x.y (Unreleased)
- [IMPROVED] Record checkpoint on empty `_changes` result in pull replications. This change optimizes 
   filtered replications when changes in remote database doesn't match the replication filter. 
- [UPGRADED] Upgraded to version 2.19.0 of the `cloudant-http` library.

# 2.4.0 (2019-01-15)
- [NEW] `Database` methods `read`, `contains`, `create`, and `delete` now accept local
  (non-replicating documents). These documents must have their document ID prefixed with `_local/`
  and must have their revision ID set to `null` (where applicable).
- [FIXED] Fixed purge_seq to return a String object when replicating with CouchDB 2.3 databases. 
     
# 2.3.0 (2018-08-14)
- [NEW] Added API for specifying a list of document IDs in the filtered pull replicator.
- [IMPROVED] Forced a TLS1.2 `SSLSocketFactory` where possible on Android API versions < 20 (it is
  already enabled by default on newer API levels).

# 2.2.0 (2018-02-14)
- [NEW] Added API for specifying a mango selector in the filtered pull replicator
- [IMPROVED] Improved efficiency of sub-query when picking winning
  revisions. This improves performance when inserting revisions,
  including during pull replication.
- [UPGRADED] Upgraded to version 2.12.0 of the `cloudant-http` library.

# 2.1.0 (2017-12-04)
- [NEW] Added API for upcoming IBM Cloud Identity and Access
  Management support for Cloudant on IBM Cloud. Note: IAM API key
  support is not yet enabled in the service.
- [IMPROVED] Updated documentation by replacing deprecated links with the latest Bluemix or CouchDB links.
- [IMPROVED] Added `seq_interval` to improve `Changes` API throughput when replicating from
             a CouchDB 2.x endpoint.
- [UPGRADED] Upgraded to version 2.11.0 of the `cloudant-http` library.

# 2.0.2 (2017-06-20)
- [FIXED] Removed cloudant-sync-datastore-android project dependency
  on com.google.android:android. This dependency was inadvertently
  made a run-time dependency where it should have been a build-time
  dependency.

# 2.0.1 (2017-04-26)
- [IMPROVED] Increased the resilience of replication to network failures.
- [FIXED] NPE when accessing indexes created in earlier versions that were
  migrated to version 2.
- [FIXED] Exception fetching attachments on a deleted revision.
- [FIXED] Correctly schedule periodic replications after WiFi connections have
  been lost.

# 2.0.0 (2017-02-07)
- [BREAKING CHANGE] With the release of version 2.0 of the library,
  there are a large number of breaking changes to package names,
  classes and methods. API users will need to make changes to their
  existing code in order to use this version of the library. Consult
  the [migration guide](https://github.com/cloudant/sync-android/blob/2.0.0/doc/migration.md) for a comprehensive list of
  changes and suggested strategies to migrate your code.
- [BREAKING CHANGE] The `name` field has been removed from
  `Attachment`. Additionally, the `name` argument has been removed
  from the `UnsavedStreamAttachment` constructors. The name of the
  attachment is the key used to add or retrieve the attachment to or
  from the attachments map.
- [IMPROVED] `DocumentStore.getInstance()` will try to create all
  necessary sub-directories in order to construct the path represented
  by the `File` argument. This differs from the behaviour of the 1.x
  versions of the library which would only attempt to create one level
  of directories.
- [REMOVED] The `batchLimitPerRun` property has been removed from the
  Pull and Push replicator builders. There is no limit to the number
  of batches in a replicator run - the replicator will run to
  completion unless an error occurs.
- [IMPROVED] Removed limitation on `DocumentStore` names. Under the
  old `Datastore` API, the directory containing the SQLite database
  had to conform to the CouchDB database name restrictions. This no
  longer applies.
- [NOTE] The "CRUD Guide" markdown document (previously located in
  `doc/crud.md`) has been migrated to a
  [java source file](https://github.com/cloudant/sync-android/blob/2.0.0/doc/CrudSamples.java).
- [NEW] `advanced()` getter on `DocumentStore` for specialist advanced use cases.
  Adds support for creating specific document revisions with history.
- [FIXED] Issue with double encoding of restricted URL characters in credentials when using
  `ReplicatorBuilder`.
- [FIXED] Issue where push replicating a large number of attachments
  could exhaust the operating system file handle limit, on some
  platforms.
- [FIXED] Issue querying indexed fields when combining the `$not` and
  `$size` operators.

# 1.1.5 (2016-12-08)
- [FIXED] Issue where replicator would not get the latest revision if `_bulk_get`
   was available.

# 1.1.4 (2016-11-23)
- [FIXED] Issue performing cookie authentication in version 1.1.3.

# 1.1.3 (2016-11-22)
- [FIXED] Incorrect message output from parameter `null` or empty checks.
- [FIXED] Issue where replications would error if the server returned missing revisions.
- [UPGRADED] Upgraded to version 2.7.0 of the `cloudant-http` library.

# 1.1.2 (2016-10-20)
- [FIXED] Issue preventing index updates from being persisted that
  impacted query performance

# 1.1.1 (2016-10-05)

- [UPGRADED] Upgraded to version 2.6.2 of the `cloudant-http` library.
- [REMOVED] Removed com.google.guava:guava:15.0 dependency. Applications may need to update their own
  dependency tree to include guava if they are using it directly, but were relying on this library
  to include it.

# 1.1.0 (2016-08-24)

- [NEW] Updated to version 2.5.1 of the `cloudant-http` library. This
  includes optional support for handling HTTP status code 429 `Too
  Many Requests` with blocking backoff and retries. To enable the
  backoff add an instance of a `Replay429Interceptor` with the desired
  number of retries and initial backoff:
  ```
  builder.addResponseInterceptors(new Replay429Interceptor(retries, initialBackoff));
  ```
  A default instance is available using 3 retries and starting with a
  250 ms backoff: `Replay429Interceptor.WITH_DEFAULTS`
- [DEPRECATED] `DatastoreManager` constructors. Use `DatastoreManager.getInstance` factory methods
  instead to guarantee only a single `DatastoreManager` instance is created for a given storage
  directory path in the scope of the `DatastoreManager` class.
- [FIX] Corrected a case where two root nodes with identical revision IDs prevented selection of the
  correct new winning revision.
- [FIX] Added migration on `Datastore` opening to repair datastores with duplicated revisions or
  attachments caused by an issue when executing concurrent pull replications with the same source and target.
  Note that in rare circumstances for some documents this may result in a different, but corrected,
  winning revision after migration.
- [FIX] Prevent insertion of multiple revisions with the same document
  ID and revision ID. Previously this could occur when executing
  concurrent pull replications with the same source and target. A
  replication encountering this condition will now terminate in the
  `ERROR` state.
- [NOTE] Due to migrations outlined above, the first time an existing
  `Datastore` is opened with this version of the library, users may
  experience a longer than usual delay before the `Datastore` is
  ready.


# 1.0.0 (2016-05-03)
- [NOTE] This library follows the
  [Semantic Versioning 2.0.0 specification](http://semver.org). To
  support this we now explicitly declare which parts of the library
  form the "public API" (see below). Subsequent version numbers will
  reflect the compatibility status of API changes.
- [IMPROVED] Javadoc output now shows "API Status" of "Public" or
  "Private" for each class. See
  [the last paragraph of this section of the README](https://github.com/cloudant/sync-android/blob/1.0.0/README.md#overview-of-the-library)
  for more details.
- [NEW] Added filtering for push replications.
- [BREAKING CHANGE] The `EventBus` APIs have been changed from the Google Guava
  (`com.google.common.eventbus`) to our own `com.cloudant.sync.event` API. The new implementation
  has increased restrictions on visibility of `@Subscribe` annotated methods. See the
  [Event guide](https://github.com/cloudant/sync-android/blob/1.0.0/doc/events.md) and javadoc for
  details.
- [BREAKING CHANGE] The `BasicDocumentRevision` and
  `MutableDocumentRevision` classes have been removed, in order to
  simplify use of API methods. All code using this library will need
  to be updated to use the `DocumentRevision` class. See the updated
  [CRUD guide](https://github.com/cloudant/sync-android/blob/1.0.0/doc/crud.md)
  for examples of how to use this class.
- [BREAKING CHANGE] Index type is now defined as an enum. This affects
 the following APIs:
  - `Index#getInstance`
  - `IndexManger#ensureIndexed`
- [BREAKING CHANGE] Creation of replication policies made easier where credentials for replicators
need to be retrieved asynchronously.
- [BREAKING CHANGE] The `DatastoreExtended` interface has been removed.
- [BREAKING CHANGE] Some internal classes have been renamed. Code
  relying on these classes will no longer compile.

# 0.15.5 (2016-02-25)
- [FIXED] Issue where `java.lang.RuntimeException: Offer timed out` could be thrown 5 minutes after
  a replication error.

# 0.15.4 (2016-02-22)
- [IMPROVED] Optimise pull replication performance. This is achieved
  by reducing excessive database traffic and batching up insertions in
  one SQL transaction.
- [FIXED] Issue where `java.lang.RuntimeException: Offer timed out` could be thrown 5 minutes after
  stopping a replication.

# 0.15.3 (2016-02-11)
- [REVERT] Revert replication optimisations which caused updated revisions to be
  inserted as new documents

# 0.15.2 (2016-02-04)
- [IMPROVED] Optimise pull replication performance. This is achieved
  by reducing excessive database traffic and batching up insertions in
  one SQL transaction.

# 0.15.1 (2016-01-25)
- [IMPROVED] Use system `http` `keepalive` default value. This was
  previously set to `false`. In most circumstances, and especially
  with `https` connections, this will improve replication time. The
  default value can be over-ridden by setting the property before
  making any replication requests, eg:
  `System.setProperty("http.keepAlive", "false");`
- [IMPROVED] Increase default `insertBatchSize` values for
  replication. Pull replications from databases which support the
  `_bulk_get` endpoint will see an improvement in performance.

# 0.15.0 (2016-01-08)
- [BREAKING CHANGE] Removed the following classes:

  - PushConfiguration
  - PullConfiguration
  - PushReplication
  - PullReplication
  - Replication
  - ReplicatorFactory

  These removals simplify configuration of replicators. This
  configuration functionality has been migrated to the
  `ReplicatorBuilder` class.

# 0.14.0 (2015-11-3)
- [BREAKING CHANGE] Removed `setCustomHeaders` from `CouchConfig`. See
  [Http Interceptors](https://github.com/cloudant/sync-android/blob/0.14.0/doc/interceptors.md)
  for a code sample which shows how to add custom request headers
  using an HTTP Request Interceptor.
- [NEW] Added replication policies, allowing users to easily create policies such as "Replicate
   every 2 hours, only when on Wifi". See the [Replication Policies User Guide](https://github.com/cloudant/sync-android/blob/0.14.0/REPLICATION_POLICIES.md).
- [IMPROVED] Replication reliability when transferring data over unreliable
  networks.

# 0.13.4 (2015-09-29)
- [FIXED] Issue where HTTP Interceptors would not be executed for `_revs_diff`
requests.


# 0.13.3 (2015-08-23)
- [FIXED] Issue where the `ReplicatorBuilder` would not handle HTTP interceptors
  correctly for push replications.

# 0.13.2 (2015-08-28)
- [FIXED] Issue where document IDs containing colons were not properly encoded during replication

# 0.13.1 (2015-08-17)
- [FIXED] Fix issue where the `ReplictorBuilder` would crash the application
when creating a pull replication if `PullFilter` is `null`.

# 0.13.0 (2015-08-13)
- [BREAKING CHANGE] Moved code for encryption on Android to the
  `sync-android-encryption` subproject. See the instructions in the [README](https://github.com/cloudant/sync-android/blob/0.13.0/README.md)
  file for how to include `sync-android-encryption` in your project.
- [BREAKING CHANGE] Removed Configuration options from `CouchConfig` which are not used
   by the HTTP Layer. See commit [815026c](https://github.com/cloudant/sync-android/commit/815026c14caf86a8cbb105af0b30f7fb73a46871)
   for details.
- [FIX] Fixed issue where at least one index had to be created before a query would execute.  
  You can now query for documents without the existence of any indexes.
- [NEW] New fields `documentsReplicated` and `batchesReplicated` added
  to ReplicationCompleted class
- [NEW] New `ReplicatorBuilder` API. This should be used to create replications in the future.
- [NEW] HTTP Interceptor API. See [Http Interceptors](https://github.com/cloudant/sync-android/blob/0.13.0/doc/interceptors.md) for details.
- [DEPRECATED] Deprecated the  following classes: `ReplicatorFactory`, `Replication`,
  `PullReplication` and `PushReplication`.

# 0.12.3 (2015-06-30)

- [FIXED] Fixed issue with the encoding of local document URIs
- [FIXED] Fixed issue where sync-android would select a different
  winning revision than CouchDB for the same revision tree.

# 0.12.2 (2015-06-25)

- [NEW] Added query support for the `$mod` operator.
- [NEW] Added query support for the `$size` operator.
- [NEW] Added CachingKeyProvider, which can be used to improve performance in
  some situations.
- [FIXED] Fixed issue where documents with an empty array value would not be
  indexed
- [FIXED] Fixed issue where AES encryption keys were not created correctly on
  android lower than API level 19

# 0.12.1 (2015-06-12)

- [FIX] Fixed issue where Base64 encoded strings for HTTP basic authentication
  could contain line breaks.
- [FIX] Custom headers set by overriding getCouchConfig in Replication classes
  were not set on HTTP requests


# 0.12.0 (2015-06-11)

- [NEW] Encryption of all data is now supported using 256-bit AES:
  JSON documents, Query indexes and attachments. See
  [encryption documentation](https://github.com/cloudant/sync-android/blob/0.12.0/doc/encryption.md)
  for details.
- [NEW] Added query text search support.  See
  [query documentation](https://github.com/cloudant/sync-android/blob/0.12.0/doc/query.md)
  for details.
- [NEW] Use HttpURLConnection instead of Apache HttpClient to
  significantly reduce dependency footprint.
- [FIX] Fix issues with database compaction.
- [FIX] Fixed encoding of `+` characters in query strings.
- [REMOVED] Removed `oneway` methods from ReplicatorFactory. Users
  should use the method described in the
  [replication documentation](https://github.com/cloudant/sync-android/blob/0.12.0/doc/replication.md)
  instead.
- [NEW] Added query support for the `$mod` operator.

# 0.11.0 (2015-04-22)

- [FIX] Using MongoDB query as the "gold" standard, query support for
  `NOT` has been fixed to return result sets correctly (as in MongoDB
  query).  Previously, the result set from a query like `{ "pet": {
  "$not" { "$eq": "dog" } } }` would include a document like `{
  "pet" : [ "cat", "dog" ], ... }` because the array contains an
  object that isn't `dog`.  The new behavior is that `$not` now
  inverts the result set, so this document will no longer be included
  because it has an array element that matches `dog`.
- [BREAKING CHANGE] Changed local document APIs. createLocalDocument
  and updateLocalDocument have been removed and replaced by
  insertLocalDocument. The return type of getLocalDocument has been
  changed to LocalDocument.
- [NEW] getVersion API added to SQLDatabaseQueue
- [NEW] Datastore will not be created if the database version is not
  supported by the version of the library opening it.
- [REMOVED] Removed legacy indexing/query code.  Users should instead
  use the new Cloudant Query - Mobile functionality.  A
  [migration document](https://github.com/cloudant/sync-android/blob/0.11.0/doc/query-migration.md)
  exists for users that need to transition from the legacy
  implementation to the new one.
- [FIX] Fix attachment handling error which could cause push
  replications to fail in some circumstances

# 0.10.0 (2015-02-16)

- [NEW] Added `listAllDatastores` method to `DatastoreManager`.
- [BREAKING CHANGE] Introduce Checked exceptions for recoverable error
  conditions, these new checked exceptions are on the Datastore
  interface and its implementing classes. See the
  [commit](https://github.com/cloudant/sync-android/commit/e6d4f685cefe9c06a9c9372723d9cc06dbc7e978)
  for more infomation.
- [FIX] Support CouchDB 2.0/Cloudant Local's array-based
  sequence number format to fix replication between the local
  database and these remote servers.
- [FIX] Fixed issue where the path portion for remote database
  URLs were incorrectly encoded if there was more than one
  path segment.
- [IMPROVED] SQLite connections are now reused across threads by
  using a serial queue to enforce isolation. This should be more
  robust and allows us to properly implement the `close()` method
  on a datastore.
- [REMOVED] Removed username and password from `PullReplication` and
  `PushReplication`. Users should set their password in the source or
  destnation URL.
- [REMOVED] Removed allDbs() from Mazha CouchClient.

# 0.9.3 (2014-12-10)

- [FIX] Fixed issue where slashes were disallowed for replication.
- [FIX] Fixed issue where deleted documents were returned for conflicts.
- [NEW] Added compact API.

# 0.9.2 (2014-11-26)

- [FIX] Fixed a build issue with 0.9.1. The jars for 0.9.1 were built
  incorrectly, causing Android users to experience the error
  'Error:duplicate files during packaging of APK'

# 0.9.1 (2014-11-21)

- [Upgraded] Upgraded SQLite4java to version 1.0.392
- [FIX] Fixed issue where resurrected documents where handled in correctly
- [FIX] Fixed dataloss issue where conflicted document attachments were lost

# 0.9.0 (2014-10-9)

- [REMOVED] Removed deprecated APIs
- [REMOVED] Removed TypedDatastore class
- [FIX] Fixed issue where some cursors were left open
- [FIX] Fixed issue  where regexp for database names was too restrictive
- [FIX] Fixed indexmanager bug which prevented fields with certain names being indexed
- [FIX] Fixed issues where sqlite connections were not being closed correctly
- [NEW] Added proguard example configuration file

# 0.7.1 (2014-08-26)

- [NEW] A new CRUD and Attachments API has been introduced. See
  doc/crud.md for details, along with a cookbook on using the new API.
- [DEPRECATED] Some existing attachments and CRUD APIs have been
  deprecated and marked with the @Deprecated annotation. These will be
  removed before version 1.0.
- [FIX] Conflict resolution has been enhanced. The revision tree is
  now properly preserved, whereas previously a new revision was always
  created even if this was not necessary. Additionally,
  `ConflictResolver`s can return `MutableDocumentRevision`s. See
  doc/conflicts.md for details.

# 0.4.0 (2014-08-01)

- [NEW] Separate builds for Android and standard Java.
  The library is now distributed as 3 jars: 1 core jar, a Java SE-specific jar and an
  Android-specific jar.
- [FIX] Always send basic authentication instead of waiting for a 401 challenge on each
  request.
- [FIX] Fix possible crash when calling updateAllIndexes.
- [FIX] Fix replication of design documents.
- [BETA] Add API to Datastore for handling attachments.
   See javadoc and code for BasicDatastore *Attachment methods.
   This feature is in beta and the API will be changed in the near future.
- [FIX] Update indexes at query time.
   Previously indexes were updated each time a document was created or modified, which was
   inefficient.
- [FIX] Fix some SQLite multi-threading issues.

# 0.3.5 (2014-03-24)

- [FIX] Update indexes automatically after documents in the datastore are
  changed, including during pull replication.
- [FIX] Fix https://github.com/cloudant/sync-android/issues/17 where requests
  were failing because we couldn't load mazha.properties.

# 0.3.4 (2014-03-17)

- [FIX] Remove classes from public API which don't need to be there.
- [FIX] Update documentation around replication to account for filters.

# 0.3.3 (Unreleased)

- [NEW] Add options to pull replication (remote -> local) to support filtered
  replications. This required changing the way replications are defined:
  - There are new classes containing the configuration of pull and push
    replications, and a new `ReplicatorFactory` method which uses them.
  - The `ReplicatorFactory.oneway(source,target)` methods have been
    deprecated in favour of a method taking a fuller suite of replication
    options. They will be removed in a future version.

# 0.3.2 (2014-02-25)

- Fix issue initialising datastores on certain Android versions.
- Add user-agent to the calls Cloudant Sync makes while replicating.

# 0.3.1 (2014-02-21)

- Change min/max query to use inclusive ranges.
- Update gradle wrapper for sample app to require gradle 1.8.

# 0.3.0 (2014-02-19)

- Add indexing and query functionality.
- Ability to find and resolve conflicts.

# 0.2.0 (Unreleased)

- Add notification events for datastore and document changes.

# 0.1.1 (Unreleased)

- Merge mazha into main codebase, as com.cloudant.mazha.

# 0.1.0

- Initial release.
- Replication.
- CRUD.
