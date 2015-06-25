# Unreleased

- [NEW] Added query support for the `$mod` operator.
- [NEW] Added query support for the `$size` operator.

# 0.12.1 (2015-06-12)

- [FIX] Fixed issue where Base64 encoded strings for HTTP basic authentication
  could contain line breaks.
- [FIX] Custom headers set by overriding getCouchConfig in Replication classes
  were not set on HTTP requests


# 0.12.0 (2015-06-11)

- [NEW] Encryption of all data is now supported using 256-bit AES:
  JSON documents, Query indexes and attachments. See
  [encryption documentation](https://github.com/cloudant/sync-android/blob/master/doc/encryption.md)
  for details.
- [NEW] Added query text search support.  See
  [query documentation](https://github.com/cloudant/sync-android/blob/master/doc/query.md)
  for details.
- [NEW] Use HttpURLConnection instead of Apache HttpClient to
  significantly reduce dependency footprint.
- [FIX] Fix issues with database compaction.
- [FIX] Fixed encoding of `+` characters in query strings.
- [REMOVED] Removed `oneway` methods from ReplicatorFactory. Users
  should use the method described in the
  [replication documentation](https://github.com/cloudant/sync-android/blob/master/doc/replication.md)
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
  [migration document](https://github.com/cloudant/sync-android/tree/master/doc/query-migration.md)
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
