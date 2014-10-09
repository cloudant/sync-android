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
