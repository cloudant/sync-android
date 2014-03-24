# 0.3.5 (Unreleased)

- [FIX] Fix https://github.com/cloudant/sync-android/issues/17 where requests
  were failing because we couldn't load mazha.properties .

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
