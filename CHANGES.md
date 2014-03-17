# 0.3.3 (2014-03-17)

- [NEW] Add options to pull replication (remote -> local) to support filtered
  replications.
  - There are new methods to create replications.
  - The `ReplicatorFactory.oneway` methods have been deprecated in favour
    of methods taking a fuller suite of replication options. They will be
    removed in a future version.

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
