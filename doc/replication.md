# Replication with Cloudant Sync Android

Replication is used to synchronise data between the local document store and a
remote database, either a CouchDB instance or a Cloudant database. Many
document stores can replicate with the same remote database, meaning that
cross-device synchronisation is achieved by setting up replications from each
device to the remote database.

## Replication Scenarios

Replication is a flexible system for copying data between local and remote
databases. Each replication is in a single direction, copying differences either
from a local database to a remote database or a remote database to a local
database.

![Push and pull replications](images/replication-push-pull.png)

Often, a single local and remote database will be kept synchronised with each
other. For example, all a user's notes synchronised between a web application
and a device-based application. To fully synchronise data between two databases,
run a push *and* a pull replication. These can be run concurrently.

![Synchronising two databases](images/replication-sync.png)

Replication is not limited to a single pair of databases. If a user has several
devices, it's simple to set up replications between each device and a central
remote database to synchronise data between devices.

![Synchronising local database with two remote databases](images/replication-multi-local.png)

Less commonly but just as tenable, data can be sent from a single local
database to several remote databases:

![Synchronising local database with two remote databases](images/replication-multi-remote.png)

A final diagram shows how to replicate databases on the local
device with several different remote databases, even if they are on different
servers.

![Replicating local databases with several remotes](images/replication-many.png)

Overall, replication is very flexible and can be set up in many topologies.
In particular, many scenarios might only require a push or a pull replication:

* A data collection application might only need to use a push replication to
  replicate data from the device to a remote database -- there's no need for
  synchronisation and therefore no corresponding pull replication.
* If only a local data cache is required, using only a pull replication will
  keep local data up to date with remote data.

### Setting Up For Sync

Currently, the replication process requires a remote database to exist already.
To avoid exposing credentials for the remote system on each device, we recommend
creating a web service to authenticate users and set up databases for client
devices. This web service needs to:

* Handle sign in/sign up for users.
* Create a new remote database for a new user.
* Grant access to the new database for the new device (e.g., via [API keys][keys]
  on Cloudant or the `_users` database in CouchDB).
* Return the database URL and credentials to the device.

[keys]: https://cloudant.com/for-developers/faq/auth/

### Replication on the Device

From the device side, replication is straightforward. You can replicate from a
local document store to a remote database, from a remote database to a local
document store, or both ways to implement synchronisation.

Replications are set up in code on a device. Use `ReplicatorBuilder` to create
a pre-configured `Replicator` object. Each `Replicator`
object can be assigned a `Listener` to receive message when replication
completes or encounters an error.

The replication examples below are 'one-shot' and will run once until
completion, at which point the local and remote databases will be
synchronised. [Replication policies](replication-policies.md) allow
for more complex "policies" to be executed, for example "only replicate when on
Wifi" or "replicate every 30 minutes".

First we create a simple listener that just sets a CountDownLatch when the
replication finishes so we can wait for a replication to finish without
needing to poll:

```java
/**
 * A {@code ReplicationListener} that sets a latch when it's told the
 * replication has finished.
 */
public class Listener {

    private final CountDownLatch latch;
    public List<Throwable> errors = new ArrayList<Throwable>();
    public int documentsReplicated = 0;
    public int batchesReplicated = 0;

    Listener(CountDownLatch latch) {
        this.latch = latch;
    }

    @Subscribe
    public void complete(ReplicationCompleted event) {
        this.documentsReplicated += event.documentsReplicated;
        this.batchesReplicated += event.batchesReplicated;
        latch.countDown();
    }

    @Subscribe
    public void error(ReplicationErrored event) {
        this.errors.add(event.errorInfo);
        latch.countDown();
    }
}
```

Next we replicate a local document store to a remote database:

```java
// Username/password are supplied in the URL and can be Cloudant API keys
URI uri = new URI("https://username:password@examples.cloudant.com/my_database");

DocumentStore ds = DocumentStore.getInstance(new File("my_datastore"));

// Create a replicator that replicates changes from the local
// datastore to the remote database.
Replicator replicator = ReplicatorBuilder.push().to(uri).from(ds).build();

// Use a CountDownLatch to provide a lightweight way to wait for completion
CountDownLatch latch = new CountDownLatch(1);
Listener listener = new Listener(latch);
replicator.getEventBus().register(listener);
replicator.start();
latch.await();
replicator.getEventBus().unregister(listener);
if (replicator.getState() != Replicator.State.COMPLETE) {
    System.err.println("Error replicating TO remote");
    System.err.println(listener.errors);
} else {
    System.out.println(String.format("Replicated %d documents in %d batches",
            listener.documentsReplicated, listener.batchesReplicated));
}
```

And getting data from a remote database to a local one:

```java
// Username/password are supplied in the URL and can be Cloudant API keys
URI uri = new URI("https://username:password@examples.cloudant.com/my_database");

DocumentStore ds = DocumentStore.getInstance(new File("my_datastore"));

// Create a replicator that replicates changes from the remote
// database to the local datastore.
Replicator replicator = ReplicatorBuilder.pull().from(uri).to(ds).build();

// Use a CountDownLatch to provide a lightweight way to wait for completion
CountDownLatch latch = new CountDownLatch(1);
Listener listener = new Listener(latch);
replicator.getEventBus().register(listener);
replicator.start();
latch.await();
replicator.getEventBus().unregister(listener);
if (replicator.getState() != Replicator.State.COMPLETE) {
    System.err.println("Error replicating FROM remote");
    System.err.println(listener.errors);
} else {
    System.out.println(String.format("Replicated %d documents in %d batches",
            listener.documentsReplicated, listener.batchesReplicated));
}
```

And running a full sync, that is, two one way replications:

```java

// Username/password are supplied in the URL and can be Cloudant API keys
URI uri = new URI("https://username:password@examples.cloudant.com/my_database");

DocumentStore ds = DocumentStore.getInstance(new File("my_datastore"));

// Create the pull replicator
Replicator pullReplicator = ReplicatorBuilder.pull().from(uri).to(ds).build();

// Create the push replicator
Replicator pushReplicator = ReplicatorBuilder.push().to(uri).from(ds).build();

// Use a latch starting at 2 as we're waiting for two replications to finish
CountDownLatch latch = new CountDownLatch(2);
Listener listener = new Listener(latch);

// Set the listener and start for both pull and push replications
pullReplicator.getEventBus().register(listener);
pullReplicator.start();
pushReplicator.getEventBus().register(listener);
pushReplicator.start();

// Wait for both replications to complete, decreasing the latch via listeners
latch.await();

// Unsubscribe the listeners
pullReplicator.getEventBus().unregister(listener);
pushReplicator.getEventBus().unregister(listener);

if (pullReplicator.getState() != Replicator.State.COMPLETE) {
    System.err.println("Error replicating FROM remote");
    System.err.println(listener.errors);
} else if (pushReplicator.getState() != Replicator.State.COMPLETE) {
    System.err.println("Error replicating TO remote");
    System.err.println(listener.errors);
} else {
    System.out.println(String.format("Replicated %d documents in %d batches",
            listener.documentsReplicated, listener.batchesReplicated));
}
```

### Using `Query` with replication

When using `Query` for querying data, we recommend you update after
replication completes to avoid a wait for indexing to catch up when the new data
is first queried:

```java
// username/password can be Cloudant API keys
URI uri = new URI("https://username:password@examples.cloudant.com/my_database");

DocumentStore ds = DocumentStore.getInstance(new File("my_datastore"));

// Create a replicator that replicates changes from the remote
// database to the local datastore.
Replicator replicator = ReplicatorBuilder.pull().from(uri).to(ds).build();

// Create a sample index on type field
ds.query().createJsonIndex(Arrays.asList(new FieldSort("fieldName")), "indexName");

// Use a CountDownLatch to provide a lightweight way to wait for completion
CountDownLatch latch = new CountDownLatch(1);
Listener listener = new Listener(latch);
replicator.getEventBus().register(listener);
replicator.start();
latch.await();
replicator.getEventBus().unregister(listener);
if (replicator.getState() != Replicator.State.COMPLETE) {
    System.out.println("Error replicating TO remote");
    System.out.println(listener.errors);
}

// Ensure all indexes are updated after replication
ds.query().refreshAllIndexes();

```

### Filtered pull replication

[Filtered replication][1] is only supported for pull replication. It requires a
`PullFilter` to be added to the `ReplicatorBuilder` object. A `PullFilter` describes
the _Filter Function_ that is used and its query parameters.

```java
Map<String, String> parameters = new HashMap<String, String>();
parameters.put("key", "value");
PullFilter filter = new PullFilter("filterDoc/filterFunctionName", parameters);
Replicator replicator = ReplicatorBuilder.pull()
                        .from(uri))
                        .to(ds)
                        .filter(filter)
                        .build();
```

[1]: http://docs.couchdb.org/en/1.4.x/replication.html#controlling-which-documents-to-replicate

