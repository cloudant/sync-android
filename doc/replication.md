# Replication with Cloudant Sync Android

_This functionality is available in versions 0.3.3 and up._

This document discusses setting up replication with the library, along
with synchronising data by using two-way (bi-direction) replication.

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
local datastore to a remote database, from a remote database to a local
datastore, or both ways to implement synchronisation.

First we create a simple listener that just sets a CountDownLatch when the
replication finishes so we can wait for a replication to finish without
needing to poll:

```java
import com.google.common.eventbus.Subscribe;

/**
 * A {@code ReplicationListener} that sets a latch when it's told the
 * replication has finished.
 */
private class Listener {

    private final CountDownLatch latch;
    public ErrorInfo error = null;

    Listener(CountDownLatch latch) {
        this.latch = latch;
    }

    @Subscribe
    public void complete(ReplicationCompleted event) {
        latch.countDown();
    }

    @Subscribe
    public void error(ReplicationErrored event) {
        this.error = event.errorInfo;
        latch.countDown();
    }
}
```

Next we replicate a local datastore to a remote database:

```java
import com.cloudant.sync.replication.ReplicationFactory;
import com.cloudant.sync.replication.Replicator;
import com.cloudant.sync.replication.PushReplication;

URI uri = new URI("https://username.cloudant.com/my_database");
Datastore ds = manager.openDatastore("my_datastore");

// Create a replicator that replicates changes from the local
// datastore to the remote database.
// username/password can be Cloudant API keys
PushReplication push = new PushReplication();
push.username = "username";
push.password = "password";
push.source = ds;
push.target = uri;

Replicator replicator = ReplicatorFactory.oneway(push);

// Use a CountDownLatch to provide a lightweight way to wait for completion
latch = new CountDownLatch(1);
Listener listener = new Listener(latch);
replicator.getEventBus().register(listener);
replicator.start();
latch.await();
replicator.getEventBus().unregister(listener);
if (replicator.getState() != Replicator.State.COMPLETE) {
    System.out.println("Error replicating TO remote");
    System.out.println(listener.error);
}
```

And getting data from a remote database to a local one:

```java
URI uri = new URI("https://username.cloudant.com/my_database");
Datastore ds = manager.openDatastore("my_datastore");

// Create a replictor that replicates changes from the remote
// database to the local datastore.
// username/password can be Cloudant API keys
PullReplicator pull = new PullReplicator();
pull.username = "username";
pull.password = "password";
pull.source = uri;
pull.target = ds;

Replicator replicator = ReplicatorFactory.oneway(pull);

// Use a CountDownLatch to provide a lightweight way to wait for completion
latch = new CountDownLatch(1);
Listener listener = new Listener(latch);
replicator.getEventBus().register(listener);
replicator.start();
latch.await();
replicator.getEventBus().unregister(listener);
if (replicator.getState() != Replicator.State.COMPLETE) {
    System.out.println("Error replicating FROM remote");
    System.out.println(listener.error);
}
```

And running a full sync, that is, two one way replicaitons:

```java

URI uri = new URI("https://username.cloudant.com/my_database");
Datastore ds = manager.openDatastore("my_datastore");

// username/password can be Cloudant API keys
PullReplication pull = new PullReplication();
pull.username = "username";
pull.password = "password";
pull.source = uri;
pull.target = ds;

PushReplication push = new PushReplication();
pull.username = "username";
pull.password = "password";
pull.source = ds;
pull.target = uri;

Replicator replicator_pull = ReplicatorFactory.oneway(pull);
Replicator replicator_push = ReplicatorFactory.oneway(push);

// Use a latch starting at 2 as we're waiting for two replications to finish
latch = new CountDownLatch(2);
Listener listener = new Listener(latch);

// Set the listener and start for both pull and push replications
replicator_pull.getEventBus().register(listener);
replicator_pull.start();
replicator_push.getEventBus().register(listener);
replicator_push.start();

// Wait for both replications to complete, decreasing the latch via listeners
latch.await();

// Unsubscribe the listeners
replicator_pull.getEventBus().unregister(listener);
replicator_push.getEventBus().unregister(listener);

// Unfortunately in this implementation we'll only record the last error
// the listener saw
if (replicator_pull.getState() != Replicator.State.COMPLETE) {
    System.out.println("Error replicating FROM remote");
    System.out.println(listener.error);
}
if (replicator_push.getState() != Replicator.State.COMPLETE) {
    System.out.println("Error replicating FROM remote");
    System.out.println(listener.error);
}
```

### Using IndexManager with replication

When using IndexManager for indexing and querying data, it needs to be updated after replication completes:

```java
import com.cloudant.sync.replication.ReplicationFactory;
import com.cloudant.sync.replication.Replicator;
import com.cloudant.sync.indexing.IndexManager;

// username/password can be Cloudant API keys
URI uri = new URI("https://username:password@username.cloudant.com/my_database");
Datastore ds = manager.openDatastore("my_datastore");

// Create a replicator that replicates changes from the local
// datastore to the remote database.
Replicator replicator = ReplicatorFactory.oneway(ds, uri);

// Create a sample index on type field
IndexManager indexManager = new IndexManager(ds);
indexManager.ensureIndexed("type", "type");

// Use a CountDownLatch to provide a lightweight way to wait for completion
latch = new CountDownLatch(1);
Listener listener = new Listener(latch);
replicator.getEventBus().register(listener);
replicator.start();
latch.await();
replicator.getEventBus().unregister(listener);
if (replicator.getState() != Replicator.State.COMPLETE) {
    System.out.println("Error replicating TO remote");
    System.out.println(listener.error);
}

// Ensure all indexes are updated after replication
indexManager.updateAllIndexes();

```

### Filtered pull replication

[Filtered replication][1] is only supported for pull replication. It requies a 
"Filter" object is added to "PullReplication" to describe the 
_Filter Function_ that is used and its query parameters. 

```java
import com.cloudant.sync.replication.ReplicationFactory;
import com.cloudant.sync.replication.Replicator;
import com.cloudant.sync.replication.PullReplication;
import com.cloudant.sync.replication.Replication.Filter;


Map<String, String> parameters = new HashMap<String, String>();
parameters.put("key", "value");
Replication.Filter filter = new Replication.Filter("filterDoc/filterFunctionName", parameters);

PullReplication pull = new PullReplication();
pull.source = this.getURI();
pull.target = this.datastore;
pull.filter = filter;

Replicator replicator = ReplicatorFactory.oneway(pullReplication);
```

[1]: http://docs.couchdb.org/en/1.4.x/replication.html#controlling-which-documents-to-replicate

### Deprecated APIs

The following APIs are still supported but deprecated. They will be soon removed from the library. 

```java
ReplicatorFactory {
    public static Replicator oneway(Datastore source, URI target);
    public static Replicator oneway(URI target, Datastore source);
}
```