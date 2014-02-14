# Replication with Cloudant Sync Android

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
import com.cloudant.sync.replication.ReplicationListener;

/**
 * A {@code ReplicationListener} that sets a latch when it's told the
 * replication has finished.
 */
private class Listener implements ReplicationListener {

    private final CountDownLatch latch;
    public ErrorInfo error = null;

    Listener(CountDownLatch latch) {
        this.latch = latch;
    }

    @Override
    public void complete(Replicator replicator) {
        latch.countDown();
    }

    @Override
    public void error(Replicator replicator, ErrorInfo error) {
        this.error = error;
        latch.countDown();
    }
}
```

Next we replicate a local datastore to a remote database:

```java
import com.cloudant.sync.replication.ReplicationFactory;
import com.cloudant.sync.replication.Replicator;

// username/password can be Cloudant API keys
URI uri = new URI("https://username:password@username.cloudant.com/my_database");
Datastore ds = manager.openDatastore("my_datastore");

// Create a replicator that replicates changes from the local
// datastore to the remote database.
Replicator replicator = ReplicatorFactory.oneway(ds, uri);

// Use a CountDownLatch to provide a lightweight way to wait for completion
latch = new CountDownLatch(1);
Listener listener = new Listener(latch);
replicator.setListener(listener);
replicator.start();
latch.await();
if (replicator.getState() != Replicator.State.COMPLETE) {
    System.out.println("Error replicating TO remote");
    System.out.println(listener.error);
}
```

And getting data from a remote database to a local one:

```java
// username/password can be Cloudant API keys
URI uri = new URI("https://username:password@username.cloudant.com/my_database");
Datastore ds = manager.openDatastore("my_datastore");

// Create a replictor that replicates changes from the remote
// database to the local datastore.
replicator = ReplicatorFactory.oneway(uri, ds);

// Use a CountDownLatch to provide a lightweight way to wait for completion
latch = new CountDownLatch(1);
Listener listener = new Listener(latch);
replicator.setListener(listener);
replicator.start();
latch.await();
if (replicator.getState() != Replicator.State.COMPLETE) {
    System.out.println("Error replicating FROM remote");
    System.out.println(listener.error);
}
```

And running a full sync, that is, two one way replicaitons:

```java
// username/password can be Cloudant API keys
URI uri = new URI("https://username:password@username.cloudant.com/my_database");
Datastore ds = manager.openDatastore("my_datastore");

replicator_pull = ReplicatorFactory.oneway(uri, ds);
replicator_push = ReplicatorFactory.oneway(ds, uri);

// Use a latch starting at 2 as we're waiting for two replications to finish
latch = new CountDownLatch(2);
Listener listener = new Listener(latch);

// Set the listener and start for both pull and push replications
replicator_pull.setListener(listener);
replicator_pull.start();
replicator_push.setListener(listener);
replicator_push.start();

// Wait for both replications to complete, decreasing the latch via listeners
latch.await();

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
