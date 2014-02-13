# Cloudant Sync - Android Datastore

**Applications use Cloudant Sync to store, index and query local JSON data on a
device and to synchronise data between many devices. Synchronisation is under
the control of the application, rather than being controlled by the underlying
system. Conflicts are also easy to manage and resolve, either on the local
device or in the remote database.**

[Cloudant Sync][eap] is an [Apache CouchDB&trade;][acdb]
replication-protocol-compatible datastore for
devices that don't want or need to run a full CouchDB instance. It's built
by [Cloudant](https://cloudant.com), building on the work of many others, and
is available under the [Apache 2.0 licence][ap2].

It is currently available in preview form on Android, with an iOS version
coming soon.

[ap2]: https://github.com/cloudant/cloudant-sync-eap/blob/master/LICENSE
[eap]: https://github.com/cloudant/cloudant-sync-eap
[acdb]: http://couchdb.apache.org/

The API is quite different from CouchDB's; we retain the 
[MVCC](http://en.wikipedia.org/wiki/Multiversion_concurrency_control) data 
model but not the HTTP-centric API.

## Using in your project

Using the library in your project should be as simple as adding it as
a dependency via [maven][maven] or [gradle][gradle].

[maven]: http://maven.apache.org/
[gradle]: http://www.gradle.org/

There are currently two jar files for the datastore:

* `cloudant-sync-datastore-android` contains the main datastore.
* `mazha` contains a simple CouchDB client.

We will be rolling `mazha` into `cloudant-sync-datastore-android` as `mazha`
only contains the functionality to support HTTP requests made for replication
and isn't intended to be used as a separate package.

### Gradle

Add the EAP maven repo and a compile time dependency on the datastore and
mazha jars:

```groovy
repositories {
    mavenLocal()
    maven { url "http://cloudant.github.io/cloudant-sync-eap/repository/" }
    mavenCentral()
}

dependencies {
    // Other dependencies
    compile group: 'com.cloudant', name: 'cloudant-sync-datastore-android', version:'0.1.0'
    compile group: 'com.cloudant', name: 'mazha', version:'0.1.1'
}
```

You can see a fuller example in the sample application's [build.gradle][sabg].

[sabg]: https://github.com/cloudant/cloudant-sync-eap/blob/master/sample/todo-sync/build.gradle

### Maven

It's a similar story in maven, add the repo and the dependencies:

```xml
<project>
  ...

  <repositories>
    ...
    <repository>
      <id>cloudant-sync-eap</id>
      <name>Cloudant Sync EAP</name>
      <url>http://cloudant.github.io/cloudant-sync-eap/repository/</url>
    </repository>
  </repositories>

  <dependencies>
    ...
    <dependency>
      <groupId>com.cloudant</groupId>
      <artifactId>cloudant-sync-datastore-android</artifactId>
      <version>0.1.0</version>
      <scope>compile</scope>
    </dependency>
    <dependency>
      <groupId>com.cloudant</groupId>
      <artifactId>mazha</artifactId>
      <version>0.1.1</version>
      <scope>compile</scope>
    </dependency>
  </dependencies>

</project>
```

## Storing and Manipulating Local Data

Once you have the dependencies installed, the classes described below should
all be available to your project.

### Datastore and DatastoreManager objects

A `Datastore` object manages a set of JSON documents, keyed by ID.

A `DatastoreManager` object manages a directory where `Datastore` objects
store their data. It's a factory object for named `Datastore` instances. A
named datastore will persist its data between application runs. Names are
arbitrary strings, with the restriction that the name must match
`^[a-zA-Z]+[a-zA-Z0-9_]*`.

It's best to give a `DatastoreManager` a directory of its own, and to make the
manager a singleton within an application. The content of the directory is
simple folders and SQLite databases if you want to take a peek.

Therefore, start by creating a `DatastoreManager` to manage datastores for
a given directory:

```java
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.Datastore;

// Create a DatastoreManager using application internal storage path
File path = getApplicationContext().getDir("datastores");
DatastoreManager helper = new DatastoreManager(path.getAbsolutePath());
```

Once you've a manager set up, it's straightforward to create datastores:

```java
Datastore ds = manager.openDatastore("my_datastore");
Datastore ds2 = manager.openDatastore("other_datastore");
```

The `DatabaseManager` handles creating and initialising non-existent
datastores, so the object returned is ready for reading and writing.

To delete a datastore:

```java
manager.deleteDatastore("my_datastore");
```

It's important to note that this doesn't check there are any active
`Datastore` objects for this datastore. The behaviour of active `Datastore`
objects after their underlying files have been deleted is undefined.

### Document CRUD APIs

Once you have a `Datastore` instance, you can use it to create, update and
delete documents.

```java
Datastore ds = manager.openDatastore("my_datastore");

// Create a document
DocumentBody body = new BasicDBBody(jsonData);
DocumentRevision revision = ds.createDocument(body);

// Read a document
DocumentRevision aRevision = ds.getDocument(revision.getId());

// Update a document
DocumentBody updatedBody = new BasicDBBody(moreJsonData);
updatedRevision = ds.updateDocument(
    revision.getId(),
    revision.getRevision(),
    updatedBody
);

// Delete a document
ds.deleteDocument(
    updatedRevision.getId(),
    updatedRevision.getRevision()
);
```

As can be seen above, the `updateDocument` and `deleteDocument` methods both
require the revision of the version of the document currently in the datastore
to be passed as an argument. This is to prevent data being overwritten, for
example if a replication had changed the document since it had been read from
the local datastore by the applicaiton.

The `getAllDocuments()` method allows iterating through all documents in the
database:

```java
// read all documents in one go
int pageSize = ds.getDocumentCount();
List<DocumentRevision> docs = ds.getAllDocuments(0, pageSize, true);
```

## Replicating Data Between Many Devices

Replication is used to synchronise data between the local datastore and a
remote database, either a CouchDB instance or a Cloudant database. Many
datastores can replicate with the same remote database, meaning that
cross-device syncronisation is acheived by setting up replications from each
device the the remote database.

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

## Conflicts

A document is really a tree of the document and its history. This is neat
because it allows us to store multiple versions of a document. In the main,
there's a single, linear tree -- just a single branch -- running from the
creation of the document to the current revision. It's possible, however,
to create further branches in the tree.

When a document has been replicated to more than one place, it's possible to
edit it concurrently in two places. When the datastores storing the document
then replicate with each other again, they each add their changes to the
document's tree. This causes an extra branch to be added to the tree for
each concurrent set of changes. When this happens, the document is said to be
_conflicted_. This creates multiple current revisions of the document, one for
each of the concurrent changes.

To make things easier, calling `Datastore#getDocument(...)` returns one of
the leaf nodes of the branches of the conflicted document. It selects the
node to return in an arbitrary but deterministic way, which means that all
replicas of the database will return the same revision for the document. The
other copies of the document are still there, however, waiting to be merged.

See more information on document trees in the [javadocs][jd] for `DocumentRevisionTree`.

[jd]: docs/

In v1 of the EAP, searching for and resolving conflicts isn't supported, but
it'll be one of the first features we add.

## Finding data

At the moment, you can only retrieve a document using its ID. However, more
sophisticated indexing and querying is on the way.

