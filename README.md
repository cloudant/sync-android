# Cloudant Sync - Android Datastore

[![Build Status](https://travis-ci.org/cloudant/sync-android.png?branch=master)](https://travis-ci.org/cloudant/sync-android)

[![maven-central-core](https://img.shields.io/maven-central/v/com.cloudant/cloudant-sync-datastore-core.svg?label=maven-central core)](http://search.maven.org/#search|ga|1|g:"com.cloudant"%20AND%20a:"cloudant-sync-datastore-core")
[![javadoc-core](http://www.javadoc.io/badge/com.cloudant/cloudant-sync-datastore-core.svg?label=javadoc-core)](http://www.javadoc.io/doc/com.cloudant/cloudant-sync-datastore-core)

[![maven-central-android](https://img.shields.io/maven-central/v/com.cloudant/cloudant-sync-datastore-android.svg?label=maven-central android)](http://search.maven.org/#search|ga|1|g:"com.cloudant"%20AND%20a:"cloudant-sync-datastore-android")
[![javadoc-android](http://www.javadoc.io/badge/com.cloudant/cloudant-sync-datastore-android.svg?label=javadoc-android)](http://www.javadoc.io/doc/com.cloudant/cloudant-sync-datastore-android)

[![maven-central-android-encryption](https://img.shields.io/maven-central/v/com.cloudant/cloudant-sync-datastore-android-encryption.svg?label=maven-central android-encryption)](http://search.maven.org/#search|ga|1|g:"com.cloudant"%20AND%20a:"cloudant-sync-datastore-android-encryption")
[![javadoc-android-encryption](http://www.javadoc.io/badge/com.cloudant/cloudant-sync-datastore-android.svg?label=javadoc-android-encryption)](http://www.javadoc.io/doc/com.cloudant/cloudant-sync-datastore-android)

[![maven-central-javase](https://img.shields.io/maven-central/v/com.cloudant/cloudant-sync-datastore-javase.svg?label=maven-central javase)](http://search.maven.org/#search|ga|1|g:"com.cloudant"%20AND%20a:"cloudant-sync-datastore-javase")
[![javadoc-javase](http://www.javadoc.io/badge/com.cloudant/cloudant-sync-datastore-javase.svg?label=javadoc-javase)](http://www.javadoc.io/doc/com.cloudant/cloudant-sync-datastore-javase)

**Applications use Cloudant Sync to store, index and query local JSON data on a
device and to synchronise data between many devices. Synchronisation is under
the control of the application, rather than being controlled by the underlying
system. Conflicts are also easy to manage and resolve, either on the local
device or in the remote database.**

Cloudant Sync is an [Apache CouchDB&trade;][acdb]
replication-protocol-compatible datastore for
devices that don't want or need to run a full CouchDB instance. It's built
by [Cloudant](https://cloudant.com), building on the work of many others, and
is available under the [Apache 2.0 licence][ap2].

[ap2]: https://github.com/cloudant/sync-android/blob/master/LICENSE
[acdb]: http://couchdb.apache.org/

The API is quite different from CouchDB's; we retain the
[MVCC](http://en.wikipedia.org/wiki/Multiversion_concurrency_control) data
model but not the HTTP-centric API.

This library is for Android and Java SE; an [iOS][ios] version is also available.

[ios]: https://github.com/cloudant/CDTDatastore

If you have questions, please join our [mailing list][mlist] and drop us a
line.

[mlist]: https://groups.google.com/forum/#!forum/cloudant-sync

## Using in your project

The library is published via Maven Central and using it in your project should
be as simple as adding it as a dependency via [maven][maven] or [gradle][gradle].

[maven]: http://maven.apache.org/
[gradle]: http://www.gradle.org/

There are currently four jar files for the datastore:

* `cloudant-sync-datastore-core`: the main datastore classes.
* `cloudant-sync-datastore-android`: Android specific classes.
* `cloudant-sync-datastore-android-encryption`: Android encryption specific classes.
* `cloudant-sync-datastore-javase`: Java SE specific classes.

### Gradle or Maven

Add the maven repo and a compile time dependency on the appropriate datastore artifact. The datastore artifact dependency will include its other required dependencies and is different depending on the target use case as shown here:

1. Target is Android. If you also want datastore encryption see 2.
    * Gradle
    ```groovy
    repositories {
        mavenLocal()
        mavenCentral()
    }

    dependencies {
        compile group: 'com.cloudant', name: 'cloudant-sync-datastore-android', version:'latest.release'
    }
    ```
    * Maven
    ```xml
    <project>
        <repositories>
        ...
        </repositories>

        <dependencies>
            <dependency>
                <groupId>com.cloudant</groupId>
                <artifactId>cloudant-sync-datastore-android</artifactId>
                <version>latest.release</version>
                <scope>compile</scope>
            </dependency>
        </dependencies>
    </project>
    ```
1. Target is Android and you want datastore encryption. 
    * Gradle
    ```groovy
    repositories {
        mavenLocal()
        mavenCentral()
    }

    dependencies {
        compile group: 'com.cloudant', name: 'cloudant-sync-datastore-android-encryption', version:'latest.release'
    }
    ```
    * Maven
    ```xml
    <project>
        <repositories>
        ...
        </repositories>

        <dependencies>
            <dependency>
                <groupId>com.cloudant</groupId>
                <artifactId>cloudant-sync-datastore-android-encryption</artifactId>
                <version>latest.release</version>
                <scope>compile</scope>
            </dependency>
        </dependencies>
    </project>
    ```
1. Target is Java SE.
    * Gradle
    ```groovy
    repositories {
        mavenLocal()
        mavenCentral()
    }

    dependencies {
        compile group: 'com.cloudant', name: 'cloudant-sync-datastore-javase', version:'latest.release'
    }
    ```
    * Maven
    ```xml
    <project>
        <repositories>
        ...
        </repositories>

        <dependencies>
            <dependency>
                <groupId>com.cloudant</groupId>
                <artifactId>cloudant-sync-datastore-javase</artifactId>
                <version>latest.release</version>
                <scope>compile</scope>
            </dependency>
        </dependencies>
    </project>
    ```
    
You can see a fuller example in the sample application's [build.gradle][sabg].

[sabg]: https://github.com/cloudant/sync-android/blob/master/sample/todo-sync/build.gradle

_Note_: Older versions than 0.3.0 had a separate Mazha jar. This was rolled
into the main jar for distribution simplicity. The dependency needs removing
from gradle and maven build files.

## Tested Platforms

The library is regularly tested on the following platforms:

Android (via emulator):

* API Level 15
* API Level 16
* API Level 17
* API Level 18
* API Level 19
* API Level 21

Java:

* 1.8 (Java 8)

## Example application

There is a [sample application and a quickstart guide](/sample/).

## Overview of the library

Once the libraries are added to a project, the basics of adding and reading
a document are:

```java
// Create a DatastoreManager using application internal storage path
File path = getApplicationContext().getDir("datastores", Context.MODE_PRIVATE);
DatastoreManager manager = new DatastoreManager(path.getAbsolutePath());

try {
    Datastore ds = manager.openDatastore("my_datastore");

    // Create a document
    DocumentRevision revision = new DocumentRevision();
    Map<String, Object> body = new HashMap<String, Object>();
    body.put("animal", "cat");
    revision.setBody(DocumentBodyFactory.create(body));
    DocumentRevision saved = ds.createDocumentFromRevision(revision);

    // Add an attachment -- binary data like a JPEG
    UnsavedFileAttachment att1 =
        new UnsavedFileAttachment(new File("/path/to/image.jpg"), "image/jpeg");
    saved.getAttachments().put(att1.name, att1);
    DocumentRevision updated = ds.updateDocumentFromRevision(saved);

    // Read a document
    DocumentRevision aRevision = ds.getDocument(updated.getId());
} catch (DatastoreException datastoreException) {

    // this will be thrown if we don't have permissions to write to the
    // datastore path
    System.err.println("Problem opening datastore: "+datastoreException);
} catch (DocumentException documentException) {

    // this will be thrown in case of errors performing CRUD operations on
    // documents
    System.err.println("Problem accessing datastore: "+documentException);
}
```

Read more in [the CRUD document](https://github.com/cloudant/sync-android/blob/master/doc/crud.md).

You can also subscribe for notifications of changes in the database, which
is described in [the events documentation](https://github.com/cloudant/sync-android/blob/master/doc/events.md).

The
[javadoc](http://www.javadoc.io/doc/com.cloudant/cloudant-sync-datastore-core/)
for the latest release version of the library is the definitive
reference for the library. Each jar contains the full javadoc for
itself and the other jars - this is for convenience at the slight
expense of duplication.

Note that each class has an "API Status" declaration in the
javadoc. This status is either "Public" or "Private". The general
guidance is that API consumers (who are building software which uses
this library) are discouraged from using "Private" API classes. This
is because they may expose implementation details of the library which
may be subject to change without notice or without change to the major
version number. This behaviour follows the
[Semantic Versioning 2.0.0 specification](http://semver.org).

### Replicating Data Between Many Devices

Replication is used to synchronise data between the local datastore and a
remote database, either a CouchDB instance or a Cloudant database. Many
datastores can replicate with the same remote database, meaning that
cross-device synchronisation is achieved by setting up replications from each
device the the remote database.

Replication is simple to get started in the common cases:

```java
URI uri = new URI("https://apikey:apipasswd@username.cloudant.com/my_database");
Datastore ds = manager.openDatastore("my_datastore");

// Replicate from the local to remote database
Replicator replicator = ReplicatorBuilder.push().from(ds).to(uri).build();

// Fire-and-forget (there are easy ways to monitor the state too)
replicator.start();
```

Read more in [the replication docs](https://github.com/cloudant/sync-android/blob/master/doc/replication.md).

### Finding data

Once you have thousands of documents in a database, it's important to have
efficient ways of finding them. We've added an easy-to-use querying API. Once
the appropriate indexes are set up, querying is as follows:

```java
Map<String, Object> query = new HashMap<String, Object>();
query.put("name", "mike");
query.put("pet", "cat");
QueryResult result = indexManager.find(query);

for (DocumentRevision revision : result) {
    // do something
}
```

See [Index and Querying Data](https://github.com/cloudant/sync-android/blob/master/doc/query.md).

For information about migrating your legacy indexing/query code to the new Cloudant Query - Android implementation.

See [Index and Querying Migration](https://github.com/cloudant/sync-android/blob/master/doc/query-migration.md)

### Conflicts

An obvious repercussion of being able to replicate documents about the place
is that sometimes you might edit them in more than one place at the same time.
When the databases containing these concurrent edits replicate, there needs
to be some way to bring these divergent documents back together. Cloudant's
MVCC data-model is used to do this.

A document is really a tree of the document and its history. This is neat
because it allows us to store multiple versions of a document. In the main,
there's a single, linear tree -- just a single branch -- running from the
creation of the document to the current revision. It's possible, however,
to create further branches in the tree. At this point your document is
conflicted and needs some surgery to resolve the conflicts and bring it
back to full health.

Learn more about this essential process in the
[conflicts documentation](https://github.com/cloudant/sync-android/blob/master/doc/conflicts.md).

## Known Issues

Some users on certain older versions of Android have reported the
following exception:

`java.lang.NoClassDefFoundError: javax/annotation/Nullable`

To fix this issue, add the following dependency to your application's
`build.gradle`:

`compile 'com.google.code.findbugs:jsr305:3.0.0'`

## Contributors

See [CONTRIBUTORS](CONTRIBUTORS).

## Contributing to the project

See [CONTRIBUTING](CONTRIBUTING.md).

## License

See [LICENSE](LICENSE).
