# Cloudant Sync - Android Datastore

[![Build Status](https://travis-ci.org/cloudant/sync-android.svg?branch=master)](https://travis-ci.org/cloudant/sync-android)

| Artifact | Links |
| ---|--- |
| Core | [![maven-central-core](https://img.shields.io/maven-central/v/com.cloudant/cloudant-sync-datastore-core.svg?label=maven-central%20core)](http://search.maven.org/#search\|ga\|1\|g:"com.cloudant"%20AND%20a:"cloudant-sync-datastore-core") [![javadoc-core](http://www.javadoc.io/badge/com.cloudant/cloudant-sync-datastore-core.svg?label=javadoc-core)](http://www.javadoc.io/doc/com.cloudant/cloudant-sync-datastore-core) |
| Android | [![maven-central-android](https://img.shields.io/maven-central/v/com.cloudant/cloudant-sync-datastore-android.svg?label=maven-central%20android)](http://search.maven.org/#search\|ga\|1\|g:"com.cloudant"%20AND%20a:"cloudant-sync-datastore-android") [![javadoc-android](http://www.javadoc.io/badge/com.cloudant/cloudant-sync-datastore-android.svg?label=javadoc-android)](http://www.javadoc.io/doc/com.cloudant/cloudant-sync-datastore-android) |
| Android (encryption) | [![maven-central-android-encryption](https://img.shields.io/maven-central/v/com.cloudant/cloudant-sync-datastore-android-encryption.svg?label=maven-central%20android-encryption)](http://search.maven.org/#search\|ga\|1\|g:"com.cloudant"%20AND%20a:"cloudant-sync-datastore-android-encryption") [![javadoc-android-encryption](http://www.javadoc.io/badge/com.cloudant/cloudant-sync-datastore-android-encryption.svg?label=javadoc-android-encryption)](http://www.javadoc.io/doc/com.cloudant/cloudant-sync-datastore-android-encryption)
| Java SE | [![maven-central-javase](https://img.shields.io/maven-central/v/com.cloudant/cloudant-sync-datastore-javase.svg?label=maven-central%20javase)](http://search.maven.org/#search\|ga\|1\|g:"com.cloudant"%20AND%20a:"cloudant-sync-datastore-javase") [![javadoc-javase](http://www.javadoc.io/badge/com.cloudant/cloudant-sync-datastore-javase.svg?label=javadoc-javase)](http://www.javadoc.io/doc/com.cloudant/cloudant-sync-datastore-javase)

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

There is a [sample Android application](https://github.com/cloudant/sync-android/blob/master/sample/todo-sync).

See the accompanying README.md for detailed instructions of how to run and build the sample.

## Overview of the library

Once the libraries are added to a project, the basics of adding and reading
a document are:

```java
// Obtain storage path on Android
File path = getApplicationContext().getDir("documentstores", Context.MODE_PRIVATE);

try {
    // Obtain reference to DocumentStore instance, creating it if doesn't exist
    DocumentStore ds = DocumentStore.getInstance(new File(path, "my_document_store"));

    // Create a document
    DocumentRevision revision = new DocumentRevision();
    Map<String, Object> body = new HashMap<String, Object>();
    body.put("animal", "cat");
    revision.setBody(DocumentBodyFactory.create(body));
    DocumentRevision saved = ds.database().create(revision);

    // Add an attachment -- binary data like a JPEG
    UnsavedFileAttachment att1 =
            new UnsavedFileAttachment(new File("/path/to/image.jpg"), "image/jpeg");
    saved.getAttachments().put("image.jpg", att1);
    DocumentRevision updated = ds.database().update(saved);

    // Read a document
    DocumentRevision aRevision = ds.database().read(updated.getId());
} catch (DocumentStoreException dse) {
    System.err.println("Problem opening or accessing DocumentStore: "+dse);
}
```

Extensive code samples can be found in 
[the CRUD samples class](https://github.com/cloudant/sync-android/blob/master/doc/CrudSamples.java).

You can also subscribe for notifications of changes in the database, which
is described in [the events documentation](https://github.com/cloudant/sync-android/blob/master/doc/events.md).

The
[javadoc](http://www.javadoc.io/doc/com.cloudant/cloudant-sync-datastore-core/)
for the latest release version of the library is the definitive
reference for the library. Each jar contains the full javadoc for
itself and the other jars - this is for convenience at the slight
expense of duplication.

All private API classes are in the `com.cloudant.sync.internal`
package. API users should not use these classes as fields, method
signatures, and implementation details may be subject to
change. Directly using the classes in these packages or calling their
methods is not supported.

Everything else under `com.cloudant.sync` is public API and subject to
the usual versioning and deprecation practices. API users can expect
this to be a stable API. This behaviour follows
the [Semantic Versioning 2.0.0 specification](http://semver.org).

Classes in the `com.cloudant.sync.internal` package are not visible in
javadoc.

### Replicating Data Between Many Devices

Replication is used to synchronise data between the local datastore and a
remote database, either a CouchDB instance or a Cloudant database. Many
datastores can replicate with the same remote database, meaning that
cross-device synchronisation is achieved by setting up replications from each
device the the remote database.

Replication is simple to get started in the common cases:

```java
URI uri = new URI("https://apikey:apipasswd@username.cloudant.com/my_database");
DocumentStore ds = DocumentStore.getInstance(new File(path, "my_datastore"));

// Replicate from the local to remote database
Replicator replicator = ReplicatorBuilder.push().from(ds).to(uri).build();

// Fire-and-forget (there are easy ways to monitor the state too)
replicator.start();
```

Read more in [the replication docs](https://github.com/cloudant/sync-android/blob/master/doc/replication.md).

### Authentication

Sync-android uses session cookies by default to authenticate with the server if
credentials are provided with in the URL. 

To use an IAM API key on IBM Bluemix, use the `iamApiKey` method when
building the `Replicator` object:

```java
Replicator replicator = ReplicatorBuilder.push()
  .from(ds)
  .to(uri)
  .iamApiKey("exampleApiKey")
  .build();
```

If you want more fine-grained control over authentication, provide an interceptor to perform the authentication for each request. For example, if you are using middleware such as [Envoy][envoy], you can use
the `BasicAuthInterceptor` to add basic authentication to requests.

Example:

```java
Replicator replicator = ReplicatorBuilder.push()
  .from(ds)
  .to(uri)
  .addRequestInterceptors(new BasicAuthInterceptor("yourUsername:yourPassword"))
  .build();
```
Other authentication mechanisms such as proxy authentication can also used by
either using the in built interceptors (see the `com.cloudant.http.interceptors`
package) or by creating your own class which conforms to
`HttpConnectionRequestInterceptor` or `HttpConnectionResponseInterceptor`.

[envoy]: https://github.com/cloudant-labs/envoy
### Finding data

Once you have thousands of documents in a database, it's important to have
efficient ways of finding them. We've added an easy-to-use querying API. Once
the appropriate indexes are set up, querying is as follows:

```java
DocumentStore ds; // instance obtained previously
Map<String, Object> query = new HashMap<String, Object>();
query.put("name", "mike");
query.put("pet", "cat");
QueryResult result = ds.query().find(query);

for (DocumentRevision revision : result) {
    // do something
}
```

See [Index and Querying Data](https://github.com/cloudant/sync-android/blob/master/doc/query.md).

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

## Issues

Before opening a new issue please consider the following:
* Only the latest release is supported. If at all possible please try to reproduce the issue using
the latest version.
* Please check the [existing issues](https://github.com/cloudant/sync-android/issues)
to see if the problem has already been reported. Note that the default search
includes only open issues, but it may already have been closed.
* Cloudant customers should contact Cloudant support for urgent issues.
* When opening a new issue [here in github](../../issues) please complete the template fully.

#### Known Issues

Some users on certain older versions of Android have reported the
following exception:

`java.lang.NoClassDefFoundError: javax/annotation/Nullable`

To fix this issue, add the following dependency to your application's
`build.gradle`:

`compile 'com.google.code.findbugs:jsr305:3.0.0'`


## Related documentation

* [Cloudant docs](https://console.bluemix.net/docs/services/Cloudant/cloudant.html#overview)
* [Cloudant Learning Center](https://developer.ibm.com/clouddataservices/cloudant-learning-center/)

## Development

For information about contributing, building, and running tests see the [CONTRIBUTING.md](CONTRIBUTING.md).

## Contributors

See [CONTRIBUTORS](CONTRIBUTORS).

## License

See [LICENSE](LICENSE).
