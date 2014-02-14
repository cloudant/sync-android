# Developing this library

Cloudant Sync - Android is written in Java and uses
[gradle](http://www.gradle.org) as its build tool.

## Requirements

The main requirements are:

* Java 1.6
* Gradle 1.8+

Optionally, and recommended:

* CouchDB

## Installing requirements

### Java

Follow the instructions for your platform.

### Gradle

In OS X, use [homebrew](http://brew.sh/):

```bash
$ brew install gradle
```

Other platforms TDB.

### CouchDB

Again, using brew:

```bash
$ brew install couchdb
```

## Building the library

The project should build out of the box with:

```bash
$ gradle build
```

This will download the dependencies, build the library and run the unit
tests.

If you want to use the library in other projects, install it to your local
maven repository:

```bash
$ gradle install
```

### Running integration tests

These require a running couchdb:

```bash
$ couchdb
Apache CouchDB 1.3.1 (LogLevel=info) is starting.
Apache CouchDB has started. Time to relax.
[info] [<0.31.0>] Apache CouchDB has started on http://127.0.0.1:5984/
```

And in another terminal:

```bash
$ gradle integrationTest
```

#### Testing using Cloudant

Certain tests need a running CouchDB instance, by default they use the local
CouchDB. To run tests with Cloudant, you need to set your Cloudant account
credentials: add the following to `local.gradle` in the same folder as
`build.gradle`:

```groovy
tasks.withType(Test) {
    systemProperty "test.with.cloudant", [true|false]
    systemProperty "test.cloudant.username", "yourCloudantUserName"
    systemProperty "test.cloudant.password", "yourCloudantPassword"
}
```

Your local.gradle should NEVER be checked into git repo as it contains your
cloudant credentials.

### Code coverage

To get code coverage, you need the `coberturaReport` task before the tests you want to run:

```bash
$ gradle coberturaReport test
$ gradle coberturaReport integrationTest
$ gradle coberturaReport systemTest
```

Code coverage metrics end up in `build/reports`.

## Using the IDEA IDE

The best way to get everything set up correctly is to use IDEA's JetGradle plugin
to automatically generate and configure an IDEA project. IDEA will then keep the 
project in sync with the `.gradle` file.

Install this via Preferences -> Plugins (under IDE Settings) -> Gradle.

You can then use File -> Import Project to create the IDEA project, which will allow
you to run gradle tasks from IDEA in addition to setting up dependencies correctly. Just
select the `build.gradle` file in the Import dialog box and select the default
settings.

An alternative is to use gradle itself, which is also able to generate an idea project.

```bash
$ gradle idea
```

See http://www.gradle.org/docs/current/userguide/idea_plugin.html.
