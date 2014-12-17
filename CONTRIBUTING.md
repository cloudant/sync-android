# Developing this library

Cloudant Sync - Android is written in Java and uses
[gradle](http://www.gradle.org) as its build tool.

## Requirements

The main requirements are:

* Java 1.6
* Gradle 1.8+
* Android SDK

Optionally, and recommended:

* CouchDB

## Coding guidelines

Adopting the [Google Java Style](https://google-styleguide.googlecode.com/svn/trunk/javaguide.html)
with the following changes:

```
4.2 
    Our block indent is +4 characters

4.4
    Our line length is 100 characters.

4.5.2
    Indent continuation of +4 characters fine, but I think 
    IDEA defaults to 8, which is okay too.
```

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

### Android SDK

Follow the instructions provided on the android developer site.

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

#### Running integration tests on Android


Running the integration tests on android requires the running of a dedicated test app,
in addition to the requirements for Java SE integration tests. 

The test application build scripts require a running emulator and the ```ANDROID_HOME```
 environment variable to be set

The minimum requirements for an android emulator:

* Minimum API Level 15 (Target API Level is 20)
* An SD card

This test app can be run via gradle


```bash
$ gradle clean installStandardTestDebug waitForTestAppToFinishTests
```
The app will run all tests on specified by the build variant on first start up, to rerun tests
you must rerun the gradle build.

#### Collecting Test Results

Test Results are displayed via the app, a test report is also available for CI builds.
The test result report is in the same format as the ANT JUnit task reports. These style reports can be interpreted by 
Jenkins and other systems.  The reports are located under the ``` build/test-results/ ``` the file name is automatically generated 
by the build scripts, the base file name is ```testResults_``` it is suffixed with a UUID for that build.


#### Testing using Cloudant

Certain tests need a running CouchDB instance, by default they use the local
CouchDB. To run tests with Cloudant, you need to set your Cloudant account
credentials: add the following to `gradle.properties` in the same folder as
`build.gradle`:

```
systemProp.test.with.cloudant=[true|false]
systemProp.test.cloudant.username=yourCloudantUserName
systemProp.test.cloudant.password=yourCloudantPassword
```

Your gradle.properties should NEVER be checked into git repo as it contains your
cloudant credentials.

### Code coverage

To get code coverage, you need the `coberturaReport` task before the tests you want to run:

```bash
$ gradle coberturaReport test
$ gradle coberturaReport integrationTest
$ gradle coberturaReport systemTest
```

Code coverage metrics end up in `build/reports`.

## Using Android Studio / IDEA IDE

The best way to get everything set up correctly is to use IDEA's JetGradle plugin
to automatically generate and configure an IDEA project. IDEA will then keep the 
project in sync with the `.gradle` file.

Install this via Preferences -> Plugins (under IDE Settings) -> Gradle.

For Android Studio, this plugin is already installed.

You can then use File -> Import Project to create the IDEA project, which will allow
you to run gradle tasks from IDEA in addition to setting up dependencies correctly. Just
select the `build.gradle` file in the Import dialog box and select the default
settings.

An alternative is to use gradle itself, which is also able to generate an idea project.

```bash
$ gradle idea
```

See [Gradle's IDEA plugin docs](http://www.gradle.org/docs/current/userguide/idea_plugin.html).

### Code Style

An IDEA code style matching the guidelines above is included in the project,
in the `.idea` folder.

If you already have the project, enable the code style follow these steps:

1. Go to _Preferences_ -> _Editor_ -> _Code Style_.
2. In the _Scheme_ dropdown, select _Project_.

IDEA will then use the style when reformatting, refactoring and so on.


### Running JUnit tests in the IDE

To run JUnit tests from Android Studio / IDEA, you will need to add some configuration to tell it
where the SQLite native library lives.

* In the menu, select Run -> Edit Configurations
* In the left hand pane, select Defaults -> JUnit
* Set VM options to:
```
-ea -Dsqlite4java.library.path=native
```

