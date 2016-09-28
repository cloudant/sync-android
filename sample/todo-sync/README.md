# Example application: Todo Sync

** The latest released sample version can be downloaded from the
[GitHub releases page][latest].**

[latest]: https://github.com/cloudant/releases/latest

The _todo-sync_ Android application shows how to do basic CRUD
(create, read, update, delete) with the local Datastore and how to
replicate between a remote Cloudant database and a local Datastore.

The application is a simple example of a "to-do" list with items which
can be created, marked "done", and deleted.

## In Brief

Create a database on your Cloudant account for the application to
synchronise with. It's best-practice to use API keys for device access
rather than your Cloudant credentials, so when the database is
created, use the Permissions tab to create an API key with:

* `read` and `write` permissions if you don't want to sync design documents.
* `admin` permissions if you want to sync design documents.

Database permissions can be set throught the
[Cloudant Dashboard](https://cloudant.com/changing-database-permissions-tutorial/).

For the example below, this database is called `example_app_todo`.

Gather the following information:

1. Your Cloudant account username.
2. The name of the database set up above.
3. API key set up above.
4. API password set up above.

Add these values to `res/values/settings.xml` as shown:

```xml
<?xml version="1.0" encoding="utf-8"?>
<resources>
    <string name="default_user">cloudant_account_name</string>
    <string name="default_dbname">example_app_todo</string>
    <string name="default_api_key">cloudant_account_name</string>
    <string name="default_api_password">cloudant_api_password</string>
</resources>
```

## Build

The [gradle][gradle] wrapper (`gradlew` or `gradlew.bat`) is bundled
with this distribution. This is used to build the sample application.

[gradle]: http://www.gradle.org/installation

You will need to install the [Android SDK][android] in order to build
and run the sample application. Remember to set your SDK location -
there are two ways to do this:

[android]: https://developer.android.com/studio/index.html

- Export the ANDROID_HOME environment variable:
```
export ANDROID_HOME=/Users/mike/Code/android/android-sdk-macosx
```

_or_

- Set up a `local.properties` file in the `todo-sync` directory with
  the location of your Android SDK:
```
sdk.dir=/Users/mike/Code/android/android-sdk-macosx
```

Now you are ready to build and run the sample application. You can run
the application on an emulator or an a development-enabled Android
device.

Either start your emulator instance or connect your device. In the
`todo-sync` directory, you should be able install the sample
application using:
```bash
../../gradlew installDebug
```

Once installed, the default settings entered in the XML file above should appear in the Settings screen of the application.

To test your connection, add a couple of tasks and hit "Upload (Push)" from the menu in the top right. You should see the JSON documents appear in your Cloudant database. Changes to the documents in the Cloudant database will be replicated back to the device when you tap "Download (Pull)".

If you see "Replication Error" rather than "Replication Complete" as a popup message, check the logs using `adb` to see what the exception was.
