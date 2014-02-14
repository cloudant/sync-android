# Example app: Todo Sync

The _todo-sync_ application shows how to do basic CRUD with the Sync Datastore and how to set up a replication between a remote Cloudant database and an application.

## Full Setup Guide

There's a full setup guide here: https://github.com/cloudant/sync-android/blob/master/doc/android-qs.md

## In Brief

Create a database on your Cloudant account for the app to synchronise with. It's best-practice to use API keys for device access rather than your Cloudant credentials, so when the database is created, use the Permissions tab to create an API key with:

* `read` and `write` permissions if you don't want to sync design documents.
* `admin` permissions if you want to sync design documents.

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
    <string name="default_api_key">dongshengcn</string>
    <string name="default_api_password">secretpassword</string>
</resources>
```

## Build

You will need [gradle][gradle] 1.8 installed to build the sample application.

[gradle]: http://www.gradle.org/installation

Set up a `local.properties` file in the root folder of the sample app with the location of your Android SDK:

```
sdk.dir=/Users/mike/Code/android/android-sdk-macosx
```

Connect your development-enabled Android device. You should be able install the sample application on to your device using:

```bash
gradle installDebug
```

Once installed, the default settings entered in the XML file above should appear in the Settings screen of the application. 

To test your connection, add a couple of tasks and hit "Upload (Push)" from the menu in the top right. You should see the JSON documents appear in your Cloudant database. Changes to the documents in the Cloudant database will be replicated back to the device when you tap "Download (Pull)".

If you see "Replication Error" rather than "Replication Complete" as a popup message, check the logs using `adb` to see what the exception was.
