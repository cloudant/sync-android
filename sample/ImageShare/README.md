#Example app: Image Share

This application shows how to use Sync to add document attachments, replicate them to a remote database and share these among different users.

## Setup

1. Create a database on your Cloudant account for the app to synchronise with.

2. Create an API key with `read` and `write` permissions.

3. Add your username, database name, key and password to settings.xml in app/src/res/values directory.

## Build

You will need [gradle][gradle] 1.8 installed to build the sample application.

[gradle]: http://www.gradle.org/installation

You will also need Build Tools version 19.1.

## Using the app

**Current** functionality:

Tapping the images will create a random document and put it in a local database.

Action bar:
Pull button replicates the remote database into your local one.
Replicate button replicates the local database to the remote.