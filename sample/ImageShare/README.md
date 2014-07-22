#Example app: Image Share

This application shows how to use Sync to add document attachments, replicate them to a remote database and share these among different users.

## Setup

1. Download the python authentication server script and run it with command `python auth_serv.py login password`, where login and password are login and password of your Cloudant account.

2. Add the server address and cloudant username to `res/values/settings.xml` :
	```xml
	<string name="default_api_server">http://ip_address:port_number</string>
    <string name="default_user">Add username here</string>
	```

Alternatively, you can add `db_name`, `api_key` and `api_password` to `res/values/settings.xml` if you want to test the app without running an authentication script. You would also need to create `db_name` database yourself. However, in this case connecting to a different database and creating a new database will not work.

## Build

You will need [gradle][gradle] 1.8 installed to build the sample application.

[gradle]: http://www.gradle.org/installation

You will also need Build Tools version 19.1.

## Using the app

The user can create a new remote database by clicking the `New DB` button. This will create a new database on account specified by `login` field of authentication server start command. `Share` button would then allow the user to share this database with others by simply sending them its name.

Alternatively, the user can connect to already created database by clicking `Connect`. This will open a window asking the user to type in (or paste) the database name he/she wishes to connect to.

The user can add new images to the app by clicking `Add` button. This will create a new document in a local database with given image as attachment.

`Push` and `Pull` buttons allow the user to synchronise with the remote database. These buttons will only work if the app was previously connected to a remote database or this user created a new database.

`Local delete` button allows to delete everyting in the local database. However, all images uploaded to the remote database will stay there.