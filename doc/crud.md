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
DatastoreManager manager = new DatastoreManager(path.getAbsolutePath());
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
