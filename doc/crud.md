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


### Create

Documents are represented as a set of revisions. To create a document, you
set up the initial revision of the document and save that to the datastore.

Create a mutable document revision object, set its body, ID and attachments
and then call `createDocumentFromRevision(MutableDocumentRevision)` to add it to the datastore:

```java
Datastore ds = manager.openDatastore("my_datastore");

// Create a document
MutableDocumentRevision rev = new MutableDocumentRevision();
rev.docId = "doc1"; // Or don't assign the docId property, we'll generate one

// Build up body content from a Map
Map<String, Object> json = new HashMap<String, Object>();
json.put("description", "Buy milk");
json.put("completed", false);
json.put("type", "com.cloudant.sync.example.task");
rev.body = DocumentBodyFactory.create(json);

DocumentRevision revision = datastore.createDocumentFromRevision(rev);
```

The only mandatory property to set before calling
`createDocumentFromRevision(MutableDocumentRevision)` is the `body`. An ID will be generated
for documents which don't have `docId` set.

### Retrieve

Once you have created one or more documents, retrieve them by ID:

```java
String docId = revision.docId;
BasicDocumentRevision retrieved = datastore.getDocument(docId);
```

You get an immutable revision back from this method call. To make changes to
the document, you need to call `mutableCopy()` on the revision and save it
back to the datastore, as shown below.

### Update

To update a document, call `mutableCopy()` on the original document revision,
make your changes and save the document:

```java
MutableDocumentRevision update = retrieved.mutableCopy();

Map<String, Object> json = retrieved.getBody().asMap();
json.put("completed", true);
update.body = DocumentBodyFactory.create(json);

datastore.updateDocumentFromRevision(update);
```

### Delete

To delete a document, you need the current revision:

```java
ds.deleteDocumentFromRevision(saved);
```

## Indexing

You don't need to know the ID of the document to retrieve it. Datastore
provides ways to index and search the fields of your JSON documents.
For more, see [index-query.md](index-query.md).

## Conflicts

## Getting all documents

The `getAllDocuments()` method allows iterating through all documents in the
database:

```java
// read all documents in one go
int pageSize = ds.getDocumentCount();
List<DocumentRevision> docs = ds.getAllDocuments(0, pageSize, true);
```

## Using attachments

You can associate attachments with the JSON documents in your datastores.
Attachments are blobs of binary data, such as photos or short sound snippets.
They should be of small size -- maximum a few MB -- because they are
replicated to and from the server in a way which doesn't allow for resuming
an upload or download.

Attachments are stored in the `attachments` property on a DocumentRevision
object. This is a map of attachments, keyed by attachment name.

To add an attachment to a document, just add (or overwrite) the attachment
in the `attachments` map:

```java
// Create a new document
MutableDocumentRevision rev = new MutableDocumentRevision();
// or get an existing one and create a mutable copy
BasicDocumentRevision retrieved = datastore.getDocument("mydoc");
MutableDocumentRevision = retrieved.mutableCopy();

rev.body = DocumentBodyFactory.create( ... );
UnsavedFileAttachment att1 = new UnsavedFileAttachment(
    new File("/path/to/image.jpg"), "image/jpeg");

// As with the document body, you can replace the attachments
rev.attachments = new HashMap<String, Attachment>();

// Or just add or update a single one:
rev.attachments.put(att1.name, att1);

DocumentRevision saved = datastore.createDocumentFromRevision(rev);
```

When creating new attachments, use `UnsavedFileAttachment` for data you already have on disk. Use
`UnsavedStreamAttachment` when you have data which comes from an InputStream or is already in
memory.

```java
UnsavedFileAttachment att1 = new UnsavedFileAttachment(
    new File("/path/to/image.jpg"), "image/jpeg");

byte[] imageData;
// ByteArrayInputStream adapts imageData to be used as a stream
UnsavedStreamAttachment att2 = new UnsavedStreamAttachment(
    new ByteArrayInputStream(imageData), "cute_cat.jpg", "image/jpeg");

MutableDocumentRevision rev = new MutableDocumentRevision();
rev.getAttachments().put(att1.name, att1);
rev.getAttachments().put(att2.name, att2);
DocumentRevision saved = datastore.createDocumentFromRevision(rev);
```

To read an attachment, get the `SavedAttachment` from the `attachments`
map. Then use `getInputStream()` to read the data:

```java
DocumentRevision retrieved = datastore.getDocument("myDoc");
Attachment att = retrieved.getAttachments().get("cute_cat.jpg");
InputStream is = att.getInputStream();

// get all bytes in memory if required
byte[] data = new byte[(int)att.getSize()];
is.read(data);
```

To remove an attachment, remove it from the `attachments` map:

```java
BasicDocumentRevision retrieved = datastore.getDocument("myDoc");
MutableDocumentRevision update = retrieved.mutableCopy();
update.remove("cute_cat.jpg");
DocumentRevision updated = datastore.updateDocumentFromRevision(update);
```

To remove all attachments, set the `attachments` property to an empty map
or `null`:

```java
update.attachments = null;
```

## Cookbook

This section shows all the ways (that I could think of) that you can update,
modify and delete documents.

### Creating a new document

This is the simplest case as we don't need to worry about previous revisions.

1. Add a document with body, but not attachments or ID. You'll get an
   autogenerated ID.
    ```java
    MutableDocumentRevision rev = new MutableDocumentRevision();
    rev.body = DocumentBodyFactory.create( ... );
    
    DocumentRevision saved = datastore.createDocumentFromRevision(rev);
    ```
    
1. Add a new document to the store with a body and ID, but without attachments.
    ```java
    MutableDocumentRevision rev = new MutableDocumentRevision();
    rev.docId = "doc1";
    rev.body = DocumentBodyFactory.create( ... );
    
    DocumentRevision saved = datastore.createDocumentFromRevision(rev);
    ```
    
1. Add a new document to the store with attachments.
    ```java
    MutableDocumentRevision rev = new MutableDocumentRevision();
    rev.docId = "doc1";
    rev.body = DocumentBodyFactory.create( ... );

    DocumentRevision saved = datastore.createDocumentFromRevision(rev);
    
    UnsavedFileAttachment att1 = new UnsavedFileAttachment(
        new File("/path/to/image.jpg", "image/jpeg"));
    rev.attachments.put(att1.name, att1);
    
    DocumentRevision saved = datastore.createDocumentFromRevision(rev);
    ```
    
1. Add a document with body and attachments, but no ID. You'll get an
   autogenerated ID.
    ```java
    MutableDocumentRevision rev = new MutableDocumentRevision();
    rev.body = DocumentBodyFactory.create( ... );
    
    DocumentRevision saved = datastore.createDocumentFromRevision(rev);
    
    UnsavedFileAttachment att1 = new UnsavedFileAttachment(
        new File("/path/to/image.jpg", "image/jpeg"));
    rev.attachments.put(att1.name, att1);
    
    DocumentRevision saved = datastore.createDocumentFromRevision(rev);
    ```
    
1. You can't create a document without a body (body is the only required property).
    ```java
    MutableDocumentRevision rev = new MutableDocumentRevision();
    
    DocumentRevision saved = datastore.createDocumentFromRevision(rev);
    // will throw java.lang.NullPointerException: Input document body can not be null
    ```
    
### Updating a document

To update a document, call `mutableCopy()` on the original document revision,
make your changes and save the document.

For the first set of examples the original document is set up with a body
and no attachments:

```java
MutableDocumentRevision rev = new MutableDocumentRevision();
rev.docId = "doc1";
rev.body = DocumentBodyFactory.create( ... );

DocumentRevision saved = datastore.createDocumentFromRevision(rev);

```
    
We also assume an attachment ready to be added:

```java
UnsavedFileAttachment att1 = new UnsavedFileAttachment(
    new File("/path/to/image.jpg", "image/jpeg"));
```
    
1. Update body for doc that has no attachments, adding no attachments
    ```java
    MutableDocumentRevision update = saved.mutableCopy();
    update.body = DocumentBodyFactory.create( ... );

    DocumentRevision updated = datastore.updateDocumentFromRevision(update);
    ```
    
1. Update body for doc with no attachments, adding attachments. Here we see
   that a mutableCopy of a document with no attachments has an
   `HashMap` set for its `attachments` property.
    ```java
    MutableDocumentRevision update = saved.mutableCopy();
    update.body = DocumentBodyFactory.create( ... );
    update.attachments.put(att1.name, att1);
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(update);
    ```
    
1. Update body for doc with no attachments, removing attachments dictionary
   entirely.
    ```java
    MutableDocumentRevision update = saved.mutableCopy();
    update.body = DocumentBodyFactory.create( ... );
    update.attachments = null;
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(update);
    ```
        
1. Update the attachments without changing the body, add attachments to a doc
   that had none.
    ```java
    MutableDocumentRevision update = saved.mutableCopy();
    update.attachments.put(att1.name, att1);
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(update);
    ```
    
1. Update attachments by copying from another revision.
    ```java
    MutableDocumentRevision anotherDoc = datastore.getDocument("anotherId");
    MutableDocumentRevision update = saved.mutableCopy();
    update.attachments = anotherDoc.attachment;
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(update);
    ```
    
1. Updating a document using an outdated source revision causes a conflict
    ```java
    MutableDocumentRevision update = saved.mutableCopy();
    update.body = DocumentBodyFactory.create( ... );
    datastore.updateDocumentFromRevision(update);
    
    MutableDocumentRevision update2 = saved.mutableCopy();
    update.body = DocumentBodyFactory.create( ... );
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(update);
    // throws ConflictException
    ```
    
For the second set of examples the original document is set up with a body and
several attachments:

```java
MutableDocumentRevision rev = new MutableDocumentRevision();
rev.docId = "doc1";
rev.body = DocumentBodyFactory.create( ... );

UnsavedFileAttachment att1 = ...
/* set up more attachments... */
rev.attachments.put(att1.name, att1);
rev.attachments.put(att2.name, att2);
rev.attachments.put(att3.name, att3);

DocumentRevision saved = datastore.createDocumentFromRevision(rev);
```
    
1. Update body without changing attachments
    ```java
    MutableDocumentRevision update = saved.mutableCopy();
    update.body = DocumentBodyFactory.create( ... );
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(update);
    // Should have the same attachments
    ```
    
1. Update the attachments without changing the body, remove attachments
    ```java
    MutableDocumentRevision update = saved.mutableCopy();
    update.attachments.remove(att1.name);
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(update);
    ```
    
1. Update the attachments without changing the body, add attachments
    ```java
    MutableDocumentRevision update = saved.mutableCopy();
    update.attachments.put(att1.name, att1);
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(update);
    ```
    
1. Update the attachments without changing the body, remove all attachments
   by setting `null` for attachments map.
    ```java
    MutableDocumentRevision update = saved.mutableCopy();
    update.attachments = null;
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(update);
    ```
    
1. Update the attachments without changing the body, remove all attachments
   by setting an empty dictionary.
    ```java
    MutableDocumentRevision update = saved.mutableCopy();
    update.attachments = new HashMap<String, Attachment>();
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(update);
    ```
    
1. Copy an attachment from one document to another.
    ```java
    MutableDocumentRevision rev = new MutableDocumentRevision();
    rev.docId = "doc1";
    rev.body = DocumentBodyFactory.create( ... );
    UnsavedFileAttachment att1 = ...
    rev.attachments.put(att1.name, att1);
    DocumentRevision revWithAttachmnets = datastore.createDocumentFromRevision(rev);
    
    // Add attachment to "saved" from "revWithAttachments"
    MutableDocumentRevision updated = saved.mutableCopy();
    Attachment savedAttachment = revWithAttachments.get("nameOfAttachment");
    update.attachments.put(savedAttachment.name, savedAttachment);
    
    DocumentRevision updated = datastore.updateFromRevision(update);
    ```
    
### Creating a document from a `mutableCopy`

It should be possible to create a new document from a `mutableCopy` of an existing document.


1. Add a document from a `mutableCopy`, with attachments
    ```java
    MutableDocumentRevision update = saved.mutableCopy();
    update.docId = "doc2";
    update.body = DocumentBodyFactory.create( ... );
    // Create att100 attachment
    update.attachments.put(att100.name, att100);
    
    DocumentRevision updated = datastore.createDocumentFromRevision(update);
    ```
    
1. Add a document from a `mutableCopy`, without attachments
    ```java
    MutableDocumentRevision update = saved.mutableCopy();
    update.docId = "doc2";
    update.body = DocumentBodyFactory.create( ... );
    update.attachments = null;
    
    DocumentRevision updated = datastore.createDocumentFromRevision(update);
    ```
    
1. Fail if the document ID is present in the datastore. Note this shouldn't
   fail if the document is being added to a different datastore.
    ```java
    DocumentRevision update = saved.mutableCopy();
    update.body = DocumentBodyFactory.create( ... );
    
    DocumentRevision updated = datastore.createDocumentFromRevision(update);
    // throws java.lang.IllegalArgumentException: Can not insert new doc, likely the docId exists already: doc1
    ```
    
### Deleting a document

1. You should be able to delete a given revision (i.e., add a tombstone to the end of the branch).

    ```java
    DocumentRevision saved = datastore.getDocument("doc1");
    DocumentRevision deleted = datastore.deleteDocumentFromRevision(saved);
    ```
    
    This would refuse to delete if `saved` was not a leaf node.


1. **Advanced** You should also be able to delete a document in its entirety by passing in an ID.

    ```java
    DocumentRevision deleted = datastore.deleteDocument("doc1");
    ```

    This marks *all* leaf nodes deleted. Make sure to read
    [conflicts.md](conflicts.md) before using this method as it can result
    in data loss (deleting conflicted versions of documents, not just the
    current winner).
