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

Create a document revision object, set its body, ID and attachments
and then call `createDocumentFromRevision(DocumentRevision)` to add it to the datastore:

```java
Datastore ds = manager.openDatastore("my_datastore");

// Create a document with a document id as the constructor argument
DocumentRevision rev = new DocumentRevision("doc1");
// Or don't assign the docId property, we'll generate one
DocumentRevision rev = new DocumentRevision();

// Build up body content from a Map
Map<String, Object> json = new HashMap<String, Object>();
json.put("description", "Buy milk");
json.put("completed", false);
json.put("type", "com.cloudant.sync.example.task");
rev.setBody(DocumentBodyFactory.create(json));

DocumentRevision revision = datastore.createDocumentFromRevision(rev);
```

The only mandatory property to set before calling
`createDocumentFromRevision(DocumentRevision)` is the `body`. An ID will be generated
for documents which don't have `docId` set.

### Retrieve

Once you have created one or more documents, retrieve them by ID:

```java
String docId = revision.docId;
BasicDocumentRevision retrieved = datastore.getDocument(docId);
```

This document is mutable and you can make changes to it, as shown below.

### Update

To update a document, make your changes and save the document:

```java
DocumentRevision retrieved; // previously retrieved

Map<String, Object> json = retrieved.getBody().asMap();
json.put("completed", true);
retreived.setBody(DocumentBodyFactory.create(json));

datastore.updateDocumentFromRevision(retrieved);
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

Attachments are stored via the `attachments` getters/setters on a DocumentRevision
object. This is a map of attachments, keyed by attachment name.

To add an attachment to a document, just add (or overwrite) the attachment
in the `attachments` map:

```java
// Create a new document
DocumentRevision rev = new DocumentRevision();
// or get an existing one
DocumentRevision retrieved = datastore.getDocument("mydoc");

// create a body
rev.body = DocumentBodyFactory.create( ... );

// create an UnsavedFileAttachment: the constructor takes
// a File object on disk and a MIME type
UnsavedFileAttachment att1 = new UnsavedFileAttachment(
    new File("/path/to/image.jpg"), "image/jpeg");

// As with the document body, you can replace the attachments
rev.setAttachments(new HashMap<String, Attachment>());

// Or just add or update a single one:
// (because the getter will always return the underlying map and not a copy)
rev.getAttachments().put(att1.name, att1);

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

DocumentRevision rev = new DocumentRevision();
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
DocumentRevision retrieved = datastore.getDocument("myDoc");
retrieved.getAttachments().remove("cute_cat.jpg");
DocumentRevision updated = datastore.updateDocumentFromRevision(retrieved);
```

To remove all attachments, set the `attachments` property to an empty map
or `null`:

```java
update.setAttachments(null);
```

## Cookbook

This section shows all the ways (that I could think of) that you can update,
modify and delete documents.

### Creating a new document

This is the simplest case as we don't need to worry about previous revisions.

1. Add a document with body, but not attachments or ID. You'll get an
   autogenerated ID.
    ```java
    DocumentRevision rev = new DocumentRevision();
    rev.setBody(DocumentBodyFactory.create( ... ));
    
    DocumentRevision saved = datastore.createDocumentFromRevision(rev);
    ```
    
1. Add a new document to the store with a body and ID, but without attachments.
    ```java
    DocumentRevision rev = new DocumentRevision("doc1");
    rev.setBody(DocumentBodyFactory.create( ... ));
    
    DocumentRevision saved = datastore.createDocumentFromRevision(rev);
    ```
    
1. Add a new document to the store with attachments.
    ```java
    DocumentRevision rev = new DocumentRevision("doc1");
    rev.setBody(DocumentBodyFactory.create( ... ));

    DocumentRevision saved = datastore.createDocumentFromRevision(rev);
    
    UnsavedFileAttachment att1 = new UnsavedFileAttachment(
        new File("/path/to/image.jpg", "image/jpeg"));
    rev.getAttachments().put(att1.name, att1);
    
    DocumentRevision saved = datastore.createDocumentFromRevision(rev);
    ```
    
1. Add a document with body and attachments, but no ID. You'll get an
   autogenerated ID.
    ```java
    DocumentRevision rev = new DocumentRevision();
    rev.setBody(DocumentBodyFactory.create( ... ));
    
    DocumentRevision saved = datastore.createDocumentFromRevision(rev);
    
    UnsavedFileAttachment att1 = new UnsavedFileAttachment(
        new File("/path/to/image.jpg", "image/jpeg"));
    rev.getAttachments().put(att1.name, att1);
    
    DocumentRevision saved = datastore.createDocumentFromRevision(rev);
    ```
    
1. You can't create a document without a body (body is the only required property).
    ```java
    DocumentRevision rev = new DocumentRevision();
    
    DocumentRevision saved = datastore.createDocumentFromRevision(rev);
    // will throw java.lang.NullPointerException: Input document body can not be null
    ```
    
### Updating a document

To update a document, make your changes and save the document.

For the first set of examples the original document is set up with a body
and no attachments:

```java
DocumentRevision rev = new DocumentRevision("doc1");
rev.setBody(DocumentBodyFactory.create( ... ));

DocumentRevision saved = datastore.createDocumentFromRevision(rev);

```
    
We also assume an attachment ready to be added:

```java
UnsavedFileAttachment att1 = new UnsavedFileAttachment(
    new File("/path/to/image.jpg", "image/jpeg"));
```
    
1. Update body for doc that has no attachments, adding no attachments
    ```java
    DocumentRevision saved; // a document we saved earlier
    update.setBody(DocumentBodyFactory.create( ... ));

    DocumentRevision updated = datastore.updateDocumentFromRevision(saved);
    ```
    
1. Update body for doc with no attachments, adding attachments. The
   attachments map is accessed and modified via the getAttachments()
   getter.
    ```java
    DocumentRevision saved; // a document we saved earlier
    saved.setBody(DocumentBodyFactory.create( ... ));
    saved.getAttachments().put(att1.name, att1);
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(saved);
    ```
    
1. Update body and remove all attachments.
    ```java
    DocumentRevision saved; // a document we saved earlier
    saved.setBody(DocumentBodyFactory.create( ... ));
    saved.setAttachments(null);
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(saved);
    ```
        
1. Update the attachments without changing the body, add attachments to a doc
   that had none.
    ```java
    DocumentRevision saved; // a document we saved earlier
    saved.getAttachments().put(att1.name, att1);
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(saved);
    ```
    
1. Update attachments by copying from another revision.
    ```java
    DocumentRevision anotherDoc = datastore.getDocument("anotherId");
    DocumentRevision saved; // a document we saved earlier
    saved.getAttachments().putAll(anotherDoc.getAttachments());
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(saved);
    ```
    
1. Updating a document using an outdated source revision causes a conflict
    ```java
    DocumentRevision saved; // a document we saved earlier
    saved.setBody(DocumentBodyFactory.create( ... ));
    datastore.updateDocumentFromRevision(saved);
    
    saved.setBody(DocumentBodyFactory.create( ... ));
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(saved);
    // throws ConflictException
    ```
    
For the second set of examples the original document is set up with a body and
several attachments:

```java
DocumentRevision rev = new DocumentRevision("doc1");
rev.setBody(DocumentBodyFactory.create( ... ));

UnsavedFileAttachment att1 = ...
/* set up more attachments... */
rev.getAttachments().put(att1.name, att1);
rev.getAttachments().put(att2.name, att2);
rev.getAttachments().put(att3.name, att3);

DocumentRevision saved = datastore.createDocumentFromRevision(rev);
```
    
1. Update body without changing attachments
    ```java
    DocumentRevision saved; // a document we saved earlier
    saved.setBody(DocumentBodyFactory.create( ... ));
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(saved);
    // Should have the same attachments
    ```
    
1. Update the attachments without changing the body, remove attachments
    ```java
    DocumentRevision saved; // a document we saved earlier
    saved.getAttachments().remove(att1.name);
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(saved);
    ```
    
1. Update the attachments without changing the body, add attachments
    ```java
    DocumentRevision saved; // a document we saved earlier
    saved.getAttachments().put(att1.name, att1);
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(saved);
    ```
    
1. Update the attachments without changing the body, remove all attachments
   by setting `null` for attachments map.
    ```java
    DocumentRevision saved; // a document we saved earlier
    saved.setAttachments(null);
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(saved);
    ```
    
1. Update the attachments without changing the body, remove all attachments
   by setting an empty dictionary.
    ```java
    DocumentRevision saved; // a document we saved earlier
    saved.setAttachments(new HashMap<String, Attachment>());
    
    DocumentRevision updated = datastore.updateDocumentFromRevision(saved);
    ```
    
1. Copy an attachment from one document to another.
    ```java
    DocumentRevision rev = new DocumentRevision("doc1");
    rev.setBody(DocumentBodyFactory.create( ... ));
    UnsavedFileAttachment att1 = ...
    rev.getAttachments().put(att1.name, att1);
    DocumentRevision revWithAttachments = datastore.createDocumentFromRevision(rev);
    
    // Add attachment to "saved" from "revWithAttachments"
    DocumentRevision saved; // a document we saved earlier
    Attachment savedAttachment = revWithAttachments.getAttachments().get("nameOfAttachment");
    saved.getAttachments().put(savedAttachment.name, savedAttachment);
    
    DocumentRevision updated = datastore.updateFromRevision(saved);
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
