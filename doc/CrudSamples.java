/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

import com.cloudant.sync.documentstore.Attachment;
import com.cloudant.sync.documentstore.DocumentBodyFactory;
import com.cloudant.sync.documentstore.DocumentRevision;
import com.cloudant.sync.documentstore.DocumentStore;
import com.cloudant.sync.documentstore.UnsavedFileAttachment;
import com.cloudant.sync.documentstore.UnsavedStreamAttachment;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * Code samples to show how to use CRUD (create, read, update, delete)
 * features of the library
 */
public class CrudSamples {

    public void crudSamples() throws Exception {
        
        // DocumentStore and Database:
        // The examples below show how to obtain a DocumentStore instance how to obtain the Database
        // object it owns.

        // Choose a path for your DocumentStores
        File path = new File("/tmp");
        // on Android, we could do something like:
        // File path = getApplicationContext().getDir("document_stores");

        // Once you've got a path, it's straightforward to create DocumentStores:

        DocumentStore ds = DocumentStore.getInstance(new File(path, "my_document_store"));
        DocumentStore ds2 = DocumentStore.getInstance(new File(path, "other_document_store"));

        // Static getInstance() methods on DocumentStore will create and initial a DocumentStore
        // instance if it doesn't already exist, or retrieve an existing one.

        // To close a DocumentStore (which closes the underlying database files and release native
        // resources):
        ds.close();

        // To delete a DocumentStore (which closes it first):
        ds.delete();

        // Document CRUD APIs

        // Once you have a DocumentStore instance, you can use it to create, update and
        // delete documents

        // Create

        // Documents are represented as a set of revisions. To create a document, you
        // set up the initial revision of the document and save that to the datastore.

        // Create a document revision object, set its body, ID and attachments
        // and then call createDocumentFromRevision(DocumentRevision) to add it to the datastore:

        // Create a document with a document id as the constructor argument
        DocumentRevision rev = new DocumentRevision("doc1");
        // Or don't assign the docId property, we'll generate one
        rev = new DocumentRevision();

        // Build up body content from a Map
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("description", "Buy milk");
        json.put("completed", false);
        json.put("type", "com.cloudant.sync.example.task");
        rev.setBody(DocumentBodyFactory.create(json));

        // Now call create(). Note that we call all of the CRUD methods through the Database
        // instance which is obtained via the database() getter.
        DocumentRevision revision = ds.database().create(rev);

        // The only mandatory property to set before calling
        // createDocumentFromRevision(DocumentRevision) is the body. An ID will be generated
        // for documents which don't have docId set.

        // Read

        // Also known as retrieve. Once you have created one or more documents, retrieve them by ID:

        String docId = revision.getId();
        DocumentRevision retrieved = ds.database().read(docId);

        // This document is mutable and you can make changes to it, as shown below.

        // Update

        // To update a document, make your changes and save the document:

        json = retrieved.getBody().asMap();
        json.put("completed", true);
        retrieved.setBody(DocumentBodyFactory.create(json));

        // Note that "updated" is the new DocumentRevision with a new revision ID
        DocumentRevision updated = ds.database().update(retrieved);

        // Delete

        // To delete a document, you need the current revision:

        ds.database().delete(retrieved);

        // Indexing and Query

        // You don't need to know the ID of the document to retrieve it. The DocumentStore
        // provides ways to index and search the fields of your JSON documents.
        // For more, see https://github.com/cloudant/sync-android/blob/master/doc/query.md.

        // Conflicts

        // Getting all documents

        // The read(int offset, int limit, boolean descending) method allows iterating through
        // all documents in the database:

        // read all documents in one go
        int pageSize = ds.database().getDocumentCount();
        List<DocumentRevision> docs = ds.database().read(0, pageSize, true);

        // Using attachments

        // You can associate attachments with the JSON documents in your datastores.
        // Attachments are blobs of binary data, such as photos or short sound snippets.
        // They should be of small length -- maximum a few MB -- because they are
        // replicated to and from the server in a way which doesn't allow for resuming
        // an upload or download.
        //
        // Attachments are stored via the `attachments` getters/setters on a DocumentRevision
        // object. This is a map of attachments, keyed by attachment name.
        //
        // To add an attachment to a document, just add (or overwrite) the attachment
        // in the `attachments` map:

        // create a body
        rev.setBody(DocumentBodyFactory.create(json));

        // create an UnsavedFileAttachment: the constructor takes
        // a File object on disk and a MIME type
        UnsavedFileAttachment att1 = new UnsavedFileAttachment(
                new File("/path/to/image.jpg"), "image/jpeg");

        // As with the document body, you can replace the attachments
        rev.setAttachments(new HashMap<String, Attachment>());

        // Or just add or update a single one:
        // (because the getter will always return the underlying map and not a copy)
        rev.getAttachments().put(att1.name, att1);

        DocumentRevision saved = ds.database().create(rev);

        // When creating new attachments, use `UnsavedFileAttachment` for data you already have on
        // disk. Use
        // `UnsavedStreamAttachment` when you have data which comes from an InputStream or is
        // already in
        // memory.

        att1 = new UnsavedFileAttachment(
                new File("/path/to/image.jpg"), "image/jpeg");

        byte[] imageData = new byte[256];
        // ByteArrayInputStream adapts imageData to be used as a stream
        UnsavedStreamAttachment att2 = new UnsavedStreamAttachment(
                new ByteArrayInputStream(imageData), "cute_cat.jpg", "image/jpeg");

        rev = new DocumentRevision();
        rev.getAttachments().put(att1.name, att1);
        rev.getAttachments().put(att2.name, att2);
        saved = ds.database().create(rev);

        // To read an attachment, get the `SavedAttachment` from the `attachments`
        // map. Then use `getInputStream()` to read the data:

        retrieved = ds.database().read("myDoc");
        Attachment att = retrieved.getAttachments().get("cute_cat.jpg");
        InputStream is = att.getInputStream();

        // get all bytes in memory if required
        byte[] data = new byte[(int) att.length];
        is.read(data);

        // To remove an attachment, remove it from the `attachments` map:

        retrieved = ds.database().read("myDoc");
        retrieved.getAttachments().remove("cute_cat.jpg");
        updated = ds.database().update(retrieved);

        // To remove all attachments, set the `attachments` property to an empty map
        // or `null`:

        updated.setAttachments(null);

        // ## Cookbook
        //
        // This section shows all the ways (that I could think of) that you can update,
        // modify and delete documents.
        //
        // ### Creating a new document
        //
        // This is the simplest case as we don't need to worry about previous revisions.
        //
        // 1. Add a document with body, but not attachments or ID. You'll get an
        // autogenerated ID.
        DocumentRevision newRev = new DocumentRevision();

        newRev.setBody(DocumentBodyFactory.create(json));

        saved = ds.database().create(newRev);

        // 1. Add a new document to the store with a body and ID, but without attachments.
        newRev = new DocumentRevision("doc1");
        newRev.setBody(DocumentBodyFactory.create(json));

        saved = ds.database().create(newRev);

        // 1. Add a new document to the store with attachments.

        newRev = new DocumentRevision("doc1");
        newRev.setBody(DocumentBodyFactory.create(json));

        att1 = new UnsavedFileAttachment(
                new File("/path/to/image.jpg"), "image/jpeg");
        newRev.getAttachments().put(att1.name, att1);

        saved = ds.database().create(newRev);

        // 1. Add a document with body and attachments, but no ID. You'll get an
        // autogenerated ID.
        newRev = new DocumentRevision();
        newRev.setBody(DocumentBodyFactory.create(json));

        att1 = new UnsavedFileAttachment(
                new File("/path/to/image.jpg"), "image/jpeg");
        rev.getAttachments().put(att1.name, att1);

        saved = ds.database().create(rev);

        // 1. You can't create a document without a body (body is the only required property).
        rev = new DocumentRevision();

        saved = ds.database().create(rev);
        // will throw java.lang.NullPointerException: Input document body can not be null

        // ### Updating a document

        // To update a document, make your changes and save the document.


        // 1. Update body for doc that has no attachments, adding no attachments
        Map<String, Object> updatedJson = new HashMap<String, Object>();
        updatedJson.put("description", "Buy eggs");
        updatedJson.put("completed", true);
        updatedJson.put("type", "com.cloudant.sync.example.task");
        saved.setBody(DocumentBodyFactory.create(updatedJson));

        updated = ds.database().update(saved);

        // 1. Update body for doc with no attachments, adding attachments. The
        // attachments map is accessed and modified via the getAttachments()
        // getter.
        saved.setBody(DocumentBodyFactory.create(updatedJson));
        saved.getAttachments().put(att1.name, att1);

        updated = ds.database().update(saved);

        // 1. Update body and remove all attachments.
        saved.setBody(DocumentBodyFactory.create(updatedJson));
        saved.setAttachments(null);

        updated = ds.database().update(saved);

        // 1. Update the attachments without changing the body, add attachments to a doc
        // that had none.
        saved.getAttachments().put(att1.name, att1);

        updated = ds.database().update(saved);

        // 1. Update attachments by copying from another revision.
        DocumentRevision anotherDoc = ds.database().read("anotherId");
        saved.getAttachments().putAll(anotherDoc.getAttachments());

        updated = ds.database().update(saved);

        // 1. Updating a document using an outdated source revision causes a conflict
        saved.setBody(DocumentBodyFactory.create(updatedJson));
        ds.database().update(saved);

        Map<String, Object> updatedMap2 = new HashMap<String, Object>();
        updatedMap2.put("goodbye", "world");
        saved.setBody(DocumentBodyFactory.create(updatedMap2));

        updated = ds.database().update(saved);
        // throws ConflictException

        // For the second set of examples the original document is set up with a body and
        // several attachments:

        DocumentRevision revWithAttachments = new DocumentRevision("doc1");
        revWithAttachments.setBody(DocumentBodyFactory.create(json));

        revWithAttachments.getAttachments().put(att1.name, att1);
        revWithAttachments.getAttachments().put(att2.name, att2);

        DocumentRevision savedWithAttachments = ds.database().create(revWithAttachments);

        // 1. Update body without changing attachments
        savedWithAttachments.setBody(DocumentBodyFactory.create(updatedJson));

        updated = ds.database().update(savedWithAttachments);
        // Should have the same attachments

        // 1. Update the attachments without changing the body, remove attachments
        saved.getAttachments().remove(att1.name);

        updated = ds.database().update(savedWithAttachments);

        // 1. Update the attachments without changing the body, add attachments
        saved.getAttachments().put(att1.name, att1);

        updated = ds.database().update(savedWithAttachments);

        // 1. Update the attachments without changing the body, remove all attachments
        // by setting `null` for attachments map.
        savedWithAttachments.setAttachments(null);

        updated = ds.database().update(savedWithAttachments);

        // 1. Update the attachments without changing the body, remove all attachments
        // by setting an empty dictionary.
        savedWithAttachments.setAttachments(new HashMap<String, Attachment>());

        updated = ds.database().update(savedWithAttachments);

        // 1. Copy an attachment from one document to another.

        DocumentRevision copiedAttachments = new DocumentRevision();
        copiedAttachments.setBody(DocumentBodyFactory.create(json));
        Attachment toCopy = revWithAttachments.getAttachments().get("image.jpg");
        copiedAttachments.getAttachments().put(toCopy.name, toCopy);
        savedWithAttachments = ds.database().create(copiedAttachments);

        // Add attachment to "saved" from "revWithAttachments"
        Attachment savedAttachment = revWithAttachments.getAttachments().get("nameOfAttachment");
        saved.getAttachments().put(savedAttachment.name, savedAttachment);

        updated = ds.database().update(saved);

        // ### Deleting a document

        // 1. You should be able to delete a given revision (i.e., add a tombstone to the end of
        // the branch).

        saved = ds.database().read("doc1");
        DocumentRevision deleted = ds.database().delete(saved);

        // This would refuse to delete if `saved` was not a leaf node.


        // 1. **Advanced** You should also be able to delete a document in its entirety by passing
        // in an ID.

        List<DocumentRevision> deleteds = ds.database().delete("doc1");

        // This marks *all* leaf nodes deleted. Make sure to read
        // [conflicts.md](conflicts.md) before using this method as it can result
        // in data loss (deleting conflicted versions of documents, not just the
        // current winner).

    }


}
