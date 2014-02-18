# Handling conflicts

_This functionality is available in versions 0.3.0 and up._

An obvious repercussion of being able to replicate documents about the place
is that sometimes you might edit them in more than one place at the same time.
When the databases containing these concurrent edits are replicated, there needs
to be some way to bring these divergent documents back together. Cloudant's
MVCC data-model is used to do this. This page describes how it works.

## Replication

The Sync datastore participates in master-less replication with
[Cloudant][cloudant]
or [Apache CouchDB][couch]. What this means is that there is no canonical
copy of the documents in each database. One of the main results of this
fact is that changes may happen to a document in many different places
concurrently. When these changes are replicated between previously
disconnected databases, conflicts arise. However, Cloudant and Cloudant
Sync provide ways to both access and resolve conflicts from within
your application.

It's important to understand that this data model is in place to make sure
that:

- The user loses no data -- we keep all versions of a document that
  haven't been superceded. That is, all leaf nodes of the tree.
- The application has as much information as possible to resolve the
  conflicts, as it's able to examine all of the leaf nodes of the
  tree before resolving a conflict.

Cloudant Sync's MVCC data layer is key to the conflict resolution process,
and can be visualised as a tree structure.

[couch]: http://couchdb.org/
[cloudant]: https://cloudant.com/

## The document tree

A document is really a tree of the document and its history. This is neat
because it allows us to store multiple versions of a document. In the main,
there's a single, linear tree -- just a single branch -- running from the
creation of the document to the current revision. This is the usual case,
and looks like this, with the revisions represented by their revision IDs:

```
1-x  ---  2-x  ---  3-x ---  4-x
                              ^
          "winning" revision /
```

The fact that the document
is a tree implies that it's possible, however, to create further branches
in the tree.

### What are conflicts?

When a document has been replicated to more than one place, it's possible to
edit it concurrently in two places. When the datastores storing the document
then replicate with each other again, they each add their changes to the
document's tree. This causes an extra branch to be added to the tree for
each concurrent set of changes. When this happens, the document is said to be
_conflicted_. This creates multiple current revisions of the document, one for
each of the concurrent changes.

Say we last replicated the document above at the `2-x` revision. We make
two changes locally (`3-x` and `4-x`) and the remote datastore has a single
change made to it (`3-y`). On replicating back from the remote, the local
datastore ends up with a document like this:

```
  replicated from remote
                     |
                     v
             ------ 3-y
            /
1-x  ---  2-x  ---  3-x  ---  4-x
                               ^
           "winning" revision /
```

We now have two non-deleted leaf nodes: the document is conflicted.

### The "winning" revision

To make things easier, calling `Datastore#getDocument(...)` returns one of
the leaf nodes of the branches of the conflicted document. It selects the
node to return in an arbitrary but deterministic way, which means that all
replicas of the database will return the same revision for the document. The
other copies of the document are still there in the case of conflicts,
however, waiting to be merged, as shown below.

See more information on document trees in the javadocs for
`DocumentRevisionTree`.

## Resolving conflicts in code

When a document has been changed in many places, it becomes
conflicted. This means that there are a number of active, alternative
versions of the document. Applications -- whether on device or a web app
communicating with the Cloudant or CouchDB HTTP interfaces -- must
resolve the conflicts by creating a merged version of the active versions
of the document, then updating the document with this and deleting the now
obsolete leaf nodes.

Fortunately, Cloudant Sync has helper methods to simplify this. There's
a method which returns all the documents in a conflicted state, along with
a helper method to streamline the process of resolving conflicts.

### Finding conflicted documents

There's a method on the `Datastore` interface:

```java
Iterator<String> getConflictedDocumentIds();
```

This method returns an iterator over the document IDs:

```java
for (String docId : datastore.getConflictedDocumentIds()) {
    System.out.println(docId);
}
```

### Resolving the conflicts

Once you've found the list of documents, you need to resolve them. This is
done one-by-one, passing a class able to resolve conflicts and a document
ID to the `resolveConflictsForDocument(String, ConflictResolver)` method
of the `Datastore` interface.

The `ConflictResolver` interface has one method:

```java
interface ConflictResolver {
    DocumentRevision resolve(String docId, List<DocumentRevision> conflicts);
}
```

This method is passed the docId and the list of active revisions, including
the current winning revision. A rather simplistic implementation would be:

```java
class PickFirstResolver implements ConflictResolver {
    DocumentRevision resolve(String docId, List<DocumentRevision> conflicts) {
        return conflicts.get(0);
    }
}
```

Clearly, in the general case this will discard the user's data(!),
but it'll do for this example.

Conceptually, the `resolveConflictsForDocument` method does the following:

1. Get all the non-deleted leaf node revisions for the document.

    ```
                 ------ 3-y
                /
    1-x  ---  2-x  ---  3-x  ---  4-x
    ```

    That's `3-y` and `4-x` here.

2. Call `resolve` with the list of revisions from (1).
3. Take the returned revision and update the current winning revision (`4-x`)
   with this revision.
4. Delete the other non-deleted leaf nodes (`3-y` in this case) of the
   document tree.

The tree ends up looking like this:

```
             ------ 3-y  ---  4-deleted
            /
1-x  ---  2-x  ---  3-x  ---  4-x  ---  5-x
                                         ^
                     "winning" revision /
```

The winning revision is now the only non-deleted leaf node, so the document
is no longer conflicted.

All this happens inside a transaction, ensuring consistency.

This resolution can be replicated to the remote document store, bringing
the two databases into a consistent state.

### Simple example

You could imagine an application running the following method
via a timer to periodically fix up any conflicts:

```java
public void resolveConflicts(Datastore datastore) {
    ConflictResolver pickFirst = new PickFirstResolver();
    for (String docId : datastore.getConflictedDocumentIds()) {
        datastore.resolveConflictsForDocument(docId, pickFirst);
    }
}
```

How often this should run depends on your application, but you'd probably
want to consider:

- Running every few minutes.
- Running when a pull replication completes.

We're always looking at ways to improve the experience around conflicts,
so be sure to file an issue if you have suggestions or problems.
