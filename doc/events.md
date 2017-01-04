# Events

Note that prior to version 1.0.0 the Google Guava EventBus (`com.google.common.eventbus` package)
API was used, but now there is an API provided in the `com.cloudant.sync.event` package. To migrate
your code you will need to adjust your imports accordingly.

Cloudant Sync uses an `EventBus` to post events to interested parties in a publish and subscribe
fashion.

The `Database` class posts events about Documents:

* `DocumentCreated`
* `DocumentDeleted`
* `DocumentUpdated`.

The `DocumentStore` class posts events about DocumentStores:

* `DocumentStoreClosed`
* `DocumentStoreCreated`
* `DocumentStoreDeleted`.

There are also generic `Modified` events for each class: `DocumentModified` and `DocumentStoreModified`
 (more about these later).

There are also events posted about replications:

* `ReplicationCompleted`
* `ReplicationErrored`

To subscribe to an event, first register the object whose methods you want to be called (in this
case, an instance of `DocumentNotificationClient`):

```java
Database database;
EventBus eventBus = database.getEventBus();
DocumentNotificationClient dnc = new DocumentNotificationClient();
eventBus.register(dnc);
```

Next, add the methods you want to be called when each event occurs and decorate them with the
`@Subscribe` annotation. These methods and the containing class must be marked public. This means
that anonymous inner classes cannot be used as event subscribers.

Here the `DocumentNotificationClient` is subscribing to the `DocumentCreated` event. Because the
events are just classes, they obey the type hierarchy. This means that it is also possible to
subscribe to the generic `DocumentModified` event to be notified of all Created/Deleted/Updated
events and then use reflection and downcasting if needed. However if you want to listen for multiple 
event types eg `DocumentCreated` and `DatastoreCreated` using the same subscriber method, the 
`Notification` marker interface needs to be used instead.

```java
public class DocumentNotificationClient {
    @Subscribe
    public void onDocumentCreated(DocumentCreated dc) {
        // do something with the event...
    }
}
```

For `DocumentStore` events there is a similar superclass: `DocumentStoreModified`.

Whether to use these generic events or one method per event type is simply a matter of style.

Note that subscribing methods are invoked synchronously. This means that it is important to
return quickly from the subscribing method - otherwise performance of the `Database` or
`DocumentStore` could be impaired.

If the subscribing method needs to start long-running tasks, we recommend you use an
asynchronous approach such as spawning a thread and returning immediately.
