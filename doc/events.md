# Events

_This functionality is available in versions 0.3.0 and up._

Cloudant Sync uses [EventBus](https://code.google.com/p/guava-libraries/wiki/EventBusExplained) from
the Google Guava library to post events to interested parties in a publish and subscribe fashion.

The `Datastore` class posts events about Documents:

* `DocumentCreated`
* `DocumentModified`
* `DocumentDeleted`.

The `DatastoreManager` class posts events about Databases:

* `DatabaseCreated`
* `DatabaseDeleted`.

To subscribe to an event, first register the object whose methods you want to be called (in this
case, an instance of `DocumentNotificationClient`):

```java
Datastore datastore;
EventBus eventBus = datastore.getEventBus();
DocumentNotificationClient dnc = new DocumentNotificationClient();
eventBus.register(dnc);
```

Next, add the methods you want to be called when each event occurs and decorate them with the
`@Subscribe` annotation.

Here the `DocumentNotificationClient` is subscribing to the `DocumentCreated` event. Because the
events are just classes, they obey the type hierarchy. This means that it is also possible to
subscribe to the generic `DocumentChanged` event to be notified of all Created/Modified/Deleted
events and then use reflection and downcasting if needed.

```java
public class DocumentNotificationClient {
    @Subscribe
    public void onDocumentCreated(DocumentCreated dc) {
        // do something with the event...
    }
}
```

For `DatastoreManger` events there is a similar superclass: `DatabaseChanged`.

Whether to use these generic events or one method per event type is simply a matter of style.

Note that subscribing methods are invoked synchronously. This means that it is important to
return quickly from the subscribing method - otherwise performance of the `Datastore` or
`DatastoreManager` could be impaired.

If the subscribing method needs to start long-running tasks, we recommend you use an
asynchronous approach such as spawning a thread and returning immediately.
