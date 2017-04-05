# Replication Policies

Replication policies work quite differently on Android from Java. Please see the relevant section of this guide 
for [Android](#android-replication-policies) or [Java](#java-replication-policies).

## Java replication policies

On Java, replication policies are much simpler than on Android.  To implement a replication policy it is necessary to create
a subclass of `ReplicationPolicyManager` and call `startReplications()` when your chosen conditions for replications to take
place are met and call `stopReplications()` when you want replications to be stopped.  The `ReplicationPolicyManager` will
ensure that if replications are currently in progress they are not restarted.  See the
`IntervalTimerReplicationPolicyManager` class, which is an example of a policy where replications are triggered at regular
intervals.

## Android replication policies

If your Android app is targeting Android 5.0 (API level 21) or above it is recommended that you use Android's [`JobScheduler`](https://developer.android.com/reference/android/app/job/JobScheduler.html)
rather than the Replication Policies described in this section. Using the `JobScheduler` you can set various conditions
for replication much like with Replication Policies and they make even more efficient use of device resources than Replication Policies.
See the section [Replication using the JobScheduler](#replication-using-the-jobscheduler) for examples and advice regarding using
the `JobScheduler`.

If your app is targeting Android 7.0 (API level 24) or above, the `WifiPeriodicReplicationReceiver` will not work correctly because
apps declaring a `BroadcastReceiver` in their manifest no longer receive the `android.net.conn.CONNECTIVITY_CHANGE` event. In
this case, you must use `JobScheduler`.

If you want your app to run on pre Android 5.0 and post Android 5.0, there are three options:
1. If you are happy to make your app dependent on Google Play Services, you can use the [Firebase JobDispatcher](https://github.com/firebase/firebase-jobdispatcher-android)
to have a `JobScheduler` compatible API that works on older versions of Android.
1. You can use the Android `JobScheduler` on API level 21 and above and Replication Policies for API levels below 21.
See [Mixing JobScheduler and Replication Policies](#mixing-jobscheduler-and-replication-policies).
1. You can use Replication Policies on their own. Although Replication Policies are quite efficient in terms of their
battery and processor use, you will get further benefits in these areas by using the `JobScheduler` where possible.
For this reason, using only Replication Policies is not recommended.

### Overview

Replication policies on Android run in a [`Service`](http://developer.android.com/reference/android/app/Service.html)
to allow them to run independently of your main application and to allow
them to be restarted if they are killed by the operating system.  To use replication policies, you need to subclass
one of the existing replication Service components and implement the specifics of your required replications.

### Creating your service

There are two options when creating your service:

1. If you don't require periodic replications, create a subclass of `ReplicationService`.
2. If you want your replications to repeat at (roughly) fixed time intervals, as well as possibly adding other criteria that
are required for replication, create a subclass of `PeriodicReplicationService`. This class takes care of the
scheduling of periodic alarms to trigger the replications. This also requires you to implement a
[`BroadcastReceiver`](http://developer.android.com/reference/android/content/BroadcastReceiver.html)
that is a subclass of `PeriodicReplicationReceiver`. 

Both Services allow other application components to bind to them. This allows components to be notified when
replication is complete and also allows the rate of any periodic replication required by the application to be varied
depending on whether there are components bound to the service or not. For example, it may be desirable to have an app that
triggers replications every 24 hours in the background when the app is not actively being used, but when the app is being
used and a particular [`Activity`](http://developer.android.com/reference/android/app/Activity.html)
is displayed it may be desirable to replicate every few minutes and then update the
[`Activity`](http://developer.android.com/reference/android/app/Activity.html) to show the data when replication has completed.

#### ReplicationService

`ReplicationService` is an abstract class.

Before replications can begin, you must call `setReplicators(Replicator[])` on your concrete implementation to set the
array of `Replicator` objects that define the replications you want to invoke. Any commands received by the `ReplicationService`
(e.g. starting or stopping replications) before `setReplicators(Replicator[])` has been called will be queued and only
processed once `setReplicators(Replicator[])` has been called.

#### PeriodicReplicationService

This abstract class is a child of `ReplicationService` and requires you to implement the following abstract methods:
* `protected abstract int getBoundIntervalInSeconds()` This method should return the interval (in seconds) you wish to have between replications when components are bound to the Service.
* `protected abstract int getUnboundIntervalInSeconds()` This method should return the interval (in seconds) you wish to have between replications when components are not bound to the Service.
* `protected abstract boolean startReplicationOnBind()` This should return `true` if you wish to have replications triggered immediately when a component binds to the Service.

Note that internally this uses [android.app.AlarmManager.setInexactRepeating()](http://developer.android.com/reference/android/app/AlarmManager.html#setInexactRepeating(int, long, long, android.app.PendingIntent)) 
to schedule the repetition of the replications at the given intervals in a battery efficient way. This means the intervals between replications will not be exact.

Before replications can begin, you must call `setReplicators(Replicator[])` on your concrete implementation to set the
array of `Replicator` objects that define the replications you want to invoke. Any commands received by the `ReplicationService`
(e.g. starting or stopping replications) before `setReplicators(Replicator[])` has been called will be queued and only
processed once `setReplicators(Replicator[])` has been called.

#### PeriodicReplicationReceiver

`PeriodicReplicationReceiver` uses a 
[`WakefulBroadcastReceiver`](http://developer.android.com/reference/android/support/v4/content/WakefulBroadcastReceiver.html)
to trigger periodic replications at the intervals specified by your
`PeriodicReplicationService`. This means your application does not have to keep running to trigger replications at the
intervals you require, but will be restarted at the time a replication is required. This class also handles resetting of
the periodic replications after a reboot of the device. You must add the following
[`<action>`](http://developer.android.com/guide/topics/manifest/action-element.html) elements to the
[`<intent-filter>`](http://developer.android.com/guide/topics/manifest/intent-filter-element.html) of the 
`PeriodicReplicationReceiver`'s entry in the `AndroidManifest.xml` to enable periodic replication:

```xml
<action android:name="com.cloudant.sync.replication.PeriodicReplicationReceiver.Alarm" />
<action android:name="android.intent.action.BOOT_COMPLETED" />
```

### Controlling the replication service

To control the replication service you start the service passing an Extra in the `Intent` used to start the service whose
key is `ReplicationService.EXTRA_COMMAND`, and whose value is one of:

* `ReplicationService.COMMAND_START_REPLICATION` This starts the replicators.
* `ReplicationService.COMMAND_STOP_REPLICATION` This stops replicators in progress.
* `PeriodicReplicationService.COMMAND_START_PERIODIC_REPLICATION` This starts the periodic replications when you are using the `PeriodicReplicationService`.
The replications will occur immediately the first time this message is sent or if they were previously stopped explicitly by sending
`PeriodicReplicationService.COMMAND_STOP_PERIODIC_REPLICATION`. However, if replications were previously stopped implicitly (e.g.
by rebooting the device), then the existing replication schedule will be resumed.
* `PeriodicReplicationService.COMMAND_STOP_PERIODIC_REPLICATION` This stops the periodic replications when you are using the `PeriodicReplicationService`.
* `PeriodicReplicationService.COMMAND_DEVICE_REBOOTED` This resets the periodic replications after the device has rebooted when you are using the
`PeriodicReplicationService`. This will be automatically called if your subclass of `PeriodicReplicationReceiver` calls through to the `onReceive()` method of `PeriodicReplicationReceiver`.
* `PeriodicReplicationService.COMMAND_RESET_REPLICATION_TIMERS` This re-evaluates the timers used by the `PeriodicReplicationService` by calling
`getBoundIntervalInSeconds()` or `getUnboundIntervalInSeconds()`. This is useful if you allow the replication interval to be dynamically changed.
For example, if you allow users to change the replication intervals in your app's settings (which should cause your implementations of
`getBoundIntervalInSeconds()` and/or `getUnboundIntervalInSeconds()` to return a different result after a change), once the change has been made,
you should send an `Intent` with this Extra so that the changes are applied to the `PeriodicReplicationService` immediately.

For example, from a subclass of `PeriodicReplicationReceiver`, you might call:

```java
Intent intent = new Intent(context.getApplicationContext(), MyReplicationService.class);
intent.putExtra(ReplicationService.EXTRA_COMMAND, PeriodicReplicationService.COMMAND_START_PERIODIC_REPLICATION);
startWakefulService(context, intent);
```
Note the use of `startWakefulService()` which gets a [`WakeLock`](http://developer.android.com/reference/android/os/PowerManager.WakeLock.html)
to ensure the device does not go to sleep while your replication is in progress.

If you want to start replications from anywhere other than a `PeriodicReplicationReceiver` you would need to replace
`startWakefulService(context, intent)` with `startService(intent)` in the above example and do any
[`WakeLock`](http://developer.android.com/reference/android/os/PowerManager.WakeLock.html)
management you require yourself.

If you need to create a custom subclass of `ReplicationService` and you add additional commands passed using the `ReplicationService.EXTRA_COMMAND`
key, you should use command IDs above 99. The IDs 99 and below are reserved for internal use.

#### WifiPeriodicReplicationReceiver

[`WifiPeriodicReplicationReceiver`](cloudant-sync-datastore-android/src/main/java/com/cloudant/sync/replication/WifiPeriodicReplicationReceiver.java)
is an example of how to extend `PeriodicReplicationReceiver` to add logic that triggers periodic replications
only when our device is connected to Wifi and stops periodic replications when we disconnect from Wifi.
This is done by extending the `PeriodicReplicationReceiver` so that the 
[`BroadcastReceiver`](http://developer.android.com/reference/android/content/BroadcastReceiver.html) responds to changes in
network connectivity.  When we detect that the device has connected to Wifi we start our PeriodicReplicationService
by sending it the command `PeriodicReplicationService.COMMAND_START_PERIODIC_REPLICATION` and when the device disconnects
from Wifi we stop the periodic replications by sending `PeriodicReplicationService.COMMAND_STOP_PERIODIC_REPLICATION` to the
[`Service`](http://developer.android.com/reference/android/app/Service.html).
 Other `Intent` actions received by the broadcast receiver are passed to parent class so that periodic alarms and
resetting of the periodic replications after reboot are handled correctly.


### Example

Lets assume we wish to configure a replication policy as follows:

* We only ever want replications to occur when the device is connected to a WiFi network.
* We want to do sync replications (pull and push).
* When the app is not displaying data to the user we want replications to occur once every 24 hours to keep the data on the device fairly fresh.
* When the app is displaying data to the user we want replications to occur every 5 minutes so the data displayed to the user is only ever a few minutes out of date if we're on Wifi.
* When the app is displaying data to the user we want to refresh the UI to display the new data when the pull replication has completed.
* After the device has rebooted, we want replications to continue in the same way as prior to the reboot.

To demonstrate how we configure this policy, we'll use the `WifiPeriodicReplicationReceiver` to trigger our replications
when the device is connected to Wifi.

#### BroadcastReceiver

Our subclass of `WifiPeriodicReplicationReceiver` is very simple and only needs to configure the name of the
subclass of `PeriodicReplicationService` that we want our [`BroadcastReceiver`](http://developer.android.com/reference/android/content/BroadcastReceiver.html)
to interact with by creating a default constructor that
passes it to the superclass's constructor. We'll be calling our service `MyReplicationService`, so our 
[`BroadcastReceiver`](http://developer.android.com/reference/android/content/BroadcastReceiver.html) looks like:

```java 
public class MyWifiPeriodicReplicationReceiver extends WifiPeriodicReplicationReceiver<MyReplicationService> {

    public MyWifiPeriodicReplicationReceiver() {
        super(MyReplicationService.class);
    }

}
```

#### Service

Our service must configure the name of our [`BroadcastReceiver`](http://developer.android.com/reference/android/content/BroadcastReceiver.html)
(i.e. `MyWifiPeriodicReplicationReceiver`) and does this
in the default constructor by passing the class's name to the superclass constructor. This allows the super class to invoke
`MyWifiPeriodicReplicationReceiver` when the periodic replications are due. We must also implement the abstract methods:

```java
public class MyReplicationService extends PeriodicReplicationService {

    public static int PUSH_REPLICATION_ID = 0;
    public static int PULL_REPLICATION_ID = 1;

    private static final String TAG = "MyReplicationService";

    private static final String TASKS_DOCUMENT_STORE_NAME = "tasks";
    private static final String DOCUMENT_STORE_DIR = "data";

    public MyReplicationService() {
        super(MyWifiPeriodicReplicationReceiver.class);
    }

    @Override
    public void onCreate() {
        super.onCreate();

        try {
            URI uri = new URI("https", "my_api_key:my_api_secret", "myaccount.cloudant.com", 443, "/" + "mydb", null, null);

            File path = context.getApplicationContext().getDir(
                DOCUMENT_STORE_DIR,
                Context.MODE_PRIVATE
            );

            DocumentStore documentStore = null;
            try {
                documentStore = DocumentStore.getInstance(new File(path, TASKS_DOCUMENT_STORE_NAME));
            } catch (DocumentStoreNotOpenedException dsnoe) {
                Log.e(TAG, "Unable to open DocumentStore", dsnoe);
            }

            Replicator pullReplicator = ReplicatorBuilder.pull().from(uri).to(documentStore).withId(PULL_REPLICATION_ID).build();
            Replicator pushReplicator = ReplicatorBuilder.push().to(uri).from(documentStore).withId(PUSH_REPLICATION_ID).build();

            // Replications will not begin until setReplicators(Replicator[]) is called.
            setReplicators(new Replicator[]{pullReplicator, pushReplicator});
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected int getBoundIntervalInSeconds() {
        return 5 * 60; // 5 minutes
    }

    @Override
    protected int getUnboundIntervalInSeconds() {
        return 24 * 60 * 60; // 24 hours
    }

    @Override
    protected boolean startReplicationOnBind() {
        // Trigger replications when a client binds to the service only if we're on WiFi.
        return WifiPeriodicReplicationReceiver.isConnectedToWifi(this);
    }
}
```

Note that we set IDs on the replications so that these IDs can be used to identify the replication that has completed or errored.

The service will not start replications until `setReplicators(Replicator[])` is called, and any commands sent to the `ReplicationService`
prior to `setReplicators(Replicator[])` being called will be queued.

If you needed to obtain credentials for the replications asynchronously (e.g. from a remote service) you could achieve this by, for example,
starting an `AsyncTask` in the `onCreate()` method and calling `setReplicators(Replicator[])` in the `AsyncTask`'s `onPostExecute()` method - e.g.:

```java
@Override
public void onCreate() {
    super.onCreate();

    new AsyncTask<Void, Void, Replicator[]>() {
        @Override
        protected Replicator[] doInBackground(Void... params) {
            // Fetch the credentials needed for replication and set up the replicators.
            // ...

            // Return the array of replicators you have configured.
            return replicators;
        }

        @Override
        protected void onPostExecute(Replicator[] replicators) {
            setReplicators(replicators);
        }

    }.execute();
}
```

#### AndroidManifest.xml

Now, we need to make sure our `AndroidManifest.xml` is updated to contain the 
[`BroadcastReceiver`](http://developer.android.com/reference/android/content/BroadcastReceiver.html) and 
[`Service`](http://developer.android.com/reference/android/app/Service.html) we've created. We need
to also ensure the [`BroadcastReceiver`](http://developer.android.com/reference/android/content/BroadcastReceiver.html)
has the correct `intent-filter` settings.

Our [`BroadcastReceiver`](http://developer.android.com/reference/android/content/BroadcastReceiver.html)
needs to know when the connectivity of the device has changed, when a periodic replication is due
and when the device has rebooted. We also don't want other apps to invoke our 
[`BroadcastReceiver`](http://developer.android.com/reference/android/content/BroadcastReceiver.html)
so we set `android:exported="false"`, so we add the following to our manifest file:

```xml
<receiver android:name=".MyWifiPeriodicReplicationReceiver" android:exported="false">
    <intent-filter>
        <action android:name="android.net.conn.CONNECTIVITY_CHANGE" />
        <action android:name="com.cloudant.sync.replication.PeriodicReplicationReceiver.Alarm" />
        <action android:name="android.intent.action.BOOT_COMPLETED" />
    </intent-filter>
</receiver>
```

We also add our service, again we don't want other apps to interact with our service so we add `android:exported="false"`

```xml
<service android:name=".MyReplicationService"
         android:exported="false" />
```

We must also request the following permissions so that our periodic replications can run:

```xml
<!-- Replications require Internet access. -->
<uses-permission android:name="android.permission.INTERNET" />
<!-- We want to know when we're on Wifi so we only replicate when we are. -->
<uses-permission android:name="android.permission.ACCESS_NETWORK_STATE" />
<!-- We don't want the device to go to sleep while a replication is in progress. -->
<uses-permission android:name="android.permission.WAKE_LOCK" />
<!-- We want to restart our periodic replications after a reboot. -->
<uses-permission android:name="android.permission.RECEIVE_BOOT_COMPLETED" />
```

#### Binding to the Service

Lets assume we have an [`Activity`](http://developer.android.com/reference/android/app/Activity.html)
that displays our data to the user. While this [`Activity`](http://developer.android.com/reference/android/app/Activity.html)
is displayed we want our more frequent updates (every 5 minutes), and we want to know when replication has completed so we can refresh the Activity's UI with the new data. Note that the [`Activity`](http://developer.android.com/reference/android/app/Activity.html)
must be running in the same process as the [`Service`](http://developer.android.com/reference/android/app/Service.html), which is the default on Android.

First we add some fields to our [`Activity`](http://developer.android.com/reference/android/app/Activity.html):
```java
// Add a handler to allow us to post UI updates on the main thread.
private final Handler mHandler = new Handler(Looper.getMainLooper());

// Reference to our service.
private ReplicationService mReplicationService;

// Flag indicating whether the Activity is currently bound to the Service.
private boolean mIsBound;
```

Now we add a [`ServiceConnection`](http://developer.android.com/reference/android/content/ServiceConnection.html)
to our [`Activity`](http://developer.android.com/reference/android/app/Activity.html)
to allow us to handle binding and unbinding from our `MyReplicationService`. This enables us to get a reference to the service when we bind to it and add a listener for `replicationCompleted` to the [`Service`](http://developer.android.com/reference/android/app/Service.html):

```java
private ServiceConnection mConnection = new ServiceConnection() {
    @Override
    public void onServiceConnected(ComponentName name, IBinder service) {
        mReplicationService = ((ReplicationService.LocalBinder) service).getService();
        mReplicationService.addListener(new PolicyReplicationsCompletedListener.SimpleListener() {
            @Override
            public void replicationCompleted(int id) {
                // Check if this is the pull replication
                if (id == MyReplicationService.PULL_REPLICATION_ID) {
                    mHandler.post(new Runnable() {
                        @Override
                        public void run() {
                            // Update the UI.
                        }
                    });
                }
            }

            @Override
            public void replicationErrored(int id) {
                // We'll just ignore the error.
            }
        });
    }

    @Override
    public void onServiceDisconnected(ComponentName name) {
        mReplicationService = null;
    }
};
```
We now bind to the service in [`Activity.onStart()`](http://developer.android.com/reference/android/app/Activity.html#onStart())
and unbind in [`Activity.onStop()`](http://developer.android.com/reference/android/app/Activity.html#onStop()):

```java
@Override
protected void onStart() {
    super.onStart();
    bindService(new Intent(this, MyReplicationService.class), mConnection, Context.BIND_AUTO_CREATE);
    mIsBound = true;
}

@Override
protected void onStop() {
    super.onStop();
    if (mIsBound) {
        unbindService(mConnection);
        mIsBound = false;
    }
}
```

# Replication using the JobScheduler

The [`JobScheduler`](https://developer.android.com/reference/android/app/job/JobScheduler.html) is a 
standard part of the Android API, so there is plenty of information available about how to use it.

## Example
The following example shows how we could configure the JobScheduler to perform a replication where:
* We want replications to occur roughly every hour.
* We only ever want replications to occur when the device is connected to an unmetered network (e.g. WiFi).
* We want to do sync replications (pull and push).
* After the device has rebooted, we want replications to continue in the same way as prior to the reboot.

### JobService

First, we create a [`JobService`](https://developer.android.com/reference/android/app/job/JobService.html)
that performs the replications. In this example we do both a pull and a push replication. We also use a
static `EventBus` and repost the events from the replicator's event buses onto this event bus.
We could, for example, register an Activity with the event bus so that the Activity can be notified
when pull replications are complete so we can update the UI.


```java
public class MyJobService extends JobService {

    public static int PUSH_REPLICATION_ID = 0;
    public static int PULL_REPLICATION_ID = 1;

    public static final String TAG = "MyJobService";

    private static final String DOCUMENT_STORE_DIR = "data";
    private static final String DOCUMENT_STORE_NAME = "tasks";

    private ReplicationTask mReplicationTask = new ReplicationTask();

    private JobParameters mJobParameters;

    private static EventBus sEventBus = new EventBus();

    // Create a simple listener that can be attached to the replications so that we can wait
    // for all replications to complete.
    public class Listener {

        private final CountDownLatch latch;

        Listener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Subscribe
        public void complete(ReplicationCompleted event) {
            latch.countDown();
            sEventBus.post(event);
        }

        @Subscribe
        public void error(ReplicationErrored event) {
            latch.countDown();
        }
    }

    // Use an AsyncTask to run the replications off the main thread.
    class ReplicationTask extends AsyncTask<Void, Void, Void> {
        @Override
        protected Void doInBackground(Void... params) {
            try {
                URI uri = new URI("https", "my_api_key:my_api_secret", "myaccount.cloudant.com", 443, "/" + "mydb", null, null);

                File path = getApplicationContext().getDir(
                    DOCUMENT_STORE_DIR,
                    Context.MODE_PRIVATE
                );

                DocumentStore documentStore = null;
                try {
                    documentStore = DocumentStore.getInstance(new File(path, DOCUMENT_STORE_NAME));
                } catch (DocumentStoreNotOpenedException dsnoe) {
                    Log.e(TAG, "Unable to open DocumentStore", dsnoe);
                }

                Replicator pullReplicator = ReplicatorBuilder.pull().from(uri).to(documentStore).withId
                    (PULL_REPLICATION_ID).build();
                Replicator pushReplicator = ReplicatorBuilder.push().to(uri).from(documentStore).withId
                    (PUSH_REPLICATION_ID).build();

                // Setup the CountDownLatch for our two replications.
                CountDownLatch latch = new CountDownLatch(2);
                Listener listener = new Listener(latch);
                pullReplicator.getEventBus().register(listener);
                pullReplicator.start();
                pushReplicator.getEventBus().register(listener);
                pushReplicator.start();

                // Wait for both replications to complete.
                latch.await();
                pullReplicator.getEventBus().unregister(listener);
                pushReplicator.getEventBus().unregister(listener);
            } catch (URISyntaxException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                Log.i(TAG, "ReplicationTask has been cancelled");
                e.printStackTrace();
            }
            return null;
        }

        @Override
        protected void onPostExecute(Void aVoid) {
            // Replications finished successfully.
            jobFinished(mJobParameters, false);
        }

        @Override
        protected void onCancelled(Void aVoid) {
            // Replications were cancelled. Request rescheduling.
            jobFinished(mJobParameters, true);
        }
    }

    @Override
    public boolean onStartJob(JobParameters jobParameters) {
        mJobParameters = jobParameters;

        if (!jobParameters.isOverrideDeadlineExpired()) {
            mReplicationTask.execute();
        } else {
            // An undocumented feature of the JobScheduler is that for a periodic job it
            // will call onStartJob at the end of the period regardless of whether
            // the other conditions for the job are met. However, when it does it
            // for this reason, jobParameters.isOverrideDeadlineExpired() will
            // be true. Since we only want to replicate if all the conditions for
            // the job are true, we just ignore this case and jobFinished().
            jobFinished(mJobParameters, false);
        }

        // Work is being done on a separate thread.
        return true;
    }

    @Override
    public boolean onStopJob(JobParameters jobParameters) {
        mReplicationTask.cancel(true);

        // We want the job rescheduled next time the conditions for execution are met.
        return true;
    }

    public static EventBus getEventBus() {
        return sEventBus;
    }
}
```

### Configuring the JobScheduler

We also need to configure the JobScheduler to run `MyJobService` when the relevant
conditions for replication are met:

```java
private static final int MY_JOB_ID = 0;

...

ComponentName jobServiceComponent = new ComponentName(this, MyJobService.class);
JobInfo.Builder builder = new JobInfo.Builder(MY_JOB_ID, jobServiceComponent);
builder.setPersisted(true);  // Persist across reboots
builder.setPeriodic(60 * 60 * 1000); // 1 hour
builder.setRequiredNetworkType(JobInfo.NETWORK_TYPE_UNMETERED);

JobScheduler jobScheduler = (JobScheduler) getSystemService(Context.JOB_SCHEDULER_SERVICE);
jobScheduler.schedule(builder.build());
```

### AndroidManifest.xml

Now we add our [`JobService`](#JobService) to the `AndroidManifest.xml`:

```xml
<service android:name=".MyJobService"
         android:permission="android.permission.BIND_JOB_SERVICE"
         android:exported="false"/>
```

Because we want our `JobService` to persist across reboots, our app must have the following permission declared in the `AndroidManifest.xml`:

```xml
<uses-permission android:name="android.permission.RECEIVE_BOOT_COMPLETED"/>
```

## Mixing JobScheduler and Replication Policies

If you want to use a combination of Replication Policies and `JobScheduler`, follow the [Replication Policy](#android-replication-policies)
guidance above and everywhere you send a message to your `ReplicationService`, make a conditional call dependent on API
level to start/cancel/restart the `JobScheduler` with your `JobService` with any necessary changes to the configuration
of your `JobService`.

To make this easier, we declare methods to start and cancel our JobScheduler tasks. You may wish to make them
static and add parameters to allow changes to be made to the setup of the job, e.g.:

```java
@TargetApi(21)
public static void startJobService(Context context, long period) {
    ComponentName jobServiceComponent = new ComponentName(context, MyJobService.class);
    JobInfo.Builder builder = new JobInfo.Builder(MY_JOB_ID, jobServiceComponent);
    builder.setPersisted(true);  // Persist across reboots
    builder.setPeriodic(period);
    builder.setRequiredNetworkType(JobInfo.NETWORK_TYPE_UNMETERED);

    JobScheduler jobScheduler = (JobScheduler) context.getSystemService(Context.JOB_SCHEDULER_SERVICE);
    jobScheduler.schedule(builder.build());
}

@TargetApi(21)
public static void cancelJobService(Context context) {
    JobScheduler jobScheduler = (JobScheduler) context.getSystemService(Context.JOB_SCHEDULER_SERVICE);
    jobScheduler.cancel(MY_JOB_ID);
}
```

Then, for example, to start a periodic replication we execute different code depending on the API level
running on the device:

```java
if (Build.VERSION.SDK_INT >= 21) {
    // Use the JobScheduler.
    long periodMilliseconds = 60 * 60 * 1000; // 1 hour
    startJobService(context, periodMilliseconds);
} else {
    // Use replication policies.
    Intent intent = new Intent(getApplicationContext(), MyReplicationService.class);
    intent.putExtra(ReplicationService.EXTRA_COMMAND, PeriodicReplicationService.COMMAND_START_PERIODIC_REPLICATION);
    startService(intent);
}
```

If the periodicity of your replications is stored in shared preferences, the implementation of your `ReplicationService`
for your Replication Policy might contain something like:

```java
@Override
protected int getUnboundIntervalInSeconds() {
    return Integer.valueOf(PreferenceManager.getDefaultSharedPreferences(this).getString("unbound_period", "0"));
}
```

Then, when the shared preference has changed, you will probably want to cancel and restart the JobScheduler
and force the Replication Policy to re-evaluate the periodicity depending on the Android API level, for example:
```java
if (Build.VERSION.SDK_INT >= 21) {
    // Use the JobScheduler.
    cancelJobService(context);
    startJobService(context, Integer.valueOf(PreferenceManager.getDefaultSharedPreferences(this).getString("unbound_period", "0")));
} else {
    // Use replication policies.
    Intent intent = new Intent(getApplicationContext(), MyReplicationService.class);
    intent.putExtra(ReplicationService.EXTRA_COMMAND, PeriodicReplicationService.COMMAND_RESET_REPLICATION_TIMERS);
    startService(intent);
}

```

