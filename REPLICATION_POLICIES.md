Replication policies work quite differently on Android from Java. Please see the relevant section of this guide 
for [Android](#android-replication-policies) or [Java](#java-replication-policies).

# Android replication policies
Replication policies on Android run in a `Service` to allow them to run independently of your main application and to allow
them to be restarted if they are killed by the operating system.  To use replication policies, you need to subclass
one of the existing replication Service components and implement the specifics of your required replications.

##Creating your service
There are two options when creating your service. 

1. If you don't require periodic replications, you should create a subclass of `ReplicationService`.
2. If you want your replications to repeat at (roughly) fixed time intervals, as well as possibly adding other criteria that
are required for replication, you should create a subclass of `PeriodicReplicationService`. This class takes care of the
scheduling of periodic alarms to trigger the replications. This also requires you to implement a `BroadcastReceiver` that is a subclass of `PeriodicReplicationReceiver`. 

Both Services allow other application components to bind to them. This allows components to be notified when
replication is complete and also allows the rate of any periodic replication required by the application to be varied
depending on whether there are components bound to the service or not. For example, it may be desirable to have an app that
triggers replications every 24 hours in the background when the app is not actively being used, but when the app is being
used and a particular `Activity` is displayed it may be desirable to replicate every few minutes and then update the
`Activity` to show the data when replication has completed.

###ReplicationService
`ReplicationService` is an abstract class and your subclass must implement the following abstract method:
* `protected abstract Replicator[] getReplicators(Context context);` This method should return an array of `Replicator` objects that define the replications you want to invoke.

###PeriodicReplicationService
This abstract class is a child of `ReplicationService` and requires you to implement the following abstract methods:
* `protected abstract Replicator[] getReplicators(Context context);` This method should return an array of `Replicator` objects that define the replications you want to invoke.
* `protected abstract int getBoundIntervalInSeconds();` This method should return the interval (in seconds) you wish to have between replications when components are bound to the Service.
* `protected abstract int getUnboundIntervalInSeconds();` This method should return the interval (in seconds) you wish to have between replications when components are not bound to the Service.
* `protected abstract boolean startReplicationOnBind();` This should return `true` if you wish to have replications triggered immediately when a component binds to the Service.

Note that internally this uses `AlarmManager.setInexactRepeating()` to schedule the repetition of the replications at the given intervals in a battery efficient way, this means the intervals between replications will not be exact.

###PeriodicReplicationReceiver
This uses a `WakefulBroadcastReceiver` to trigger periodic replications at the intervals specified by your
`PeriodicReplicationService`. This means your application does not have to keep running to trigger replications at the
intervals you require, but will be restarted at the time a replication is required. This class also handles resetting of
the periodic replications after a reboot of the device. You must add the following `intent-filter`s to your
`PeriodicReplicationReceiver`'s entry in the `AndroidManifest.xml` to enable periodic replication:

```xml
<action android:name="com.cloudant.sync.replication.PeriodicReplicationReceiver.Alarm" />
<action android:name="android.intent.action.BOOT_COMPLETED" />
```

##Controlling the replication service
To control the replication service you start the service passing an Extra in the `Intent` used to start the service whose
key is `ReplicationService.EXTRA_COMMAND`, and whose value is one of:

* `ReplicationService.COMMAND_START_REPLICATION` This is used to start the replicators.
* `ReplicationService.COMMAND_STOP_REPLICATION` This is used to stop replicators in progress.
* `PeriodicReplicationService.COMMAND_START_PERIODIC_REPLICATION` This only applies if you're using the `PeriodicReplicationService`. This starts the periodic replications.
* `PeriodicReplicationService.COMMAND_STOP_PERIODIC_REPLICATION` This only applies if you're using the `PeriodicReplicationService`. This stops the periodic replications.
* `PeriodicReplicationService.COMMAND_DEVICE_REBOOTED` This only applies if you're using the `PeriodicReplicationService`. This resets the periodic replications after the device has rebooted. This will be automatically called if your subclass of `PeriodicReplicationReceiver` calls through to the `onReceive()` method of `PeriodicReplicationReceiver`.

For example, from a subclass of `PeriodicReplicationReceiver`, you might call:

```java
Intent intent = new Intent(context.getApplicationContext(), MyReplicationService.class);
intent.putExtra(ReplicationService.EXTRA_COMMAND, PeriodicReplicationService.COMMAND_START_PERIODIC_REPLICATION);
startWakefulService(context, intent);
```
Note the use of `startWakefulService()` which gets a `WakeLock` to ensure the device does not go to sleep while your replication is in progress.

If you want to start replications from anywhere other than a `PeriodicReplicationReceiver` you would need to replace
`startWakefulService(context, intent);` with `startService(intent);` in the above example and do any `WakeLock`
management you require yourself.

###WifiPeriodicReplicationReceiver
This class is an example of how to extend `PeriodicReplicationReceiver` to add logic that triggers periodic replications
only when our device is connected to Wifi and stops periodic replications when we disconnect from Wifi.
This is done by extending the `PeriodicReplicationReceiver` so that the BroadcastReceiver responds to changes in
network connectivity.  When we detect that the device has connected to Wifi we start our PeriodicReplicationService
by sending it the command `PeriodicReplicationService.COMMAND_START_PERIODIC_REPLICATION` and when the device disconnects
from Wifi we stop the periodic replications by sending `PeriodicReplicationService.COMMAND_STOP_PERIODIC_REPLICATION` to the
Service. Other Intent actions received by the broadcast receiver are passed to parent class so that periodic alarms and
resetting of the periodic replications after reboot are handled correctly.


##Example

Lets assume we wish to configure a replication policy as follows:

* We only ever want replications to occur when the device is connected to a WiFi network.
* We want to do sync replications (pull and push).
* When the app is not displaying data to the user we want replications to occur once every 24 hours to keep the data on the device fairly fresh.
* When the app is displaying data to the user we want replications to occur every 5 minutes so the data displayed to the user is only ever a few minutes out of date if we're on Wifi.
* When the app is displaying data to the user we want to refresh the UI to display the new data when the pull replication has completed.
* After the device has rebooted, we want replications to continue in the same way as prior to the reboot.

To demonstrate how we configure this policy, we'll use the `WifiPeriodicReplicationReceiver` to trigger our replications
when the device is connected to Wifi.

###BroadcastReceiver
Our subclass of `WifiPeriodicReplicationReceiver` is very simple and only needs to configure the name of the subclass of `PeriodicReplicationService` that we want our `BroadcastReceiver` to interact with by creating a default constructor that
passes it to the superclass's constructor. We'll be calling our service `MyReplicationService`, so our `BroadcastReceiver` looks like:

```java 
public class MyWifiPeriodicReplicationReceiver extends WifiPeriodicReplicationReceiver<MyReplicationService> {

    public MyWifiPeriodicReplicationReceiver() {
        super(MyReplicationService.class);
    }

}
```

###Service
Our service must configure the name of our `BroadcastReceiver` (i.e. `MyWifiPeriodicReplicationReceiver`) and does this
in the default constructor by passing the class's name to the superclass constructor. This allows the super class to invoke
`MyWifiPeriodicReplicationReceiver` when the periodic replications are due. We must also implement the abstract methods:

```java
public class MyReplicationService extends PeriodicReplicationService {

    public static int PUSH_REPLICATION_ID = 0;
    public static int PULL_REPLICATION_ID = 1;

    private static final String TAG = "MyReplicationService";

    private static final String TASKS_DATASTORE_NAME = "tasks";
    private static final String DATASTORE_MANGER_DIR = "data";

    public MyReplicationService() {
        super(MyWifiPeriodicReplicationReceiver.class);
    }

    @Override
    protected Replicator[] getReplicators(Context context) {
        try {
            URI uri = new URI("https", "my_api_key:my_api_secret", "myaccount.cloudant.com", 443, "/" + "mydb", null, null);;

            File path = context.getApplicationContext().getDir(
                DATASTORE_MANGER_DIR,
                Context.MODE_PRIVATE
            );

            DatastoreManager manager = new DatastoreManager(path.getAbsolutePath());
            Datastore datastore = null;
            try {
                datastore = manager.openDatastore(TASKS_DATASTORE_NAME);
            } catch (DatastoreNotCreatedException dnce) {
                Log.e(TAG, "Unable to open Datastore", dnce);
            }

            Replicator pullReplicator = ReplicatorBuilder.pull().from(uri).to(datastore).withId(PULL_REPLICATION_ID).build();
            Replicator pushReplicator = ReplicatorBuilder.push().to(uri).from(datastore).withId(PUSH_REPLICATION_ID).build();

            return new Replicator[]{pullReplicator, pushReplicator};
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
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
        return WifiPeriodicReplicationReceiver.isConnectedToWifi(this);;
    }
}
```

Note that we set IDs on the replications so that these IDs can be used to identify the replication that has completed or errored.

###AndroidManifest.xml
Now, we need to make sure our `AndroidManifest.xml` is updated to contain the `BroadcastReceiver` and `Service` we've created. We need to also ensure the `BroadcastReceiver` has the correct `intent-filter` settings.

Our `BroadcastReceiver` needs to know when the connectivity of the device has changed, when a periodic replication is due
and when the device has rebooted. We also don't want other apps to invoke our BroadcastReceiver so we set `android:exported="false"`, so we add the following to our manifest file:

```xml
<receiver android:name=".MyBackgroundWifiIntervalReplicationPolicyManager" android:exported="false">
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

###Binding to the Service
Lets assume we have an `Activity` that displays our data to the user. While this `Activity` is displayed we want our more frequent updates (every 5 minutes), and we want to know when replication has completed so we can refresh the Activity's UI with the new data. Note that the `Activity` must be running in the same process as the `Service`, which is the default on Android.

First we add some fields to our `Activity`:
```java
// Add a handler to allow us to post UI updates on the main thread.
private final Handler mHandler = new Handler(Looper.getMainLooper());

// Reference to our service.
private ReplicationService mReplicationService;

// Flag indicating whether the Activity is currently bound to the Service.
private boolean mIsBound;
```

Now we add a `ServiceConnection` to our `Activity` to allow us to handle binding and unbinding from our `MyReplicationService`. This enables us to get a reference to the service when we bind to it and add a listener for `replicationComplete` to the `Service`:

```java
private ServiceConnection mConnection = new ServiceConnection() {
    @Override
    public void onServiceConnected(ComponentName name, IBinder service) {
        mReplicationService = ((ReplicationService.LocalBinder) service).getService();
        mReplicationService.addListener(new ReplicationService.SimpleReplicationCompleteListener() {
            @Override
            public void replicationComplete(int id) {
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
We now bind to the service in `Activity.onStart()` and unbind in `Activity.onStop()`:

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

# Java replication policies

On Java, replication policies are much simpler than on Android.  To implement a replication policy it is necessary to create
a subclass of `ReplicationPolicyManager` and call `startReplications()` when your chosen conditions for replications to take
place are met and call `stopReplications()` when you want replications to be stopped.  The `ReplicationPolicyManager` will
ensure that if replications are currently in progress they are not restarted.  See the
`IntervalTimerReplicationPolicyManager` class, which is an example of a policy where replications are triggered at regular
intervals.

