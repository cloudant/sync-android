package com.cloudant.sync.replication;

import android.content.Context;
import android.content.Intent;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;

/**
 * <p>This class extends {@link PeriodicReplicationReceiver} so that periodic replications are only
 * started when the device is connected to a WiFi network and are stopped when the device
 * disconnects from a WiFi network.</p>
 *
 * <p>This uses the standard Android broadcasts to detect connectivity change and then trigger the
 * periodic replications.</p>
 *
 * <p>To use this, you should create a subclass of this class whose default constructor calls
 * the constructor of this class passing in the name of the concrete
 * {@link PeriodicReplicationService}
 * you are using - e.g.:</p>
 * <pre>
 * public class MyWifiPeriodicReplicationReceiver
 *     extends WifiPeriodicReplicationReceiver&lt;MyReplicationService&gt; {
 *
 *     public MyWifiPeriodicReplicationReceiver() {
 *         super(MyReplicationService.class);
 *     }
 *
 * }
 * </pre>
 *
 * <p>You should then add your subclass to the {@code AndroidManifest.xml} as a child of the {@code
 * application} tag and add {@link android.content.IntentFilter}s as follows:</p>
 * <pre>
 * &lt;receiver android:name=".MyWifiPeriodicReplicationReceiver" android:exported="false"&gt;
 *     &lt;intent-filter&gt;
 *         &lt;action android:name="android.net.conn.CONNECTIVITY_CHANGE" /&gt;
 *         &lt;action android:name="com.cloudant.sync.replication.PeriodicReplicationReceiver.Alarm" /&gt;
 *         &lt;action android:name="android.intent.action.BOOT_COMPLETED" /&gt;
 *     &lt;/intent-filter&gt;
 * &lt;/receiver&gt;
 * </pre>
 *
 * <p>You must then add the following permissions to your {@code AndroidManifest.xml} file:</p>
 * <pre>
 * &lt;uses-permission android:name="android.permission.INTERNET" /&gt;
 * &lt;uses-permission android:name="android.permission.ACCESS_NETWORK_STATE" /&gt;
 * &lt;uses-permission android:name="android.permission.WAKE_LOCK" /&gt;
 * &lt;uses-permission android:name="android.permission.RECEIVE_BOOT_COMPLETED" /&gt;
 * </pre>
 *
 * @param <T> The {@link PeriodicReplicationService} component triggered by this
 * {@link android.content.BroadcastReceiver}
 */

public abstract class WifiPeriodicReplicationReceiver<T extends PeriodicReplicationService>
    extends PeriodicReplicationReceiver {

    protected WifiPeriodicReplicationReceiver(Class<T> clazz) {
        super(clazz);
    }

    @Override
    public void onReceive(Context context, Intent intent) {
        if (ConnectivityManager.CONNECTIVITY_ACTION.equals(intent.getAction())) {

            int command;
            if (isConnectedToWifi(context)) {
                // State has changed to connected.
                command = PeriodicReplicationService.COMMAND_START_PERIODIC_REPLICATION;
            } else {
                // State has changed to disconnected.
                command = PeriodicReplicationService.COMMAND_STOP_PERIODIC_REPLICATION;
            }
            Intent serviceIntent = new Intent(context.getApplicationContext(), clazz);
            serviceIntent.putExtra(PeriodicReplicationService.EXTRA_COMMAND, command);
            startWakefulService(context, serviceIntent);
        } else {
            // Pass on the processing to the superclass to handle alarms and reboot.
            super.onReceive(context, intent);
        }
    }

    public static boolean isConnectedToWifi(Context context) {
        ConnectivityManager cm =
            (ConnectivityManager) context.getSystemService(Context.CONNECTIVITY_SERVICE);
        NetworkInfo activeNetwork = cm.getActiveNetworkInfo();

        return activeNetwork != null
            && activeNetwork.getType() == ConnectivityManager.TYPE_WIFI
            && activeNetwork.isConnectedOrConnecting();
    }

}
