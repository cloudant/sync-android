/*
 * Copyright © 2016 IBM Corp. All rights reserved.
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

package com.cloudant.sync.replication;

import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
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

    private static final String WAS_ON_WIFI_SUFFIX = ".wasOnWifi";

    protected WifiPeriodicReplicationReceiver(Class<T> clazz) {
        super(clazz);
    }

    @Override
    public void onReceive(Context context, Intent intent) {
        if (ConnectivityManager.CONNECTIVITY_ACTION.equals(intent.getAction())) {

            int command = ReplicationService.COMMAND_NONE;
            boolean isConnectedToWifi = isConnectedToWifi(context);

            if (isConnectedToWifi == wasOnWifi(context)) {
                // This receiver will get a CONNECTIVITY_ACTION when we disconnect from networks
                // as well as when we connect to networks. We only want to do anything if we were
                // we were on WiFi and now are not, or were not on WiFi and now are.
                return;
            }else if (isConnectedToWifi) {
                // State has changed to connected.
                setWasOnWifi(context, true);
                if (PeriodicReplicationService.replicationsPending(context, clazz)) {
                    // There was a replication in progress when we lost WiFi, so restart
                    // replication immediately.
                    command = ReplicationService.COMMAND_START_REPLICATION;
                }
            } else if (!isConnectedToWifi(context)) {
                // State has changed to disconnected.
                setWasOnWifi(context, false);

                command = ReplicationService.COMMAND_STOP_REPLICATION;
            }

            if (command != ReplicationService.COMMAND_NONE) {
                Intent serviceIntent = new Intent(context.getApplicationContext(), clazz);
                serviceIntent.putExtra(PeriodicReplicationService.EXTRA_COMMAND, command);
                startWakefulService(context, serviceIntent);
            }
        } else if (!ALARM_ACTION.equals(intent.getAction()) || isConnectedToWifi (context)) {
            // Pass on the processing to the superclass if this is not an alarm, or if it's an
            // alarm and we're connected to WiFi.
           super.onReceive(context, intent);
        } else {
            PeriodicReplicationService.setReplicationsPending(context, clazz, true);
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

    public void setWasOnWifi(Context context, boolean onWifi) {
        SharedPreferences prefs = context.getSharedPreferences(PeriodicReplicationService
            .PREFERENCES_FILE_NAME, Context
            .MODE_PRIVATE);
        SharedPreferences.Editor editor = prefs.edit();
        editor.putBoolean(PeriodicReplicationService.constructKey(clazz,
            WAS_ON_WIFI_SUFFIX), onWifi);
        editor.apply();
    }

    private boolean wasOnWifi(Context context) {
        SharedPreferences prefs = context.getSharedPreferences(PeriodicReplicationService
            .PREFERENCES_FILE_NAME, Context
            .MODE_PRIVATE);
        return prefs.getBoolean(PeriodicReplicationService.constructKey(clazz,
            WAS_ON_WIFI_SUFFIX), false);
    }

}
