/*
 * Copyright © 2016 Cloudant, Inc. All rights reserved.
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

package com.cloudant.todo.replicationpolicy;

import android.content.Context;
import android.preference.PreferenceManager;
import android.util.Log;

import com.cloudant.sync.documentstore.Database;
import com.cloudant.sync.documentstore.DocumentStore;
import com.cloudant.sync.documentstore.DocumentStoreNotOpenedException;
import com.cloudant.sync.replication.PeriodicReplicationService;
import com.cloudant.sync.replication.Replicator;
import com.cloudant.sync.replication.ReplicatorBuilder;
import com.cloudant.sync.replication.WifiPeriodicReplicationReceiver;
import com.cloudant.todo.ui.activities.ReplicationSettingsActivity;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

public class MyReplicationService extends PeriodicReplicationService<MyWifiPeriodicReplicationReceiver> {
    private static final String TAG = "MyReplicationService";
    private static final String TASKS_DATASTORE_NAME = "tasks";
    private static final String DATASTORE_MANGER_DIR = "data";
    public static int PUSH_REPLICATION_ID = 0;
    public static int PULL_REPLICATION_ID = 1;

    public MyReplicationService() {
        super(MyWifiPeriodicReplicationReceiver.class);
    }

    @Override
    public void onCreate() {
        super.onCreate();

        try {
            URI uri = ReplicationSettingsActivity.constructServerURI(this);

            File path = getApplicationContext().getDir(
                DATASTORE_MANGER_DIR,
                Context.MODE_PRIVATE
            );

            Database database = null;
            try {
                database = DocumentStore.getInstance(new File(path, TASKS_DATASTORE_NAME)).database;
            } catch (DocumentStoreNotOpenedException dsnoe) {
                Log.e(TAG, "Unable to open Datastore", dsnoe);
            }

            Replicator pullReplicator = ReplicatorBuilder.pull().from(uri).to(database).withId
                (PULL_REPLICATION_ID).build();
            Replicator pushReplicator = ReplicatorBuilder.push().to(uri).from(database).withId
                (PUSH_REPLICATION_ID).build();

            // Replications will not begin until setReplicators(Replicator[]) is called.
            setReplicators(new Replicator[]{pullReplicator, pushReplicator});
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected int getBoundIntervalInSeconds() {
        return Integer.valueOf(PreferenceManager.getDefaultSharedPreferences(this).getString
            (ReplicationSettingsActivity.SETTINGS_BOUND_REPLICATION_MINUTES, "0")) * 60;
    }

    @Override
    protected int getUnboundIntervalInSeconds() {
        return Integer.valueOf(PreferenceManager.getDefaultSharedPreferences(this).getString
            (ReplicationSettingsActivity.SETTINGS_UNBOUND_REPLICATION_MINUTES, "0")) * 60;
    }

    @Override
    protected boolean startReplicationOnBind() {
        // Trigger replications when a client binds to the service only if we're on WiFi.
        return WifiPeriodicReplicationReceiver.isConnectedToWifi(this);
    }
}
