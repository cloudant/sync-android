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

import com.cloudant.sync.documentstore.DocumentStore;
import com.cloudant.sync.documentstore.DocumentStoreNotOpenedException;
import com.cloudant.sync.replication.PeriodicReplicationService;
import com.cloudant.sync.replication.Replicator;
import com.cloudant.sync.replication.ReplicatorBuilder;
import com.cloudant.sync.replication.WifiPeriodicReplicationReceiver;
import com.cloudant.todo.ui.activities.SettingsActivity;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

public class TodoReplicationService extends
    PeriodicReplicationService<TodoWifiPeriodicReplicationReceiver> {
    private static final String TAG = "TodoRS";
    private static final String DOCUMENT_STORE_DIR = "data";
    private static final String DOCUMENT_STORE_NAME = "tasks";
    public static int PUSH_REPLICATION_ID = 0;
    public static int PULL_REPLICATION_ID = 1;

    public TodoReplicationService() {
        super(TodoWifiPeriodicReplicationReceiver.class);
    }

    @Override
    public void onCreate() {
        super.onCreate();

        try {
            URI uri = SettingsActivity.constructTodoURI(this);

            File path = getApplicationContext().getDir(
                DOCUMENT_STORE_DIR,
                Context.MODE_PRIVATE
            );

            DocumentStore documentStore = null;
            try {
                documentStore = DocumentStore.getInstance(new File(path, DOCUMENT_STORE_NAME));
            } catch (DocumentStoreNotOpenedException e) {
                Log.e(TAG, "Unable to open DocumentStore", e);
            }

            Replicator pullReplicator = ReplicatorBuilder.pull().from(uri).to(documentStore).withId
                (PULL_REPLICATION_ID).build();
            Replicator pushReplicator = ReplicatorBuilder.push().to(uri).from(documentStore).withId
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
            (SettingsActivity.TODO_BOUND_REPLICATION_MINUTES, "0")) * 60;
    }

    @Override
    protected int getUnboundIntervalInSeconds() {
        return Integer.valueOf(PreferenceManager.getDefaultSharedPreferences(this).getString
            (SettingsActivity.TODO_UNBOUND_REPLICATION_MINUTES, "0")) * 60;
    }

    @Override
    protected boolean startReplicationOnBind() {
        // Trigger replications when a client binds to the service only if we're on WiFi.
        return WifiPeriodicReplicationReceiver.isConnectedToWifi(this);
    }
}
