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

package com.cloudant.todo.ui.activities;

import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Build;
import android.preference.PreferenceActivity;
import android.preference.PreferenceManager;
import android.widget.Toast;

import com.cloudant.sync.replication.PeriodicReplicationService;
import com.cloudant.sync.replication.ReplicationService;
import com.cloudant.todo.R;
import com.cloudant.todo.replicationpolicy.TodoReplicationService;
import com.cloudant.todo.replicationpolicy.TwitterReplicationService;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class SettingsActivity extends PreferenceActivity implements SharedPreferences
    .OnSharedPreferenceChangeListener {

    public static final String TODO_CLOUDANT_USER = "todo_cloudant_username";
    public static final String TODO_CLOUDANT_DB = "todo_cloudant_dbname";
    public static final String TODO_CLOUDANT_API_KEY = "todo_cloudant_api_key";
    public static final String TODO_CLOUDANT_API_SECRET = "todo_cloudant_api_password";

    public static final String TODO_UNBOUND_REPLICATION_MINUTES =
        "todo_unbound_replication_minutes";
    public static final String TODO_BOUND_REPLICATION_MINUTES =
        "todo_bound_replication_minutes";


    public static final String TWITTER_CLOUDANT_USER = "twitter_cloudant_username";
    public static final String TWITTER_CLOUDANT_DB = "twitter_cloudant_dbname";
    public static final String TWITTER_CLOUDANT_API_KEY = "twitter_cloudant_api_key";
    public static final String TWITTER_CLOUDANT_API_SECRET = "twitter_cloudant_api_password";

    public static final String TWITTER_UNBOUND_REPLICATION_MINUTES =
        "twitter_unbound_replication_minutes";
    public static final String TWITTER_BOUND_REPLICATION_MINUTES =
        "twitter_bound_replication_minutes";

    private static final String HOST_SUFFIX = ".cloudant.com";

    /**
     * <p>Returns the URI for the remote database, based on the app's
     * configuration.</p>
     *
     * @return the remote database's URI
     * @throws URISyntaxException if the settings give an invalid URI
     */
    public static URI constructTodoURI(Context context)
        throws URISyntaxException {
        // We store this in plain text for the purposes of simple demonstration,
        // you might want to use something more secure.
        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(context);
        String username = prefs.getString(TODO_CLOUDANT_USER, "");
        String dbName = prefs.getString(TODO_CLOUDANT_DB, "");
        String apiKey = prefs.getString(TODO_CLOUDANT_API_KEY, "");
        String apiSecret = prefs.getString(TODO_CLOUDANT_API_SECRET, "");
        String host = username + HOST_SUFFIX;

        // We recommend always using HTTPS to talk to Cloudant.
        return new URI("https", apiKey + ":" + apiSecret, host, 443, "/" + dbName, null, null);
    }

    public static URI constructTwitterURI(Context context)
        throws URISyntaxException {
        // We store this in plain text for the purposes of simple demonstration,
        // you might want to use something more secure.
        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(context);
        String username = prefs.getString(TWITTER_CLOUDANT_USER, "");
        String dbName = prefs.getString(TWITTER_CLOUDANT_DB, "");
        String apiKey = prefs.getString(TWITTER_CLOUDANT_API_KEY, "");
        String apiSecret = prefs.getString(TWITTER_CLOUDANT_API_SECRET, "");
        String host = username + HOST_SUFFIX;

        // We recommend always using HTTPS to talk to Cloudant.
        return new URI("https", apiKey + ":" + apiSecret, host, 443, "/" + dbName, null, null);
    }

    @Override
    protected void onResume() {
        super.onResume();
        PreferenceManager.getDefaultSharedPreferences(this)
            .registerOnSharedPreferenceChangeListener(this);
    }

    @Override
    protected void onPause() {
        super.onPause();
        PreferenceManager.getDefaultSharedPreferences(this)
            .unregisterOnSharedPreferenceChangeListener(this);
    }

    @Override
    public void onSharedPreferenceChanged(SharedPreferences sharedPreferences, String key) {
        if (TODO_BOUND_REPLICATION_MINUTES.equals(key)) {
            Toast.makeText(this, "Updating the TODO bound replication time", Toast.LENGTH_SHORT)
                .show();
            resetTodoReplicationTimers();
        } else if (TODO_UNBOUND_REPLICATION_MINUTES.equals(key)) {
            Toast.makeText(this, "Updating the TODO unbound replication time", Toast.LENGTH_SHORT)
                .show();
            resetTodoReplicationTimers();
        } else if (TWITTER_BOUND_REPLICATION_MINUTES.equals(key)) {
            Toast.makeText(this, "Updating the TWITTER bound replication time", Toast.LENGTH_SHORT)
                .show();
            resetTwitterReplicationTimers();
        } else if (TWITTER_UNBOUND_REPLICATION_MINUTES.equals(key)) {
            Toast.makeText(this, "Updating the TWITTER unbound replication time", Toast
                .LENGTH_SHORT)
                .show();
            resetTwitterReplicationTimers();
        }
    }

    private void resetTodoReplicationTimers() {
        if (Build.VERSION.SDK_INT >= 21) {
            TodoActivity.cancelTodoJobService(getApplicationContext());
            TodoActivity.startTodoJobService(getApplicationContext(), true);
        } else {
            Intent intent = new Intent(getApplicationContext(), TodoReplicationService.class);
            intent.putExtra(ReplicationService.EXTRA_COMMAND, PeriodicReplicationService
                .COMMAND_RESET_REPLICATION_TIMERS);
            startService(intent);
        }
    }

    private void resetTwitterReplicationTimers() {
        if (Build.VERSION.SDK_INT >= 21) {
            TodoActivity.cancelTwitterJobService(getApplicationContext());
            TodoActivity.startTwitterJobService(getApplicationContext(), false);
        } else {
            Intent intent = new Intent(getApplicationContext(), TwitterReplicationService.class);
            intent.putExtra(ReplicationService.EXTRA_COMMAND, PeriodicReplicationService
                .COMMAND_RESET_REPLICATION_TIMERS);
            startService(intent);
        }
    }

    @Override
    public void onBuildHeaders(List<Header> target) {
        loadHeadersFromResource(R.xml.preference_headers, target);
    }
}