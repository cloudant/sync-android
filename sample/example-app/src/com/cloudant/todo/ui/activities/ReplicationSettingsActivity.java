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

import android.app.Activity;
import android.content.Context;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.preference.PreferenceManager;
import android.widget.Toast;

import com.cloudant.todo.ui.fragments.SettingsFragment;

import java.net.URI;
import java.net.URISyntaxException;

public class ReplicationSettingsActivity extends Activity implements SharedPreferences
    .OnSharedPreferenceChangeListener {

    public static final String SETTINGS_CLOUDANT_USER = "pref_cloudant_username";
    public static final String SETTINGS_CLOUDANT_DB = "pref_cloudant_dbname";
    public static final String SETTINGS_CLOUDANT_API_KEY = "pref_cloudant_api_key";
    public static final String SETTINGS_CLOUDANT_API_SECRET = "pref_cloudant_api_password";

    public static final String SETTINGS_UNBOUND_REPLICATION_MINUTES =
        "pref_unbound_replication_minutes";
    public static final String SETTINGS_BOUND_REPLICATION_MINUTES =
        "pref_bound_replication_minutes";

    private static final String HOST_SUFFIX = ".cloudant.com";

    /**
     * <p>Returns the URI for the remote database, based on the app's
     * configuration.</p>
     *
     * @return the remote database's URI
     * @throws URISyntaxException if the settings give an invalid URI
     */
    public static URI constructServerURI(Context context)
        throws URISyntaxException {
        // We store this in plain text for the purposes of simple demonstration,
        // you might want to use something more secure.
        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(context);
        String username = prefs.getString(SETTINGS_CLOUDANT_USER, "");
        String dbName = prefs.getString(SETTINGS_CLOUDANT_DB, "");
        String apiKey = prefs.getString(SETTINGS_CLOUDANT_API_KEY, "");
        String apiSecret = prefs.getString(SETTINGS_CLOUDANT_API_SECRET, "");
        String host = username + HOST_SUFFIX;

        // We recommend always using HTTPS to talk to Cloudant.
        return new URI("https", apiKey + ":" + apiSecret, host, 443, "/" + dbName, null, null);
    }

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        getFragmentManager().beginTransaction().replace(android.R.id.content,
            new SettingsFragment()).commit();

        PreferenceManager.getDefaultSharedPreferences(this)
            .registerOnSharedPreferenceChangeListener(this);
    }

    @Override
    public void onSharedPreferenceChanged(SharedPreferences sharedPreferences, String key) {
        if (SETTINGS_BOUND_REPLICATION_MINUTES.equals(key)) {
            Toast.makeText(this, "Updating the bound replication time", Toast.LENGTH_SHORT).show();
        } else if (SETTINGS_UNBOUND_REPLICATION_MINUTES.equals(key)) {
            Toast.makeText(this, "Updating the unbound replication time", Toast.LENGTH_SHORT)
                .show();
        }
    }
}