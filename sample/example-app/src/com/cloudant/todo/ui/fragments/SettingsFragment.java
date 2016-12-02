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

package com.cloudant.todo.ui.fragments;

import android.os.Bundle;
import android.preference.PreferenceFragment;

import com.cloudant.todo.R;

public class SettingsFragment extends PreferenceFragment {

    private static final String KEY_SETTINGS = "settings";
    private static final String VALUE_TODO_SETTINGS = "todo_settings";
    private static final String VALUE_TWITTER_SETTINGS = "twitter_settings";

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        String settings = getArguments().getString(KEY_SETTINGS);
        if (VALUE_TODO_SETTINGS.equals(settings)) {
            addPreferencesFromResource(R.xml.todo_preferences);
        } else if (VALUE_TWITTER_SETTINGS.equals(settings)) {
            addPreferencesFromResource(R.xml.twitter_preferences);
        }
    }
}
