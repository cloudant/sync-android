/*
 * Copyright © 2015 Cloudant, Inc. All rights reserved.
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

package com.cloudant.todo;

import com.cloudant.sync.documentstore.ConflictException;

import java.net.URISyntaxException;
import java.util.List;

import android.app.AlertDialog;
import android.app.Dialog;
import android.app.ListActivity;
import android.content.DialogInterface;
import android.content.Intent;
import android.content.SharedPreferences;
import android.content.SharedPreferences.OnSharedPreferenceChangeListener;
import android.os.Bundle;
import android.preference.PreferenceManager;
import android.text.Editable;
import android.text.TextWatcher;
import android.util.Log;
import android.view.ActionMode;
import android.view.KeyEvent;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ListView;
import android.widget.Toast;

public class TodoActivity
        extends ListActivity
        implements OnSharedPreferenceChangeListener {

    static final String LOG_TAG = "TodoActivity";

    private static final int DIALOG_NEW_TASK = 1;
    private static final int DIALOG_PROGRESS = 2;

    static final String SETTINGS_CLOUDANT_USER = "pref_key_username";
    static final String SETTINGS_CLOUDANT_DB = "pref_key_dbname";
    static final String SETTINGS_CLOUDANT_API_KEY = "pref_key_api_key";
    static final String SETTINGS_CLOUDANT_API_SECRET = "pref_key_api_password";

    // Main data model object
    private static TasksModel sTasks;
    private TaskAdapter mTaskAdapter;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        Log.d(LOG_TAG, "onCreate()");
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_todo);

        // Load default settings when we're first created.
        PreferenceManager.setDefaultValues(this, R.xml.preferences, false);

        // Register to listen to the setting changes because replicators
        // uses information managed by shared preference.
        SharedPreferences sharedPref = PreferenceManager.getDefaultSharedPreferences(this);
        sharedPref.registerOnSharedPreferenceChangeListener(this);

        // Protect creation of static variable.
        if (sTasks == null) {
            // Model needs to stay in existence for lifetime of app.
            this.sTasks = new TasksModel(this.getApplicationContext());
        }

        // Register this activity as the listener to replication updates
        // while its active.
        this.sTasks.setReplicationListener(this);

        // Load the tasks from the model
        this.reloadTasksFromModel();
    }

    @Override
    protected void onDestroy() {
        Log.d(LOG_TAG, "onDestroy()");
        super.onDestroy();

        // Clear our reference as listener.
        this.sTasks.setReplicationListener(null);
    }

    //
    // HELPER METHODS
    //

    private void reloadReplicationSettings() {
        try {
            this.sTasks.reloadReplicationSettings();
        } catch (URISyntaxException e) {
            Log.e(LOG_TAG, "Unable to construct remote URI from configuration", e);
            Toast.makeText(getApplicationContext(),
                    R.string.replication_error,
                    Toast.LENGTH_LONG).show();
        }
    }

    private void reloadTasksFromModel() {
        List<Task> tasks = this.sTasks.allTasks();
        this.mTaskAdapter = new TaskAdapter(this, tasks);
        this.setListAdapter(this.mTaskAdapter);
    }

    private void createNewTask(String desc) {
        Task t = new Task(desc);
        sTasks.createDocument(t);
        reloadTasksFromModel();
    }

    private void toggleTaskCompleteAt(int position) {
        try {
            Task t = (Task) mTaskAdapter.getItem(position);
            t.setCompleted(!t.isCompleted());
            t = sTasks.updateDocument(t);
            mTaskAdapter.set(position, t);
        } catch (ConflictException e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteTaskAt(int position) {
        try {
            Task t = (Task) mTaskAdapter.getItem(position);
            sTasks.deleteDocument(t);
            mTaskAdapter.remove(position);
            Toast.makeText(TodoActivity.this,
                    "Deleted item : " + t.getDescription(),
                    Toast.LENGTH_SHORT).show();
        } catch (ConflictException e) {
            throw new RuntimeException(e);
        }
    }

    public void onCompleteCheckboxClicked(View view) {
        this.toggleTaskCompleteAt(view.getId());
    }

    void stopReplication() {
        sTasks.stopAllReplications();
        this.dismissDialog(DIALOG_PROGRESS);
        mTaskAdapter.notifyDataSetChanged();
    }

    /**
     * Called by TasksModel when it receives a replication complete callback.
     * TasksModel takes care of calling this on the main thread.
     */
    void replicationComplete() {
        reloadTasksFromModel();
        Toast.makeText(getApplicationContext(),
                R.string.replication_completed,
                Toast.LENGTH_LONG).show();
        dismissDialog(DIALOG_PROGRESS);
    }

    /**
     * Called by TasksModel when it receives a replication error callback.
     * TasksModel takes care of calling this on the main thread.
     */
    void replicationError() {
        Log.i(LOG_TAG, "error()");
        reloadTasksFromModel();
        Toast.makeText(getApplicationContext(),
                R.string.replication_error,
                Toast.LENGTH_LONG).show();
        dismissDialog(DIALOG_PROGRESS);
    }

    //
    // EVENT HANDLING
    //

    @Override
    protected void onListItemClick(ListView l, View v, int position, long id) {
        if(mActionMode != null) {
            mActionMode.finish();
        }

        // Make the newly clicked item the currently selected one.
        this.getListView().setItemChecked(position, true);
        mActionMode = this.startActionMode(mActionModeCallback);
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.todo, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle item selection
        switch (item.getItemId()) {
            case R.id.action_new:
                this.showDialog(DIALOG_NEW_TASK);
                return true;
            case R.id.action_download:
                this.showDialog(DIALOG_PROGRESS);
                sTasks.startPullReplication();
                return true;
            case R.id.action_upload:
                this.showDialog(DIALOG_PROGRESS);
                sTasks.startPushReplication();
                return true;
            case R.id.action_settings:
                this.startActivity(
                        new Intent().setClass(this, SettingsActivity.class)
                );
                return true;
            default:
                return super.onOptionsItemSelected(item);
        }
    }

    @Override
    protected Dialog onCreateDialog(int id, Bundle args) {
        switch (id) {
            case DIALOG_NEW_TASK:
                return createNewTaskDialog();
            case DIALOG_PROGRESS:
                return createProgressDialog();
            default:
                throw new RuntimeException("No dialog defined for id: " + id);
        }
    }

    public Dialog createProgressDialog() {
        AlertDialog.Builder builder = new AlertDialog.Builder(this);
        final View v = this.getLayoutInflater().inflate(R.layout.dialog_loading, null);

        DialogInterface.OnClickListener negativeClick = new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialog, int which) {
                stopReplication();
            }
        };

        DialogInterface.OnKeyListener keyListener = new DialogInterface.OnKeyListener() {
            @Override
            public boolean onKey(DialogInterface dialog, int keyCode, KeyEvent event) {
                if (keyCode == KeyEvent.KEYCODE_BACK) {
                    Toast.makeText(getApplicationContext(),
                            R.string.replication_running, Toast.LENGTH_LONG).show();
                    return true;
                }
                return false;
            }
        };

        builder.setView(v).setNegativeButton("Stop", negativeClick).setOnKeyListener(keyListener);

        return builder.create();
    }

    public Dialog createNewTaskDialog() {
        AlertDialog.Builder builder = new AlertDialog.Builder(this);
        View v = this.getLayoutInflater().inflate(R.layout.dialog_new_task, null);
        final EditText description = (EditText) v.findViewById(R.id.new_task_desc);

        // Check description is present, if so add a task otherwise show an error
        DialogInterface.OnClickListener positiveClick = new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialog, int id) {
                if (description.getText().length() > 0) {
                    createNewTask(description.getText().toString());
                    description.getText().clear();
                } else {
                    Toast.makeText(getApplicationContext(),
                            R.string.task_not_created,
                            Toast.LENGTH_LONG).show();
                }
            }
        };

        DialogInterface.OnClickListener negativeClick = new DialogInterface.OnClickListener() {
            public void onClick(DialogInterface dialog, int id) {
                dialog.dismiss();
            }
        };

        builder.setView(v).setTitle(R.string.new_task)
                .setPositiveButton(R.string.create, positiveClick)
                .setNegativeButton(R.string.cancel, negativeClick);

        final AlertDialog d = builder.create();

        // Enable "Create" button when the description has some characters
        final TextWatcher textWatcher = new TextWatcher() {
            @Override
            public void onTextChanged(CharSequence s, int start,
                                      int before, int count) {
                final Button b = d.getButton(DialogInterface.BUTTON_POSITIVE);
                b.setEnabled(description.getText().length() > 0);
            }

            @Override
            public void beforeTextChanged(CharSequence s, int start,
                                          int count, int after) {
            }

            @Override
            public void afterTextChanged(Editable s) {
            }
        };

        d.setOnShowListener(new DialogInterface.OnShowListener() {
            @Override
            public void onShow(DialogInterface dialog) {
                final Button b = d.getButton(DialogInterface.BUTTON_POSITIVE);
                b.setEnabled(description.getText().length() > 0);
                description.addTextChangedListener(textWatcher);
            }
        });

        return d;
    }

    @Override
    public void onSharedPreferenceChanged(SharedPreferences sharedPreferences,
                                          String key) {
        Log.d(LOG_TAG, "onSharedPreferenceChanged()");
        reloadReplicationSettings();
    }

    ActionMode mActionMode = null;
    private ActionMode.Callback mActionModeCallback = new ActionMode.Callback() {

        @Override
        public boolean onCreateActionMode(ActionMode mode, Menu menu) {
            mode.getMenuInflater().inflate(R.menu.context_menu, menu);
            return true;
        }

        @Override
        public boolean onPrepareActionMode(ActionMode mode, Menu menu) {
            return false;
        }

        @Override
        public boolean onActionItemClicked(ActionMode mode, MenuItem item) {
            switch (item.getItemId()) {
                case R.id.action_delete:
                    deleteTaskAt(getListView().getCheckedItemPosition());
                    mode.finish();
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public void onDestroyActionMode(ActionMode mode) {
            getListView().setItemChecked(getListView().getCheckedItemPosition(), false);
            mActionMode = null;
        }
    };
}
