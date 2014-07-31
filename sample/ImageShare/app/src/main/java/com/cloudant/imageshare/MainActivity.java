package com.cloudant.imageshare;

import android.app.Activity;
import android.app.AlertDialog;
import android.content.DialogInterface;
import android.content.SharedPreferences;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Build;
import android.os.Bundle;
import android.preference.PreferenceManager;
import android.util.DisplayMetrics;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.EditText;
import android.widget.GridView;
import android.widget.Toast;
import android.content.Intent;

import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DocumentBody;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.datastore.UnsavedStreamAttachment;
import com.cloudant.sync.replication.Replication;
import com.cloudant.sync.replication.ReplicatorFactory;
import com.cloudant.sync.replication.Replicator;
import com.cloudant.sync.replication.PushReplication;
import com.cloudant.sync.replication.PullReplication;
import com.cloudant.sync.datastore.Attachment;

import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class MainActivity extends Activity{

    private Datastore ds;
    private DatastoreManager manager;
    public ImageAdapter adapter;
    private boolean isEmulator = false;
    private String api_key;
    private String api_pass;
    private String db_name;

    private enum ReplicationType {
        Pull,
        Push
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        // Get the API key if it's already stored in preferences
        getDataFromPrefs();

        // Check if running on emulator
        isEmulator = "generic".equals(Build.BRAND.toLowerCase());

        // Create a grid of images
        createGrid();

        // Create an empty datastore
        initDatastore();
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.main, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item){
        switch(item.getItemId()){
            case R.id.action_add:
                addImage();
                return true;
            case R.id.action_replicate:
                replicateDatastore(ReplicationType.Push);
                return true;
            case R.id.action_delete:
                deleteDatastore();
                reloadView();
                return true;
            case R.id.action_pull_replicate:
                replicateDatastore(ReplicationType.Pull);
                adapter.clearImageData();
                loadDatastore();
                reloadView();
                return true;
            case R.id.action_share:
                Intent sendIntent = new Intent();
                sendIntent.setAction(Intent.ACTION_SEND);
                sendIntent.putExtra(Intent.EXTRA_TEXT, db_name);
                sendIntent.setType("text/plain");
                startActivity(sendIntent);
                return true;
            case R.id.action_connect:
                buildDialog();
                return true;
            case R.id.action_new:
                try {
                    createRemoteDatabase();
                    Toast.makeText(MainActivity.this,
                            "Remote database was created",
                            Toast.LENGTH_SHORT).show();
                } catch (Exception e){
                    e.printStackTrace();
                    Toast.makeText(MainActivity.this,
                            "Remote database was NOT created",
                            Toast.LENGTH_SHORT).show();
                }
                return true;
        }
        return super.onOptionsItemSelected(item);
    }

    // Load the image into the grid view add it to a document in local db
    public void addImage(){
        if (isEmulator){
            Uri path = Uri.parse("android.resource://com.cloudant.imageshare/" +
                    R.raw.pic1);
            try {
                createDoc(path);
                Toast.makeText(MainActivity.this,
                        "Document was written to local db",
                        Toast.LENGTH_SHORT).show();
            } catch (IOException e) {
                e.printStackTrace();
                Toast.makeText(MainActivity.this,
                        "Writing to local db failed",
                        Toast.LENGTH_SHORT).show();
            }
            reloadView();
        } else {
            // Take the user to their chosen image selection app (gallery or file manager)
            Intent pickIntent = new Intent();
            pickIntent.setType("image/*");
            pickIntent.setAction(Intent.ACTION_GET_CONTENT);
            startActivityForResult(Intent.createChooser(pickIntent, "Select Picture"), 1);
        }
    }

    // Opens the dialog for user to input the remote database name
    private void buildDialog(){
        AlertDialog.Builder alert = new AlertDialog.Builder(this);

        alert.setTitle("Database name");
        final EditText input = new EditText(this);
        alert.setView(input);

        alert.setPositiveButton("Ok", new DialogInterface.OnClickListener() {
            public void onClick(DialogInterface dialog, int whichButton) {
                String value = input.getText().toString();
                try {
                    connectToRemoteDatabase(value);
                    Toast.makeText(MainActivity.this,
                            "Connected to remote database",
                            Toast.LENGTH_SHORT).show();
                } catch (Exception e) {
                    e.printStackTrace();
                    Toast.makeText(MainActivity.this,
                            "Failed to connect to remote database",
                            Toast.LENGTH_SHORT).show();
                }
            }
        });

        alert.setNegativeButton("Cancel", new DialogInterface.OnClickListener() {
            public void onClick(DialogInterface dialog, int whichButton) {
                // Canceled.
            }
        });

        alert.show();
    }

    // Processing the output of image selection app
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        if (resultCode == RESULT_OK && requestCode == 1) {
            try {
                Uri imageUri = data.getData();
                createDoc(imageUri);
                reloadView();
                Toast.makeText(MainActivity.this,
                        "Document was written to local db",
                        Toast.LENGTH_SHORT).show();
            } catch (IOException e) {
                e.printStackTrace();
                Toast.makeText(MainActivity.this,
                        "Writing to local db failed",
                        Toast.LENGTH_SHORT).show();
            }
        }
        super.onActivityResult(requestCode, resultCode, data);
    }



    // Creates a DatastoreManager using application internal storage path
    public void initDatastore() {
        File path = getApplicationContext().getDir("datastores", 0);
        manager = new DatastoreManager(path.getAbsolutePath());
        ds = manager.openDatastore("my_datastore");
        loadDatastore();
    }

    // Delete and open a new local datastore
    public void deleteDatastore(){
        try {
            manager.deleteDatastore("my_datastore");
            ds = manager.openDatastore("my_datastore");
            Toast.makeText(MainActivity.this,
                    "Database deleted",
                    Toast.LENGTH_SHORT).show();
        } catch (IOException e) {
            e.printStackTrace();
            Toast.makeText(MainActivity.this,
                    "Database deletion failed",
                    Toast.LENGTH_SHORT).show();
        }
        adapter.clearImageData();
    }

    // Load images from datastore
    public void loadDatastore() {
        try {
            // Read all documents in one go
            int pageSize = ds.getDocumentCount();
            List<DocumentRevision> docs = ds.getAllDocuments(0, pageSize, true);
            for (DocumentRevision rev : docs) {
                Attachment a = ds.getAttachment(rev, "image.jpg");
                adapter.addImage(a);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void replicateDatastore(ReplicationType r){
        try {
            URI uri = new URI("https://" + getString(R.string.default_user)
                    + ".cloudant.com/" + db_name);
            //URI uri = new URI("http://10.0.2.2:5984/sync-test");

            Replication replication;
            if (r == ReplicationType.Pull) {
                PullReplication pull = new PullReplication();
                pull.target = ds;
                pull.source = uri;
                replication = pull;
            } else {
                PushReplication push = new PushReplication();
                push.source = ds;
                push.target = uri;
                replication = push;
            }

            replication.username = api_key;
            replication.password = api_pass;
            Replicator replicator = ReplicatorFactory.oneway(replication);

            // Use a CountDownLatch to provide a lightweight way to wait for completion
            CountDownLatch latch = new CountDownLatch(1);
            ReplicationListener listener = new ReplicationListener(latch);
            replicator.getEventBus().register(listener);
            replicator.start();

            latch.await();
            replicator.getEventBus().unregister(listener);

            if (replicator.getState() != Replicator.State.COMPLETE) {
                Toast.makeText(MainActivity.this,
                        "Replication error",
                        Toast.LENGTH_SHORT).show();
            } else {
                Toast.makeText(MainActivity.this,
                        "Replication finished",
                        Toast.LENGTH_SHORT).show();
            }
        } catch (Exception e) {
            e.printStackTrace();
            Toast.makeText(MainActivity.this,
                    "Replication failed",
                    Toast.LENGTH_SHORT).show();
        }
    }

    // Creates a document in local database with given attachment
    public void createDoc(Uri path) throws IOException{
        adapter.addImage(path, this);
        InputStream is = getContentResolver().openInputStream(path);
        DocumentBody doc = new BasicDoc("Marco","Polo");
        DocumentRevision revision = ds.createDocument(doc);
        uploadAttachment(revision.getId(), is);
    }

    // Adds attachment to a document
    public void uploadAttachment(String id, InputStream is) {
        try {
            Attachment att = new UnsavedStreamAttachment(is, "image.jpg", "image/jpeg");
            List<Attachment> atts = new ArrayList<Attachment>();
            atts.add(att);
            DocumentRevision oldRevision = ds.getDocument(id); //doc id
            DocumentRevision newRevision = null;
            // set attachment
            ds.updateAttachments(oldRevision, atts);
        } catch (Exception e) {
            e.printStackTrace();
            Toast.makeText(MainActivity.this,
                    "Attachment was not added",
                    Toast.LENGTH_SHORT).show();
        }
    }

    private void createRemoteDatabase() throws Exception {
        String api_server = getString(R.string.default_api_server);
        AsyncTask request = new AsyncRequestNewDB().execute(api_server);
        String response = (String) request.get();
        JSONObject json = new JSONObject(response);
        api_key = json.get("key").toString();
        api_pass = json.get("password").toString();
        db_name = json.get("db_name").toString();
        saveToPrefs(api_key, api_pass, db_name);
    }

    private void connectToRemoteDatabase(String db) throws Exception{
        String api_server = getString(R.string.default_api_server) + "/get_key";
        AsyncTask httpRequest = new AsyncRequestAPI().execute(api_server, db);
        String response = (String) httpRequest.get();
        JSONObject json = new JSONObject(response);
        api_key = json.get("key").toString();
        db_name = db;
        api_pass = json.get("password").toString();
        saveToPrefs(api_key, api_pass, db_name);
    }

    // Saves authentication data to local preferences
    private void saveToPrefs(String key, String pass, String db) {
        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(this);
        final SharedPreferences.Editor editor = prefs.edit();
        editor.putString("key", key);
        editor.putString("pass", pass);
        editor.putString("db", db);
        editor.commit();
    }

    // Checks local preferences for authentication data, if not found - loads default values
    private void getDataFromPrefs() {
        SharedPreferences sharedPrefs = PreferenceManager.getDefaultSharedPreferences(this);
        try {
            api_key = sharedPrefs.getString("key", getString(R.string.default_api_key));
            api_pass = sharedPrefs.getString("pass", getString(R.string.default_api_password));
            db_name = sharedPrefs.getString("db", getString(R.string.default_dbname));
        } catch (Exception e) {
            // Close the app if no authentication data can be loaded
            e.printStackTrace();
            System.exit(1);
        }
    }

    // Creates grid of images
    private void createGrid(){
        DisplayMetrics displayMetrics = getResources().getDisplayMetrics();
        GridView gridview = (GridView) findViewById(R.id.gridview);
        int screenW = displayMetrics.widthPixels - 40;
        gridview.setColumnWidth(screenW/2);
        adapter = new ImageAdapter(this, screenW/2);
        gridview.setAdapter(adapter);
    }

    private void reloadView() {
        GridView gridview = (GridView) findViewById(R.id.gridview);
        gridview.invalidate();
        gridview.requestLayout();
    }
}
