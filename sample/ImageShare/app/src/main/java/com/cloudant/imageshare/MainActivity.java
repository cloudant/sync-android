package com.cloudant.imageshare;

import android.app.Activity;
import android.os.Bundle;
import android.util.DisplayMetrics;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.AdapterView;
import android.widget.GridView;
import android.widget.Toast;

import com.cloudant.sync.datastore.ConflictException;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DocumentBody;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.replication.ReplicatorFactory;
import com.cloudant.sync.replication.Replicator;
import com.cloudant.sync.replication.PushReplication;
import com.cloudant.sync.replication.PullReplication;
import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.UnsavedFileAttachment;

import java.io.IOException;
import java.net.URI;

import java.io.File;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class MainActivity extends Activity {

    private Datastore ds;
    private DatastoreManager manager;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        //Create a grid of images
        GridView gridview = (GridView) findViewById(R.id.gridview);
        int screenW = getScreenW();
        gridview.setColumnWidth(screenW/2);
        ImageAdapter adapter = new ImageAdapter(this, screenW/2);
        gridview.setAdapter(adapter);

        initDatastore();

        /*
        Picture in the right column adds a document to a local datastore
        while picture on the left adds it and replicates the whole database to remote.
        */
        gridview.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            public void onItemClick(AdapterView<?> parent, View v, int position, long id) {
                // Create a document
                DocumentBody doc = new BasicDoc("Position: " + position, "ID: " + id);
                DocumentRevision revision = ds.createDocument(doc);

                //uploadAttachment(revision.getId(),"sample_" + position + ".jpg");

                Toast.makeText(MainActivity.this,
                                "Document " + revision.getId() + " written to local db",
                                Toast.LENGTH_SHORT).show();
                }
        });
    }

    private int getScreenW(){
        DisplayMetrics displayMetrics = getResources().getDisplayMetrics();

        return (int) (displayMetrics.widthPixels / displayMetrics.density);
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.main, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        switch(item.getItemId()){
            case R.id.action_settings:
                return true;
            case R.id.action_replicate:
                replicateDatastore();
                return true;
            case R.id.action_pull_replicate:
                pullReplicateDatastore();
                return true;
        }
        return super.onOptionsItemSelected(item);
    }

    public void initDatastore(){
        // Create a DatastoreManager using application internal storage path
        File path = getApplicationContext().getDir("datastores", 0);
        manager = new DatastoreManager(path.getAbsolutePath());

        ds = manager.openDatastore("my_datastore");
    }

    public void replicateDatastore(){
        try {
            URI uri = new URI("https://" + getString(R.string.default_user)
                    + ".cloudant.com/" + getString(R.string.default_dbname));

            // Create a replicator that replicates changes from the local
            // datastore to the remote database.
            // username/password can be Cloudant API keys
            PushReplication push = new PushReplication();
            push.username = getString(R.string.default_api_key);
            push.password = getString(R.string.default_api_password);
            push.source = ds;
            push.target = uri;
            Replicator replicator = ReplicatorFactory.oneway(push);

            // Use a CountDownLatch to provide a lightweight way to wait for completion
            CountDownLatch latch = new CountDownLatch(1);
            ReplicationListener listener = new ReplicationListener(latch);
            replicator.getEventBus().register(listener);
            replicator.start();
            latch.await();
            replicator.getEventBus().unregister(listener);

            if (replicator.getState() != Replicator.State.COMPLETE) {
                System.out.println("Error replicating TO remote");
                System.out.println(listener.error);
            }

            Toast.makeText(MainActivity.this, "Local db is replicated.",
                    Toast.LENGTH_SHORT).show();
        } catch (Exception e) {
            Log.d("Exception found! ", e.toString());
        }
    }

    public void pullReplicateDatastore() {
        try {
            URI uri = new URI("https://" + getString(R.string.default_user)
                               + ".cloudant.com/" + getString(R.string.default_dbname));

            // username/password can be Cloudant API keys
            PullReplication pull = new PullReplication();
            pull.username = getString(R.string.default_api_key);
            pull.password = getString(R.string.default_api_password);
            pull.target = ds;
            pull.source = uri;
            Replicator replicator = ReplicatorFactory.oneway(pull);

            // Use a CountDownLatch to provide a lightweight way to wait for completion
            CountDownLatch latch = new CountDownLatch(1);
            ReplicationListener listener = new ReplicationListener(latch);
            replicator.getEventBus().register(listener);
            replicator.start();

            latch.await();
            replicator.getEventBus().unregister(listener);

            if (replicator.getState() != Replicator.State.COMPLETE) {
                System.out.println("Error replicating TO remote");
                System.out.println(listener.error);
            }

            Toast.makeText(MainActivity.this, "Local db is updated.",
                    Toast.LENGTH_SHORT).show();
        } catch (Exception e){
            Log.d("Exception found! ", e.toString());
        }
    }

    public void uploadAttachment(String id, String name){
        // simple 1-rev attachment
        String attachmentName = name;
        File f = new File("fixture", attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "image/jpeg");
        List<Attachment> atts = new ArrayList<Attachment>();
        atts.add(att);
        DocumentRevision oldRevision = ds.getDocument(id); //doc id
        DocumentRevision newRevision = null;
        try {
            // set attachment
            newRevision = ds.updateAttachments(oldRevision, atts);
        } catch (IOException ioe) {
            Log.d("IOException thrown: ", ioe.toString());
        } catch (ConflictException e) {
            Log.d("ConflictException thrown: ", e.toString());
        }
        // push replication
        //push();
    }
}
