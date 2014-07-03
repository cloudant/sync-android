package com.cloudant.imageshare;

import android.app.Activity;
import android.graphics.Bitmap;
import android.net.Uri;
import android.os.Bundle;
import android.provider.MediaStore;
import android.util.DisplayMetrics;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.AdapterView;
import android.widget.GridView;
import android.widget.Toast;
import android.content.Intent;

import com.cloudant.sync.datastore.ConflictException;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DocumentBody;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.datastore.UnsavedStreamAttachment;
import com.cloudant.sync.replication.ReplicatorFactory;
import com.cloudant.sync.replication.Replicator;
import com.cloudant.sync.replication.PushReplication;
import com.cloudant.sync.replication.PullReplication;
import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.UnsavedFileAttachment;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import java.io.File;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.cloudant.imageshare.R.drawable.sample_0;

public class MainActivity extends Activity {

    private Datastore ds;
    private DatastoreManager manager;
    private ImageAdapter adapter;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        //Create a grid of images
        GridView gridview = (GridView) findViewById(R.id.gridview);
        int screenW = getScreenW();
        gridview.setColumnWidth(screenW/2);
        adapter = new ImageAdapter(this, screenW/2);
        gridview.setAdapter(adapter);

        initDatastore();

        /*
        Picture in the right column adds a document to a local datastore
        while picture on the left adds it and replicates the whole database to remote.
        */
        gridview.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            public void onItemClick(AdapterView<?> parent, View v, int position, long id) {
                // Create a document
                DocumentBody doc = new BasicDoc("Position " + position, "ID " + id);
                DocumentRevision revision = ds.createDocument(doc);

                uploadAttachment(revision.getId(), position);

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
            case R.id.action_add:
                //take the user to their chosen image selection app (gallery or file manager)
                Intent pickIntent = new Intent();
                pickIntent.setType("image/*");
                pickIntent.setAction(Intent.ACTION_GET_CONTENT);
                startActivityForResult(Intent.createChooser(pickIntent, "Select Picture"), 1);
                return true;

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

    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        if (resultCode == RESULT_OK && requestCode == 1) {
            try {
                Uri imageUri = data.getData();
                adapter.addImage(imageUri, this);
                //reload the view
                GridView gridview = (GridView) findViewById(R.id.gridview);
                gridview.invalidate();
                gridview.requestLayout();
            } catch (IOException e) {
                Log.d("IOException found! ", e.toString());
            }
        }
        super.onActivityResult(requestCode, resultCode, data);
    }

    public void initDatastore(){
        // Create a DatastoreManager using application internal storage path
        File path = getApplicationContext().getDir("datastores", 0);
        manager = new DatastoreManager(path.getAbsolutePath());
        try {
            manager.deleteDatastore("my_datastore");
        } catch (IOException e){
            Log.d("MainActivity:initDatastore", e.toString());
        }
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

    public void uploadAttachment(String id, int position){
        Log.d("uploadAttachment", "At position: " + position);
        // simple 1-rev attachment
        //File f = adapter.getItem(position);
        try {
            InputStream is = adapter.getStream(position);

            Attachment att = new UnsavedStreamAttachment(is, "image" + position, "image/jpeg");
            List<Attachment> atts = new ArrayList<Attachment>();
            atts.add(att);
            DocumentRevision oldRevision = ds.getDocument(id); //doc id
            DocumentRevision newRevision = null;
            // set attachment
            newRevision = ds.updateAttachments(oldRevision, atts);
            Log.d("Main","Doc with attachment: " + id);
        } catch (Exception e) {
            Log.d("Exception thrown: ", e.toString());
        }
        // push replication
        //push();
    }
}
