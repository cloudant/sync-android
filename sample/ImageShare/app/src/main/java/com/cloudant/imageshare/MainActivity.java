package com.cloudant.imageshare;

import android.app.Activity;
import android.app.Application;
import android.content.res.AssetManager;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.util.DisplayMetrics;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.AdapterView;
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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class MainActivity extends Activity{

    private Datastore ds;
    private DatastoreManager manager;
    private ImageAdapter adapter;
    private boolean isEmulator = false;

    private enum ReplicationType {
        Pull,
        Push
    }

    @Override
    protected void onCreate(Bundle savedInstanceState){
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        // Check if running on emulator
        isEmulator = "generic".equals(Build.BRAND.toLowerCase());

        // Create a grid of images
        GridView gridview = (GridView) findViewById(R.id.gridview);
        int screenW = getScreenW();
        gridview.setColumnWidth(screenW/2);
        adapter = new ImageAdapter(this, screenW/2);
        gridview.setAdapter(adapter);
        reloadView(); //TODO: Check if makes the difference with regards to column num

        // Create an empty datastore
        try {
            initDatastore();
        } catch (IOException e){
            e.printStackTrace();
            System.exit(1);
        }

        // When a picture is clicked a new document is created and the image is added as attachment
        gridview.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            public void onItemClick(AdapterView<?> parent, View v, int position, long id) {
                DocumentBody doc = new BasicDoc("Position " + position, "ID " + id);
                DocumentRevision revision = ds.createDocument(doc);

                uploadAttachment(revision.getId(), position);

                Toast.makeText(MainActivity.this,
                                "Document " + revision.getId() + " written to local db",
                                Toast.LENGTH_SHORT).show();
                }
        });
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
                if (isEmulator){
                    loadAsset(getResources().openRawResource(R.raw.sample_5));
                    reloadView();
                } else {
                    // Take the user to their chosen image selection app (gallery or file manager)
                    Intent pickIntent = new Intent();
                    pickIntent.setType("image/*");
                    pickIntent.setAction(Intent.ACTION_GET_CONTENT);
                    startActivityForResult(Intent.createChooser(pickIntent, "Select Picture"), 1);
                    reloadView();
                }
                return true;
            case R.id.action_settings:
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
            case R.id.action_refresh:
                adapter.clearImageData();
                loadDatastore();
                reloadView();
                return true;
        }
        return super.onOptionsItemSelected(item);
    }


    // Processing the output of image selection app
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        if (resultCode == RESULT_OK && requestCode == 1) {
            try {
                Uri imageUri = data.getData();
                adapter.addImage(imageUri, this);
            } catch (IOException e) {
                Log.d("IOException found! ", e.toString());
            }
        }
        super.onActivityResult(requestCode, resultCode, data);
    }

    // Creates a DatastoreManager using application internal storage path
    public void initDatastore() throws IOException {
        File path = getApplicationContext().getDir("datastores", 0);
        manager = new DatastoreManager(path.getAbsolutePath());
        ds = manager.openDatastore("my_datastore");
        loadDatastore();
    }

    public void deleteDatastore(){
        try {
            manager.deleteDatastore("my_datastore");
            ds = manager.openDatastore("my_datastore");
        } catch (IOException e) {
            Log.d("MainActivity", e.toString());
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
                adapter.loadImage(a.getInputStream(), this);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void replicateDatastore(ReplicationType r){
        try {
            URI uri = new URI("https://" + getString(R.string.default_user)
                    + ".cloudant.com/" + getString(R.string.default_dbname));
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

            replication.username = getString(R.string.default_api_key);
            replication.password = getString(R.string.default_api_password);
            Replicator replicator = ReplicatorFactory.oneway(replication);

            // Use a CountDownLatch to provide a lightweight way to wait for completion
            CountDownLatch latch = new CountDownLatch(1);
            ReplicationListener listener = new ReplicationListener(latch);
            replicator.getEventBus().register(listener);
            replicator.start();

            latch.await();
            replicator.getEventBus().unregister(listener);

            if (replicator.getState() != Replicator.State.COMPLETE) {
                Log.d("MainActivity","Error replicating" + listener.error.toString());
            } else {
                Toast.makeText(MainActivity.this, "Replication finished",
                        Toast.LENGTH_SHORT).show();
            }
        } catch (Exception e) {
            Log.d(r.toString() + "Replicate exception", e.toString());
        }
    }

    public void uploadAttachment(String id, int position) {
        try {
            InputStream is = adapter.getStream(position);

            Attachment att = new UnsavedStreamAttachment(is, "image.jpg", "image/jpeg");
            List<Attachment> atts = new ArrayList<Attachment>();
            atts.add(att);
            DocumentRevision oldRevision = ds.getDocument(id); //doc id
            DocumentRevision newRevision = null;
            // set attachment
            newRevision = ds.updateAttachments(oldRevision, atts);
        } catch (Exception e) {
            Log.d("UploadAttachment exception", e.toString());
        }
    }

    // Move an asset to a file and pass it to adapter
    //TODO: Investigate why attachment cannot be loaded from the asset
    private void loadAsset(InputStream in_s) {
        try {
            InputStream in = null;
            OutputStream out = null;
            in = in_s;
            String outs = "/data/data/com.cloudant.imageshare/";
            Log.d("out", outs);
            File outFile = new File(outs, "image.jpg");

            out = new FileOutputStream(outFile);
            copyFile(in, out);
            in.close();
            in = null;
            out.flush();
            out.close();
            out = null;
            outs = "file:///" + outs;
            Uri uri = Uri.parse(outs + "image.jpg");
            adapter.addImage(uri, this);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void copyFile(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[1024];
        int read;
        while ((read = in.read(buffer)) != -1) {
            out.write(buffer, 0, read);
        }
    }

    private void reloadView() {
        GridView gridview = (GridView) findViewById(R.id.gridview);
        gridview.invalidate();
        gridview.requestLayout();
    }

    private int getScreenW() {
        DisplayMetrics displayMetrics = getResources().getDisplayMetrics();
        return (int) (displayMetrics.widthPixels / displayMetrics.density);
    }
}
