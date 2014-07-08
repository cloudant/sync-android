package com.cloudant.imageshare;

import android.app.Activity;
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
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class MainActivity extends Activity {

    private Datastore ds;
    private DatastoreManager manager;
    private ImageAdapter adapter;
    private boolean isEmulator = false;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
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

        // Create an empty datastore
        initDatastore();

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
    public boolean onOptionsItemSelected(MenuItem item) {
        switch(item.getItemId()){
            case R.id.action_add:
                if (isEmulator){
                    loadAsset();
                    reloadView();
                } else {
                    // Take the user to their chosen image selection app (gallery or file manager)
                    Intent pickIntent = new Intent();
                    pickIntent.setType("image/*");
                    pickIntent.setAction(Intent.ACTION_GET_CONTENT);
                    startActivityForResult(Intent.createChooser(pickIntent, "Select Picture"), 1);
                }
                return true;
            case R.id.action_settings:
                return true;
            case R.id.action_replicate:
                pushReplicateDatastore();
                return true;
            case R.id.action_delete:
                deleteDatastore();
                reloadView();
                return true;
            case R.id.action_pull_replicate:
                pullReplicateDatastore();
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
                reloadView();
            } catch (IOException e) {
                Log.d("IOException found! ", e.toString());
            }
        }
        super.onActivityResult(requestCode, resultCode, data);
    }

    // Creates a DatastoreManager using application internal storage path
    public void initDatastore(){
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
    public void loadDatastore(){
        // read all documents in one go
        int pageSize = ds.getDocumentCount();
        List<DocumentRevision> docs = ds.getAllDocuments(0, pageSize, true);
        for (DocumentRevision rev : docs) {
            try {
                /*Log.d("rev",rev.getRevision());
                Log.d("id",rev.getId());
                Map<String,Object> m = rev.getBody().asMap();
                if (m.isEmpty()) System.out.println("Empty body :(");
                for (Map.Entry entry : m.entrySet()) {
                    System.out.println(entry.getKey() + ", " + entry.getValue());
                }
                for (Attachment a : ds.attachmentsForRevision(rev)) {
                    Log.d("attachment", a.toString());
                } */
                Attachment a = ds.getAttachment(rev, "image");
                /*Log.d("name",a.name);
                Log.d("size", "" + a.getSize());*/
                adapter.loadImage(a.getInputStream(), this);
            } catch (Exception e) {
                Log.d("IOException:loadDatastore", e.toString());
            }
        }
    }

    public void pushReplicateDatastore(){
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
                Log.d("MainActivity","Error replicating TO remote" + listener.error.toString());
            } else {
                Toast.makeText(MainActivity.this, "Local db is replicated.",
                        Toast.LENGTH_SHORT).show();
            }
        } catch (Exception e) {
            Log.d("PushReplicate exception", e.toString());
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
                Log.d("MainActivity","Error replicating FROM remote" + listener.error.toString());
            } else {
                Toast.makeText(MainActivity.this, "Local db is updated.",
                        Toast.LENGTH_SHORT).show();
            }
        } catch (Exception e){
            Log.d("PullReplicate exception", e.toString());
        }
    }

    public void uploadAttachment(String id, int position){
        try {
            InputStream is = adapter.getStream(position);

            Attachment att = new UnsavedStreamAttachment(is, "image", "image/jpeg");
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
    private void loadAsset(){
        try {
            InputStream in = null;
            OutputStream out = null;
            //in = getAssets().open("sample_0.jpg");
            in = getResources().openRawResource(R.raw.sample_5);
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
        } catch (Exception e){
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

    private void reloadView(){
        GridView gridview = (GridView) findViewById(R.id.gridview);
        gridview.invalidate();
        gridview.requestLayout();
    }

    private int getScreenW(){
        DisplayMetrics displayMetrics = getResources().getDisplayMetrics();
        return (int) (displayMetrics.widthPixels / displayMetrics.density);
    }
}
