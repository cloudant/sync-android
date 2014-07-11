package com.cloudant.imageshare;

import android.content.Context;
import android.content.Intent;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.net.Uri;
import android.test.ActivityInstrumentationTestCase2;
import android.test.ActivityUnitTestCase;
import android.test.suitebuilder.annotation.MediumTest;
import android.util.Log;
import android.widget.Toast;

import com.cloudant.imageshare.MainActivity;
import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.DocumentBody;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.datastore.UnsavedStreamAttachment;
import com.cloudant.sync.replication.PullReplication;
import com.cloudant.sync.replication.PushReplication;
import com.cloudant.sync.replication.Replicator;
import com.cloudant.sync.replication.ReplicatorFactory;

import junit.framework.TestCase;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.Exception;
import java.net.HttpCookie;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class MainActivityTest extends ActivityInstrumentationTestCase2<MainActivity> {

    private DatastoreManager datastoreManager;
    private Datastore ds;
    private MainActivity testActivity;
    private Context context;
    private HttpClient httpClient;

    int[] raws = {R.raw.pic1,R.raw.pic2,R.raw.pic3,R.raw.pic4,R.raw.pic5};

    public MainActivityTest() {
        super(MainActivity.class);
    }

    /*public MainActivityTest(Class<MainActivity> activityClass) {
        super(activityClass);
    }*/

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        testActivity = getActivity();
        context = testActivity.getApplicationContext();
        File path = context.getDir("datastores", 0);
        datastoreManager = new DatastoreManager(path.getAbsolutePath());
        try {
            datastoreManager.deleteDatastore("my_datastore");
        } catch (IOException e) {
            e.printStackTrace();
        }
        ds = datastoreManager.openDatastore("my_datastore");
        HttpResponse r = loadNewDB("sync-test-1");
    }

    @Override
    protected void tearDown() throws Exception{
        datastoreManager.deleteDatastore("my_datastore");
        HttpResponse r = deleteDB("sync-test-1");
        super.tearDown();
    }

    /*public void testPreconditions() {
        assertNotNull("MainActivity is null", testActivity);
        assertNotNull("datastoreManager is null", datastoreManager);
        assertNotNull("Datastore is null", ds);
        assertEquals("my_datastore", ds.getDatastoreName());
    }*/

    public void testAllImages() throws Exception{
        for (int image : raws) {
            InputStream is = testActivity.getResources().openRawResource(image);

            // Check if decoding works
            Bitmap bitmap = BitmapFactory.decodeStream(is);
            assertNotNull("Bitamp is null before replication", bitmap);
            is.reset();

            Uri image_uri = loadAsset(is);
            assertNotNull("Uri is null", image_uri);

            DocumentBody doc = new BasicDoc("Marco", "Polo");
            assertEquals("Polo", doc.asMap().get("Marco"));

            DocumentRevision revision = ds.createDocument(doc);
            assertEquals("Polo", ds.getDocument(revision.getId()).getBody().asMap().get("Marco"));

            DocumentRevision newRevision = addAttachment(revision, image_uri);
            assertNotNull(ds.getAttachment(newRevision, "image.jpg"));
        }
        pushReplicateDatastore("sync-test-1");

        datastoreManager.deleteDatastore("my_datastore");
        ds = datastoreManager.openDatastore("my_datastore");

        pullReplicateDatastore("sync-test-1");

        // read all documents in one go
        int pageSize = ds.getDocumentCount();
        List<DocumentRevision> docs = ds.getAllDocuments(0, pageSize, true);
        for (DocumentRevision rev : docs) {
            Attachment a = ds.getAttachment(rev, "image.jpg");
            assertNotNull("Attachment is null",a);
            InputStream is = a.getInputStream();

            // Check again if decoding works
            Bitmap bitmap = BitmapFactory.decodeStream(a.getInputStream());
            assertNotNull("Bitamp is null after replication", bitmap);
        }
    }

    public void testIm0() throws Exception{
        InputStream is = testActivity.getResources().openRawResource(raws[0]);
        individualImageCheck(is);
    }

    public void testIm1() throws Exception{
        InputStream is = testActivity.getResources().openRawResource(raws[1]);
        individualImageCheck(is);
    }

    public void testIm2() throws Exception{
        InputStream is = testActivity.getResources().openRawResource(raws[2]);
        individualImageCheck(is);
    }

    public void testIm3() throws Exception{
        InputStream is = testActivity.getResources().openRawResource(raws[3]);
        individualImageCheck(is);
    }

    public void testIm4() throws Exception{
        InputStream is = testActivity.getResources().openRawResource(raws[4]);
        individualImageCheck(is);
    }

    public void testLargeAttachment() throws Exception{
        InputStream is = testActivity.getResources().openRawResource(R.raw.big_photo);
        individualImageCheck(is);
    }

    private void individualImageCheck(InputStream is) throws Exception{
        // Check if decoding works
        Bitmap bitmap = BitmapFactory.decodeStream(is);
        assertNotNull("Bitamp is null before replication", bitmap);
        is.reset();

        Uri image_uri = loadAsset(is);
        assertNotNull("Uri is null", image_uri);

        DocumentBody doc = new BasicDoc("Marco", "Polo");
        assertEquals("Polo", doc.asMap().get("Marco"));

        DocumentRevision revision = ds.createDocument(doc);
        assertEquals("Polo", ds.getDocument(revision.getId()).getBody().asMap().get("Marco"));

        DocumentRevision newRevision = addAttachment(revision, image_uri);
        assertNotNull(ds.getAttachment(newRevision, "image.jpg"));

        //Push/Pull
        pushReplicateDatastore("sync-test-1");

        datastoreManager.deleteDatastore("my_datastore");
        ds = datastoreManager.openDatastore("my_datastore");

        pullReplicateDatastore("sync-test-1");

        // read document
        DocumentRevision final_rev = ds.getDocument(newRevision.getId());
        Attachment a = ds.getAttachment(final_rev, "image.jpg");
        assertNotNull("Attachment is null",a);

        // Check again if decoding works
        bitmap = BitmapFactory.decodeStream(a.getInputStream());
        assertNotNull("Bitamp is null after replication", bitmap);
    }

    private void pullReplicateDatastore(String dbname) throws Exception{
        URI uri = new URI("http://10.0.2.2:5984/" + dbname);
        //URI uri = new URI("https://" + testActivity.getString(R.string.default_user)
        //        + ".cloudant.com/" + testActivity.getString(R.string.default_dbname));

        // username/password can be Cloudant API keys
        PullReplication pull = new PullReplication();

        pull.username = testActivity.getString(R.string.default_api_key);
        pull.password = testActivity.getString(R.string.default_api_password);

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

        assertEquals(Replicator.State.COMPLETE, replicator.getState());
    }

    private void pushReplicateDatastore(String dbname) throws Exception{
        URI uri = new URI("http://10.0.2.2:5984/" + dbname);
        //URI uri = new URI("https://" + testActivity.getString(R.string.default_user)
        //        + ".cloudant.com/" + testActivity.getString(R.string.default_dbname));

        PushReplication push = new PushReplication();

        push.username = testActivity.getString(R.string.default_api_key);
        push.password = testActivity.getString(R.string.default_api_password);

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

        assertEquals(Replicator.State.COMPLETE, replicator.getState());
    }

    private DocumentRevision addAttachment(DocumentRevision revision, Uri image_uri) throws Exception{
        InputStream stream = context.getContentResolver().openInputStream(image_uri);
        Attachment att = new UnsavedStreamAttachment(stream, "image.jpg", "image/jpeg");
        List<Attachment> atts = new ArrayList<Attachment>();
        atts.add(att);
        DocumentRevision oldRevision = ds.getDocument(revision.getId()); //doc id
        DocumentRevision newRevision = null;
        // set attachment
        newRevision = ds.updateAttachments(oldRevision, atts);
        return newRevision;
    }

    private HttpResponse loadNewDB(String name) throws Exception{
        httpClient = new DefaultHttpClient();
        HttpPut httpPut = new HttpPut("http://10.0.2.2:5984/" + name);
        return httpClient.execute(httpPut);
    }

    private HttpResponse deleteDB(String name)  throws Exception{
        HttpDelete httpDelete = new HttpDelete("http://10.0.2.2:5984/" + name);
        return httpClient.execute(httpDelete);
    }

    // Move an asset to a file and pass it to adapter
    private Uri loadAsset(InputStream in_s){
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
            return  uri;
        } catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    private void copyFile(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[1024];
        int read;
        while ((read = in.read(buffer)) != -1) {
            out.write(buffer, 0, read);
        }
    }
}