package com.cloudant.sync.datastore;

import com.cloudant.android.Base64OutputStreamFactory;
import com.cloudant.mazha.Response;
import com.cloudant.sync.replication.ReplicationTestBase;
import com.cloudant.sync.util.TestUtils;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class DocumentRevisionBuilderTest extends ReplicationTestBase {

    @Test
    public void buildRevisionFromMapValidMap() throws Exception {

        Map<String,String> jsonMap = new HashMap<String,String>();
        jsonMap.put("_id","someIdHere");
        jsonMap.put("_rev","3-750dac460a6cc41e6999f8943b8e603e");
        jsonMap.put("aKey","aValue");
        jsonMap.put("hello","world");

        Map<String,String> body = new HashMap<String,String>();
        body.put("aKey","aValue");
        body.put("hello","world");

        URI uri = new URI("http://localhost:5984");

        DocumentRevision revision =
                DocumentRevisionBuilder.buildRevisionFromMap(uri,jsonMap);


        Assert.assertNotNull(revision);
        Assert.assertEquals(body,revision.getBody().asMap());
        Assert.assertEquals(revision.getId(),"someIdHere");
        Assert.assertEquals(revision.getRevision(),"3-750dac460a6cc41e6999f8943b8e603e");

    }

    @Test
    public void buildRevisionFromMapValidMapDelectedDoc() throws Exception {

        Map<String,Object> jsonMap = new HashMap<String,Object>();
        jsonMap.put("_id","someIdHere");
        jsonMap.put("_rev","3-750dac460a6cc41e6999f8943b8e603e");
        jsonMap.put("_deleted",Boolean.TRUE);
        jsonMap.put("aKey","aValue");
        jsonMap.put("hello","world");

        Map<String,String> body = new HashMap<String,String>();
        body.put("aKey","aValue");
        body.put("hello","world");

        URI uri = new URI("http://localhost:5984");

        BasicDocumentRevision revision =
                DocumentRevisionBuilder.buildRevisionFromMap(uri,jsonMap);


        Assert.assertNotNull(revision);
        Assert.assertEquals(body,revision.getBody().asMap());
        Assert.assertEquals(revision.getId(),"someIdHere");
        Assert.assertEquals(revision.getRevision(),"3-750dac460a6cc41e6999f8943b8e603e");
        Assert.assertTrue(revision.isDeleted());

    }

    @Test
    public void buildRevisionFromMapValidMapAllFields() throws Exception {

        Map<String,Object> jsonMap = new HashMap<String,Object>();
        jsonMap.put("_id","someIdHere");
        jsonMap.put("_rev","3-750dac460a6cc41e6999f8943b8e603e");
        jsonMap.put("aKey","aValue");
        jsonMap.put("hello","world");
        jsonMap.put("_attachments",new HashMap<String,String>());
        jsonMap.put("_conflicts",new String[0]);
        jsonMap.put("_deleted_conflicts",new HashMap<String,Object>());
        jsonMap.put("_local_seq",1);
        jsonMap.put("_revs_info",new HashMap<String,Object>());
        jsonMap.put("_revisions",new String[0]);


        Map<String,String> body = new HashMap<String,String>();
        body.put("aKey","aValue");
        body.put("hello","world");

        URI uri = new URI("http://localhost:5984");

        BasicDocumentRevision revision =
                DocumentRevisionBuilder.buildRevisionFromMap(uri,jsonMap);


        Assert.assertNotNull(revision);
        Assert.assertEquals(body,revision.getBody().asMap());
        Assert.assertEquals(revision.getId(),"someIdHere");
        Assert.assertEquals(revision.getRevision(),"3-750dac460a6cc41e6999f8943b8e603e");
        Assert.assertEquals(revision.getAttachments().size(),0);

    }

    @Test(expected = IllegalArgumentException.class)
    public void buildRevisionFromMapInValidMap() throws Exception {

        Map<String,String> jsonMap = new HashMap<String,String>();
        jsonMap.put("_id","someIdHere");
        jsonMap.put("_rev","3-750dac460a6cc41e6999f8943b8e603e");
        jsonMap.put("_notValidKey","not valid");
        jsonMap.put("aKey","aValue");
        jsonMap.put("hello","world");

        Map<String,String> body = new HashMap<String,String>();
        body.put("aKey","aValue");
        body.put("hello","world");

        URI uri = new URI("http://localhost:5984");

        DocumentRevision revision =
                DocumentRevisionBuilder.buildRevisionFromMap(uri,jsonMap);

    }

    @Test
    public void buildRevisionFromMapValidMapWithAttachments() throws Exception {

        //lets the get the attachment encoded

        File file = TestUtils.loadFixture("fixture/bonsai-boston.jpg");

        byte[] unencodedAttachment = FileUtils.readFileToByteArray(file);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        OutputStream out = Base64OutputStreamFactory.get(byteArrayOutputStream);
        out.write(unencodedAttachment);
        out.flush();
        byteArrayOutputStream.flush();
        out.close();
        byteArrayOutputStream.close();

        byte[] encodedAttachment = byteArrayOutputStream.toByteArray();

        String encodedAttachmentString = new String(encodedAttachment);


        Map<String,Object> jsonMap = new HashMap<String,Object>();
        jsonMap.put("_id","someIdHere");
        jsonMap.put("_rev","3-750dac460a6cc41e6999f8943b8e603e");
        jsonMap.put("aKey","aValue");
        jsonMap.put("hello","world");

        Map<String,Map<String,Object>> attachments = new HashMap<String,Map<String,Object>>();
        Map<String,Object> bonsai = new HashMap<String,Object>();
        bonsai.put("length",encodedAttachmentString.length());
        bonsai.put("digest","thisisahasiswear");
        bonsai.put("revops",1);
        bonsai.put("content_type","image/jpeg");
        bonsai.put("stub", false);
        bonsai.put("data",encodedAttachmentString);

        attachments.put("bonsai-boston.jpg",bonsai);
        jsonMap.put("_attachments",attachments);

        Map<String,String> body = new HashMap<String,String>();
        body.put("aKey","aValue");
        body.put("hello","world");

        URI uri = new URI("http://localhost:5984/someIdHere");

        DocumentRevision revision =
                DocumentRevisionBuilder.buildRevisionFromMap(uri,jsonMap);


        Assert.assertNotNull(revision);
        Assert.assertEquals(body,revision.getBody().asMap());
        Assert.assertEquals(revision.getId(),"someIdHere");
        Assert.assertEquals(revision.getRevision(),"3-750dac460a6cc41e6999f8943b8e603e");
        Assert.assertEquals(revision.getAttachments().size(),1);
        ByteArrayInputStream expected = new ByteArrayInputStream(unencodedAttachment);
        InputStream actual = revision.getAttachments().get("bonsai-boston.jpg").getInputStream();
        Assert.assertTrue(TestUtils.streamsEqual(expected,actual));


    }

    @Test
    public void buildRevisionFromMapValidMapWithAttachmentsDataExcluded() throws Exception {

        //lets the get the attachment encoded

        File file = TestUtils.loadFixture("fixture/bonsai-boston.jpg");

        byte[] unencodedAttachment = FileUtils.readFileToByteArray(file);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        OutputStream out = Base64OutputStreamFactory.get(byteArrayOutputStream);
        out.write(unencodedAttachment);

        //close and flush all streams
        out.flush();
        byteArrayOutputStream.flush();
        out.close();
        byteArrayOutputStream.close();

        byte[] encodedAttachment = byteArrayOutputStream.toByteArray();

        String encodedAttachmentString = new String(encodedAttachment);

        //create a revision on a couchDB instance so we can test the download
        //of attachments
        Map<String,Object> remoteDoc = new HashMap<String,Object>();
        remoteDoc.put("_id","someIdHere");
        Map<String,Map<String,Object>> remoteAttachments = new HashMap<String,Map<String,Object>>();
        Map<String,Object> remoteBonsai = new HashMap<String,Object>();
        remoteBonsai.put("length",encodedAttachmentString.length());
        remoteBonsai.put("digest","thisisahasiswear");
        remoteBonsai.put("revops",1);
        remoteBonsai.put("content_type","image/jpeg");
        remoteBonsai.put("data",encodedAttachmentString);

        remoteAttachments.put("bonsai-boston.jpg",remoteBonsai);
        remoteDoc.put("_attachments",remoteAttachments);


        remoteDb.create(remoteDoc);


        //build up the test json map


        Map<String,Object> jsonMap = new HashMap<String,Object>();
        jsonMap.put("_id","someIdHere");
        jsonMap.put("_rev","3-750dac460a6cc41e6999f8943b8e603e");
        jsonMap.put("aKey","aValue");
        jsonMap.put("hello","world");

        Map<String,Map<String,Object>> attachments = new HashMap<String,Map<String,Object>>();
        Map<String,Object> bonsai = new HashMap<String,Object>();
        bonsai.put("length",unencodedAttachment.length);
        bonsai.put("digest","thisisahasiswear");
        bonsai.put("revops",1);
        bonsai.put("content_type","image/jpeg");
        bonsai.put("stub",true);

        attachments.put("bonsai-boston.jpg",bonsai);
        jsonMap.put("_attachments",attachments);

        Map<String,String> body = new HashMap<String,String>();
        body.put("aKey","aValue");
        body.put("hello","world");

        URI uri =  new URI(remoteDb.getCouchClient().getDefaultDBUri().toString()+"/"+"someIdHere");

        //create the document revision and test

        DocumentRevision revision =
                DocumentRevisionBuilder.buildRevisionFromMap(uri,jsonMap);


        Assert.assertNotNull(revision);
        Assert.assertEquals(body,revision.getBody().asMap());
        Assert.assertEquals(revision.getId(),"someIdHere");
        Assert.assertEquals(revision.getRevision(),"3-750dac460a6cc41e6999f8943b8e603e");
        Assert.assertEquals(revision.getAttachments().size(),1);
        InputStream attachmentInputStream = revision.getAttachments().get("bonsai-boston.jpg").getInputStream();
        InputStream expectedInputStream = new ByteArrayInputStream(unencodedAttachment);
        Assert.assertTrue(TestUtils.streamsEqual(expectedInputStream, attachmentInputStream));

    }

    @Test
    public void buildRevisionFromMapValidMapWithAttachmentsDataExcludedNonWinningRev() throws Exception {

        //lets the get the attachment encoded

        File file = TestUtils.loadFixture("fixture/bonsai-boston.jpg");

        byte[] unencodedAttachment = FileUtils.readFileToByteArray(file);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        OutputStream out = Base64OutputStreamFactory.get(byteArrayOutputStream);
        out.write(unencodedAttachment);

        //close and flush all streams
        out.flush();
        byteArrayOutputStream.flush();
        out.close();
        byteArrayOutputStream.close();

        byte[] encodedAttachment = byteArrayOutputStream.toByteArray();

        String encodedAttachmentString = new String(encodedAttachment);

        //create a revision on a couchDB instance so we can test the download
        //of attachments
        Map<String,Object> remoteDoc = new HashMap<String,Object>();
        remoteDoc.put("_id","someIdHere");
        Map<String,Map<String,Object>> remoteAttachments = new HashMap<String,Map<String,Object>>();
        Map<String,Object> remoteBonsai = new HashMap<String,Object>();
        remoteBonsai.put("length",encodedAttachmentString.length());
        remoteBonsai.put("digest","thisisahasiswear");
        remoteBonsai.put("revops",1);
        remoteBonsai.put("content_type","image/jpeg");
        remoteBonsai.put("data",encodedAttachmentString);

        remoteAttachments.put("bonsai-boston.jpg",remoteBonsai);
        remoteDoc.put("_attachments",remoteAttachments);


        Response response = remoteDb.create(remoteDoc);
        remoteDoc.put("_rev",response.getRev());

        remoteDoc.remove("_attachments");
        remoteDb.update("someIdHere",remoteDoc);


        //build up the test json map


        Map<String,Object> jsonMap = new HashMap<String,Object>();
        jsonMap.put("_id","someIdHere");
        jsonMap.put("_rev","3-750dac460a6cc41e6999f8943b8e603e");
        jsonMap.put("aKey","aValue");
        jsonMap.put("hello","world");

        Map<String,Map<String,Object>> attachments = new HashMap<String,Map<String,Object>>();
        Map<String,Object> bonsai = new HashMap<String,Object>();
        bonsai.put("length",unencodedAttachment.length);
        bonsai.put("digest","thisisahasiswear");
        bonsai.put("revops",1);
        bonsai.put("content_type","image/jpeg");
        bonsai.put("stub",true);

        attachments.put("bonsai-boston.jpg",bonsai);
        jsonMap.put("_attachments",attachments);

        Map<String,String> body = new HashMap<String,String>();
        body.put("aKey","aValue");
        body.put("hello","world");

        URI uri =  new URI(remoteDb.getCouchClient().getDefaultDBUri().toString()+"/"+"someIdHere"+"?rev="+response.getRev());

        //create the document revision and test

        DocumentRevision revision =
                DocumentRevisionBuilder.buildRevisionFromMap(uri,jsonMap);


        Assert.assertNotNull(revision);
        Assert.assertEquals(body,revision.getBody().asMap());
        Assert.assertEquals(revision.getId(),"someIdHere");
        Assert.assertEquals(revision.getRevision(),"3-750dac460a6cc41e6999f8943b8e603e");
        Assert.assertEquals(revision.getAttachments().size(),1);
        InputStream attachmentInputStream = revision.getAttachments().get("bonsai-boston.jpg").getInputStream();
        InputStream expectedInputStream = new ByteArrayInputStream(unencodedAttachment);
        Assert.assertTrue(TestUtils.streamsEqual(expectedInputStream, attachmentInputStream));

    }

    @Test
    public void buildRevisionFromMapValidMapWithTextAttachmentsDataExcluded() throws Exception {

        //lets the get the attachment encoded

        String attachmentText = "Hello World";

        byte[] unencodedAttachment = attachmentText.getBytes();

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        OutputStream out = Base64OutputStreamFactory.get(byteArrayOutputStream);
        out.write(unencodedAttachment);

        //close and flush all streams
        out.flush();
        byteArrayOutputStream.flush();
        out.close();
        byteArrayOutputStream.close();

        byte[] encodedAttachment = byteArrayOutputStream.toByteArray();

        String encodedAttachmentString = new String(encodedAttachment);

        //create a revision on a couchDB instance so we can test the download
        //of attachments
        Map<String,Object> remoteDoc = new HashMap<String,Object>();
        remoteDoc.put("_id","someIdHere");
        Map<String,Map<String,Object>> remoteAttachments = new HashMap<String,Map<String,Object>>();
        Map<String,Object> remoteBonsai = new HashMap<String,Object>();
        remoteBonsai.put("length",encodedAttachmentString.length());
        remoteBonsai.put("digest","thisisahasiswear");
        remoteBonsai.put("revops",1);
        remoteBonsai.put("content_type","text/plain");
        remoteBonsai.put("data",encodedAttachmentString);

        remoteAttachments.put("hello.txt",remoteBonsai);
        remoteDoc.put("_attachments",remoteAttachments);


        remoteDb.create(remoteDoc);


        //build up the test json map


        Map<String,Object> jsonMap = new HashMap<String,Object>();
        jsonMap.put("_id","someIdHere");
        jsonMap.put("_rev","3-750dac460a6cc41e6999f8943b8e603e");
        jsonMap.put("aKey","aValue");
        jsonMap.put("hello","world");

        Map<String,Map<String,Object>> attachments = new HashMap<String,Map<String,Object>>();
        Map<String,Object> bonsai = new HashMap<String,Object>();
        bonsai.put("length",unencodedAttachment.length);
        bonsai.put("digest","thisisahasiswear");
        bonsai.put("revops",1);
        bonsai.put("content_type","text/plain");
        bonsai.put("stub",true);

        attachments.put("hello.txt",bonsai);
        jsonMap.put("_attachments",attachments);

        Map<String,String> body = new HashMap<String,String>();
        body.put("aKey","aValue");
        body.put("hello","world");

        URI uri =  new URI(remoteDb.getCouchClient().getDefaultDBUri().toString()+"/"+"someIdHere");

        //create the document revision and test

        DocumentRevision revision =
                DocumentRevisionBuilder.buildRevisionFromMap(uri,jsonMap);


        Assert.assertNotNull(revision);
        Assert.assertEquals(body,revision.getBody().asMap());
        Assert.assertEquals(revision.getId(),"someIdHere");
        Assert.assertEquals(revision.getRevision(),"3-750dac460a6cc41e6999f8943b8e603e");
        Assert.assertEquals(revision.getAttachments().size(),1);
        InputStream attachmentInputStream = revision.getAttachments().get("hello.txt").getInputStream();
        InputStream expectedInputStream = new ByteArrayInputStream(unencodedAttachment);
        Assert.assertTrue(TestUtils.streamsEqual(expectedInputStream, attachmentInputStream));

    }



}